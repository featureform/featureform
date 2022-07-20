---
description: A quick start guide for Featureform on AWS EKS.
---

# Quickstart (Kubernetes)

This quickstart will walk through creating a few simple features, labels, and a training set using Postgres and Redis. We will use a transaction fraud training set.

## Step 1: Install Featureform client

Install the Featureform SDK via Pip.

```
pip install featureform
```

## Step 2: Deploy EKS

You can follow our [Minikube](deployment/minikube.md) or [Kubernetes](deployment/kubernetes.md) deployment guide. This will walk through a simple AWS deployment of Featureform with our quick start Helm chart containing Postgres and Redis.

Install the AWS CLI then run the following command to create an EKS cluster.

```
eksctl create cluster \
--name featureform \
--version 1.21 \
--region us-east-1 \
--nodegroup-name linux-nodes \
--nodes 1 \
--nodes-min 1 \
--nodes-max 4 \
--with-oidc \
--managed
```

## Step 3: Install Helm charts

We'll be installing three Helm Charts: Featureform, the Quickstart Demo, and Certificate Manager.

First we need to add the Helm repositories.

```
helm repo add featureform https://storage.googleapis.com/featureform-helm/ 
helm repo add jetstack https://charts.jetstack.io 
helm repo update
```

Now we can install the Helm charts.

```
helm install certmgr jetstack/cert-manager \
    --set installCRDs=true \
    --version v1.8.0 \
    --namespace cert-manager \
    --create-namespace
helm install my-featureform featureform/featureform
helm install quickstart featureform/quick-start
```

## Step 4: Create TLS certificate

We can a self-signed TLS certificate for connecting directly to the load balancer.

Wait until the load balancer has been created. It can be checked using:

```
kubectl get ingress
```

When the ingresses have a valid address, we can update the deployment to create the public certificate.

```
export FEATUREFORM_HOST=$(kubectl get ingress | grep "grpc-ingress" | awk {'print $4'} | column -t)
helm upgrade my-featureform featureform/featureform --set global.hostname=$FEATUREFORM_HOST
```

We can save and export our self-signed certificate.

```
kubectl get secret featureform-ca-secret -o=custom-columns=':.data.tls\.crt'| base64 -d > tls.crt
export FEATUREFORM_CERT=$(pwd)/tls.crt
```

The dashboard is now viewable at your ingress address.

```
echo $FEATUREFORM_HOST
```

## Step 5: Register providers

The Quickstart helm chart creates a Postgres instance with preloaded data, as well as an empty Redis standalone instance. Now that they are deployed, we can write a config file in Python.

{% code title="definitions.py" %}
```python
import featureform as ff

redis = ff.register_redis(
    name = "redis-quickstart",
    host="quickstart-redis", # The internal dns name for redis
    port=6379,
    description = "A Redis deployment we created for the Featureform quickstart"
)

postgres = ff.register_postgres(
    name = "postgres-quickstart",
    host="quickstart-postgres", # The internal dns name for postgres
    port="5432",
    user="postgres",
    password="password",
    database="postgres",
    description = "A Postgres deployment we created for the Featureform quickstart"
)
```
{% endcode %}

Once we create our config file, we can apply it to our Featureform deployment.

```bash
featureform apply definitions.py
```

## Step 6: Define our resources

We will create a user profile for us, and set it as the default owner for all the following resource definitions.

{% code title="definitions.py" %}
```python
ff.register_user("featureformer").make_default_owner()
```
{% endcode %}

Now we'll register our  user fraud dataset in Featureform.

{% code title="definitions.py" %}
```python
transactions = postgres.register_table(
    name = "transactions",
    variant = "kaggle",
    description = "Fraud Dataset From Kaggle",
    table = "Transactions", # This is the table's name in Postgres
)
```
{% endcode %}

Next, we'll define a SQL transformation on our dataset.

{% code title="definitions.py" %}
```python
@postgres.sql_transformation(variant="quickstart", schedule="* * * * *")
def average_user_transaction():
    """the average transaction amount for a user """
    return "SELECT CustomerID as user_id, avg(TransactionAmount) " \
           "as avg_transaction_amt from {{transactions.kaggle}} GROUP BY user_id"
    
```
{% endcode %}

Next, we'll register a passenger entity to associate with a feature and label.

{% code title="definitions.py" %}
```python
user = ff.register_entity("user")
# Register a column from our transformation as a feature
average_user_transaction.register_resources(
    entity=user,
    entity_column="user_id",
    inference_store=redis,
    features=[
        {"name": "avg_transactions", "variant": "quickstart", "column": "avg_transaction_amt", "type": "float32"},
    ],
)
# Register label from our base Transactions table
transactions.register_resources(
    entity=user,
    entity_column="customerid",
    labels=[
        {"name": "fraudulent", "variant": "quickstart", "column": "isfraud", "type": "bool"},
    ],
)
```
{% endcode %}

Finally, we'll join together the feature and label intro a training set.

{% code title="definitions.py" %}
```python
ff.register_training_set(
    "fraud_training", "quickstart",
    label=("fraudulent", "quickstart"),
    features=[("avg_transactions", "quickstart")],
)
```
{% endcode %}

Now that our definitions are complete, we can apply it to our Featureform instance.

```bash
featureform apply definitions.py
```

## Step 7: Serve features for training and inference

Once we have our training set and features registered, we can train our model.

```python
import featureform as ff

client = ff.ServingClient()
dataset = client.dataset("fraud_training", "quickstart")
training_dataset = dataset.repeat(10).shuffle(1000).batch(8)
for feature_batch in training_dataset:
    # Train model
```

We can serve features in production once we deploy our trained model as well.

```python
import featureform as ff

client = ff.ServingClient()
fpf = client.features([("avg_transactions", "quickstart")], {"user": "C1410926"})
# Run features through model
```
