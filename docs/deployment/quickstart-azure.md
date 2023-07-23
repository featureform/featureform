---
description: A quick start guide for Featureform on Azure AKS.
---

# Quickstart (Azure)

This quickstart will walk through creating a few simple features, labels, and a training set using Postgres and Redis. We will use a transaction fraud training set.

## Step 1: Install Featureform client

### Requirements

- Python 3.7-3.10
- Kubectl
- Azure CLI
- An available domain/subdomain name

Install the Featureform SDK via Pip.

```shell
pip install featureform
```

## Step 2: Export domain name

Featureform uses [gRPC](https://grpc.io/) which, in combination with the
[nginx ingress](https://github.com/kubernetes/ingress-nginx) requires a fully qualified domain name.

```shell
export FEATUREFORM_HOST=<your_domain_name>
```

## Step 3: Setup the AKS Cluster

This step will provision a single node Kubernetes cluster with AKS

### Login

Login to the Azure CLI

```shell
az login
```

### Create Resource Group

Create a resource group for the kubernetes cluster

```shell
az group create --name FeatureformResourceGroup --location eastus
```

### Create A Cluster

Create a single node cluster for Featureform

```shell
az aks create --resource-group FeatureformResourceGroup --name FeatureformAKSCluster --node-count 1 --generate-ssh-keys
```

### Add To Kubeconfig

Add the cluster information to the kubeconfig as a the current context

```shell
az aks get-credentials --resource-group FeatureformResourceGroup --name FeatureformAKSCluster
```

### Verify connection

```shell
kubectl get nodes
```

You should get a result like:

```shell
NAME                                STATUS   ROLES   AGE     VERSION
aks-nodepool1-25554489-vmss000000   Ready    agent   7m56s   v1.24.6
```

## Step 4: Install Helm charts

We'll be installing three Helm Charts: Featureform, the Quickstart Demo, and Certificate Manager.

First we need to add the Helm repositories.

```shell
helm repo add featureform https://storage.googleapis.com/featureform-helm/ 
helm repo add jetstack https://charts.jetstack.io 
helm repo update
```

Now we can install the Helm charts.

```shell
helm install certmgr jetstack/cert-manager \
    --set installCRDs=true \
    --version v1.8.0 \
    --namespace cert-manager \
    --create-namespace
    
helm install featureform featureform/featureform \
    --set global.publicCert=true \
    --set global.localCert=false \
    --set global.hostname=$FEATUREFORM_HOST
    
helm install quickstart featureform/quickstart
```

## Step 5: Setup Domain Name

### Get the ingress IP address

Get the IP address of the ingress. It may take a minute or so to show.

```shell
kubectl get ingress
```

In your DNS provider create two records:

| Key                | Value                         | Record Type |
|--------------------|-------------------------------|-------------|
| <your_domain_name> | <ingress_ip_address>          | A           |
| <your_domain_name> | 0 issuewild "letsencrypt.org" | CAA         |

This will allow the client to securely connect to the cluster by allowing the cluster to provision its
own public IP address.

You can check when the cluster is ready by running

```shell
kubectl get cert
```

and checking that the status of the certificates is ready.

## Step 6: Register providers

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

```shell
featureform apply definitions.py
```

## Step 7: Define our resources

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
@postgres.sql_transformation(variant="quickstart")
def average_user_transaction():
    """the average transaction amount for a user """
    return "SELECT CustomerID as user_id, avg(TransactionAmount) " \
           "as avg_transaction_amt from {{transactions.kaggle}} GROUP BY user_id"
    
```

{% endcode %}

Next, we'll register a passenger entity to associate with a feature and label.

{% code title="definitions.py" %}

```python
@ff.entity
class User:
    # Register a column from our transformation as a feature
    avg_transactions = ff.Feature(
        average_user_transaction[["user_id", "avg_transaction_amt"]],
        variant: "quickstart",
        type=ff.Float32,
        inference_store=redis
    )
    # Register label from our base Transactions table
    fraudulent = ff.Label(
        transactions[["customerid", "isfraud"]],
        variant: "quickstart",
        type=ff.Bool,
    )
```

{% endcode %}

Finally, we'll join together the feature and label into a training set.

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
dataset = client.training_set("fraud_training", "quickstart")
training_dataset = dataset.repeat(10).shuffle(1000).batch(8)
for row in training_dataset:
    print(row.features(), row.label())
```

We can serve features in production once we deploy our trained model as well.

```python
import featureform as ff

client = ff.ServingClient()
fpf = client.features([("avg_transactions", "quickstart")], {"user": "C1410926"})
print(fpf)
```
