---
description: A quick start guide for Featureform on AWS EKS.
---

# Quickstart (Kubernetes)

This quickstart will walk through creating a few simple features, labels, and a training set using Postgres and Redis. We will use a transaction fraud training set.

## Step 1: Install Featureform client

### Requirements

- Python 3.7+

Install the Featureform SDK via Pip.

```shell
pip install featureform
```

## Step 2: Deploy EKS

You can follow our [Docker](docker-quickstart.md) or [Kubernetes](kubernetes.md) deployment guide. This will walk through a simple AWS deployment of Featureform with our quick start Helm chart containing Postgres and Redis.

Install the [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html) and [`eksctl`](https://eksctl.io/introduction/#installation) then run the following command to create an EKS cluster.

```shell
eksctl create cluster \
--name featureform \
--version 1.24 \
--region us-east-1 \
--nodegroup-name linux-nodes \
--nodes 1 \
--nodes-min 1 \
--nodes-max 4 \
--with-oidc \
--managed
```

Newer versions of `eksctl` require you to separately add a Container Storage Interface (CSI) driver to support Persistent Volume Claims. For complete details on adding the Amazon EBS CSI driver to your EKS cluster, see [Managing the Amazon EBS CSI driver as an Amazon EKS add-on](https://docs.aws.amazon.com/eks/latest/userguide/managing-ebs-csi.html); however, the below examples should allow for a simple deployment.

### Create an Amazon EBS CSI Driver IAM Role

```shell
eksctl create iamserviceaccount \
  --name ebs-csi-controller-sa \
  --namespace kube-system \
  --cluster featureform \
  --region us-east-1 \
  --attach-policy-arn arn:aws:iam::aws:policy/service-role/AmazonEBSCSIDriverPolicy \
  --approve \
  --role-only \
  --role-name AmazonEKS_EBS_CSI_DriverRole
```

### Create the Amazon EBS CSI Add-On

To easily find the account ID you used to create the cluster, run:

```shell
aws sts get-caller-identity --query Account --output text
```

Then, to add the Amazon EBS CSI add-on, run:

```shell
eksctl create addon \
--name aws-ebs-csi-driver \
--cluster featureform \
--region us-east-1 \
--service-account-role-arn arn:aws:iam::<AWS ACCOUNT ID>:role/AmazonEKS_EBS_CSI_DriverRole \
--force
```

## Step 3: Install Helm charts

We'll be installing three Helm Charts: Featureform, the Quickstart Demo, and Certificate Manager.

First we need to add the Helm repositories.

```shell
helm repo add featureform https://storage.googleapis.com/featureform-helm/ 
helm repo add jetstack https://charts.jetstack.io 
helm repo update
```

Prior to installing the Helm charts, export your `FEATUREFORM_HOST` value:

```shell
export FEATUREFORM_HOST=aws-eks-demo.featureform.com
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

## Step 4: Register providers

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
        average_user_transaction[["user_id", "avg_transaction_amt"]], # We can optional include the `timestamp_column` "timestamp" here
        type=ff.Float32,
        inference_store=redis,
    )
    # Register label from our base Transactions table
    fraudulent = ff.Label(
        transactions[["customerid", "isfraud"]], variant="quickstart", type=ff.Bool
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
# Run features through model
```
