---
description: A quick start guide for Featureform with Docker.
---

# Quickstart (Docker)
This quickstart uses the standalone containered version of Featureform. It can be used to connect to the same providers 
as the Kubernetes Hosted version, but lacks the scaling capabilities.

This quickstart will walk through creating a few simple features, labels, and a training set from a fraud
detection dataset using Postgres and Redis.

### Requirements

- Python 3.7+
- Docker

## Step 1: Install The Featureform CLI
```shell
pip install featureform
```

## Step 2: Pull Containers
We'll use Postgres and Redis as providers in this quickstart. We've created a Featureform Postgres container that
has preloaded data. You can checkout the source data [here](https://featureform-demo-files.s3.amazonaws.com/transactions.csv), 
we'll just be using a sample of it.

We'll also run the Featureform container in the foreground so logs are available.

```shell
docker run -d -p 5432:5432 featureformcom/postgres
docker run -d -p 6379:6379 redis
docker run -p 7878:7878 -p 80:80 featureformcom/featureform
```

Note: If using an M1 Mac, run the following command instead for compatibility 
```shell
docker run -p 7878:7878 -p 80:80 -e ETCD_ARCH="ETCD_UNSUPPORTED_ARCH=arm64" featureformcom/featureform
```

Wait 30-60 seconds to allow the Postgres container to fully initialize the sample data. 

## Step 3: Set Environment Variables
We can store the address of our container endpoint for applying definitions and serving later. 

```shell
export FEATUREFORM_HOST=localhost:7878 
```

## Step 4: Apply Definitions
Featureform definitions can be stored as both a local file and URL. Multiple files can be applied at the same time. 

We'll set the `--insecure` flag since we're using an unencrypted endpoint on the container.

```shell
featureform apply https://featureform-demo-files.s3.amazonaws.com/definitions.py --insecure
```

## Step 5: Dashboard and Serving
The dashboard is available at [localhost](http://localhost)

In the dashboard you should be able to see that 2 Sources, 1 Feature, 1 Label, and 1 Training Set has been created. 

You can check also check the status of the training set with:

```shell
featureform get training-set fraud_training default --insecure
```

When the status of these resources is READY, you can serve them with:
```shell
curl https://featureform-demo-files.s3.amazonaws.com/serving.py -o serving.py && python serving.py
curl https://featureform-demo-files.s3.amazonaws.com/training.py -o training.py && python training.py
```

This will download and run sample serving and training scripts, which will serve a single feature value and a sample 
of a training data set. 

# How Does It Work?
Now that we have everything running, we'll walk through what was done to create the training set and feature.

## Apply
If we download the definitions.py file, we can see what Featureform is doing when we run `featureform apply`.

First we register the Postgres and Redis containers as providers so Featureform is aware of them.
{% code title="definitions.py" %}
```python
import featureform as ff

postgres = ff.register_postgres(
    name="postgres-quickstart",
    host="host.docker.internal",  # The docker dns name for postgres
    port="5432",
    user="postgres",
    password="password",
    database="postgres",
)

redis = ff.register_redis(
    name = "redis-quickstart",
    host="host.docker.internal", # The docker dns name for redis
    port=6379,
)
```
{% endcode %}

We can then register our sources. 

The first source we'll register is our Transactions table that exists in Postgres. This is so Featureform is aware that the
Transactions table exists and can be used as a dependency. 

We can then create a Transformation source off of our Transactions table. This is done using an SQL query that is 
executed in Postgres and saved in a table.
{% code title="definitions.py" %}
```python
ff.register_user("featureform_user").make_default_owner()

transactions = postgres.register_table(
    name="transactions",
    table="Transactions",  # This is the table's name in Postgres
)

@postgres.sql_transformation()
def average_user_transaction():
    return "SELECT CustomerID as user_id, avg(TransactionAmount) " \
           "as avg_transaction_amt from {{transactions.default}} GROUP BY user_id"
```
{% endcode %}

We can then register our feature, label, and training set.

The feature is registered off of the table we created with our SQL Transformation. 

The label is registered off of our base Transactions table.

We can join them together to form a Training Set. A Training Set can be create with one or many features. joined with a 
label. 
{% code title="definitions.py" %}
```python
user = ff.register_entity("user")
# Register a column from our transformation as a feature
average_user_transaction.register_resources(
    entity=user,
    entity_column="user_id",
    inference_store=redis,
    features=[
        {"name": "avg_transactions", "column": "avg_transaction_amt", "type": "float32"},
    ],
)
# Register label from our base Transactions table
transactions.register_resources(
    entity=user,
    entity_column="customerid",
    labels=[
        {"name": "fraudulent", "column": "isfraud", "type": "bool"},
    ],
)

ff.register_training_set(
    "fraud_training",
    label="fraudulent",
    features=["avg_transactions"],
)
```
{% endcode %}

## Serving

We can serve single features from Redis with the Serving Client. The Feature method takes the name of the feature
and an entity that we want the value for.
{% code title="serving.py" %}
```python
from featureform import ServingClient

serving = ServingClient(insecure=True)

user_feat = serving.features(["avg_transactions"], {"user": "C1214240"})
print("User Result: ")
print(user_feat)
```
{% endcode %}

## Training

We can serve a training dataset from Postgres with the Serving Client as well. This example takes the name of the 
training set and returns 25 rows of the training set. 
{% code title="training.py" %}
```python
from featureform import ServingClient

client = ServingClient(insecure=True)
dataset = client.training_set("fraud_training")

for i, batch in enumerate(dataset):
    print(batch)
    if i > 25:
        break
```
{% endcode %}