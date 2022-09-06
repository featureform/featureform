---
description: A quick start guide for Featureform on GCP using Terraform.
---

# Quickstart (GCP)

This quickstart will walk through creating a few simple features, labels, and a training set using Bigquery and Firestore. 
We will use a transaction fraud training set.

### Requirements

- Python 3.7+
- [Terraform v1.2.7+](https://www.terraform.io/downloads)
- [A Google Cloud Platform Project](https://cloud.google.com/)
- [gcloud CLI](https://cloud.google.com/sdk/gcloud)
- [kubectl](https://kubernetes.io/docs/tasks/tools/)
- An available domain/subdomain you own that can be pointed at your cluster IP

## Step 1: Clone the Featureform Repo
```shell
https://github.com/featureform/featureform.git
cd featureform/terraform/gcp
```

## Step 2: Create GCP Services
We'll start BigQuery, Firestore, and Google Kubernetes Engine (GKE). (Specific services can be enabled/disabled as needed in
terraform.auto.tfvars)

We need to set:
```shell
export PROJECT_ID=<your-project-id>           # Your GCP Project ID
export DATASET_ID=featureform                 # The BigQuery Dataset we'll use
export BUCKET_NAME=<your-bucket-name>         # A GCP Storage Bucket where we can store test data
export COLLECTION_ID=featureform_collection   # A Firestore Collection ID
export FEATUREFORM_HOST=<your-domain-name>    # The domain name that you own
```
### Set our CLI to our current project
```shell
cd gcp_services
gcloud auth application-default login   # Gives Terraform access to GCP
gcloud config set project $PROJECT_ID   # Sets  our GCP Project
terraform init; \
terraform apply -auto-approve \
-var="project_id=$PROJECT_ID" \
-var="bigquery_dataset_id=$DATASET_ID" \
-var="storage_bucket_name=$BUCKET_NAME" # Run Terraform \
-var="firestore_collection_name=$COLLECTION_ID"
```

## Step 3: Configure Kubectl
We need to load the GKE config into our kubeconfig.

```shell
gcloud container clusters get-credentials $(terraform output -raw kubernetes_cluster_name) --region $(terraform output -raw region)
```

## Step 4: Install Featureform
We'll use terraform to install Featureform on our GKE cluster.

```shell
cd ../featureform
terraform init; terraform apply -auto-approve -var="featureform_hostname=$FEATUREFORM_HOST"
```

## Step 5: Direct Your Domain To Featureform

Featureform automatically provisions a public certificate for your domain name. 

To connect, you need to point your domain name at the Featureform GKE Cluster.

We can get the IP Address for the cluster using:
```shell
kubectl get ingress | grep "grpc-ingress" | awk {'print $4'} | column -t
```

You need to add 2 records to your DNS provider for the (sub)domain you intend to use:
1. A CAA record for letsencrypt.org value: `0 issuewild "letsencrypt.org"`. This allows letsencrypt to automatically generate a public certificate
2. An A record with the value of the outputted value from above

## Step 6: Load Demo Data
We can load some demo data into BigQuery that we can transform and serve.

```shell
# Load sample data into a bucket in the same project
curl  https://featureform-demo-files.s3.amazonaws.com/transactions_short.csv | gsutil cp - gs://$BUCKET_NAME/transactions.csv

# Load the bucket data into BigQuery
bq load --autodetect --source_format=CSV $DATASET_ID.Transactions gs://$BUCKET_NAME/transactions.csv
```

## Step 7: Install the Featureform SDK

```
pip install featureform
```

## Step 8: Register providers
GCP Registered providers require a GCP Credentials file for a user that has permissions for Firestore and BigQuery.

{% code title="definitions.py" %}

```python
import os
import featureform as ff

project_id = os.getenv("PROJECT_ID")
collection_id=os.getenv("COLLECTION_ID")
dataset_id = os.getenv("DATASET_ID")

firestore = ff.register_firestore(
    name="firestore-quickstart",
    description="A Firestore deployment we created for the Featureform quickstart",
    project_id=project_id,
    collection=collection_id,
    credentials_path="<path-to-bigquery-credentials-file>"
)

bigquery = ff.register_bigquery(
    name="bigquery-quickstart",
    description="A BigQuery deployment we created for the Featureform quickstart",
    project_id=project_id,
    dataset_id=dataset_id,
    credentials_path="<path-to-bigquery-credentials-file>"
)
```
{% endcode %}

Once we create our config file, we can apply it to our Featureform deployment.

```bash
featureform apply definitions.py
```

## Step 9: Define our resources

We will create a user profile for us, and set it as the default owner for all the following resource definitions.

Now we'll register our  user fraud dataset in Featureform.

{% code title="definitions.py" %}
```python
transactions = bigquery.register_table(
    name="transactions",
    description="Fraud Dataset From Kaggle",
    table="Transactions", # This is the table's name in Postgres
)
```
{% endcode %}

Next, we'll define a SQL transformation on our dataset.

{% code title="definitions.py" %}
```python
@bigquery.sql_transformation()
def average_user_transaction():
    return "SELECT CustomerID as user_id, avg(TransactionAmount) " \
           "as avg_transaction_amt from {{transactions.default}} GROUP BY user_id"
    
```
{% endcode %}

Next, we'll register a passenger entity to associate with a feature and label.

{% code title="definitions.py" %}
```python
# Register a column from our transformation as a feature
user = ff.register_entity("user")

average_user_transaction.register_resources(
    entity=user,
    entity_column="user_id",
    inference_store=firestore,
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
```
{% endcode %}

Finally, we'll join together the feature and label intro a training set.

{% code title="definitions.py" %}
```python
ff.register_training_set(
    "fraud_training",
    label="fraudulent",
    features=["avg_transactions"],
)
```
{% endcode %}

Now that our definitions are complete, we can apply it to our Featureform instance.

```bash
featureform apply definitions.py
```

## Step 10: Serve features for training and inference

Once we have our training set and features registered, we can train our model.

```python
import featureform as ff

client = ff.ServingClient()
dataset = client.training_set("fraud_training")
training_set = dataset.shuffle(10000)
for batch in training_set:
    print(batch)
```

Example Output:
```text
Features: [279.76] , Label: False
Features: [254.] , Label: False
Features: [1000.] , Label: False
Features: [5036.] , Label: False
Features: [10.] , Label: False
Features: [884.08] , Label: False
Features: [56.] , Label: False
...
```

We can serve features in production once we deploy our trained model as well.

```python
import featureform as ff

client = ff.ServingClient()
fpf = client.features(["avg_transactions"], {"user": "C1011381"})
print(fpf)
```
Example Output:
```text
[1500.0]
```
