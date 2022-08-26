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
We'll start BigQuery, Firestore, and Google Kubernetes Engine (GKE). (Specific services can be enabled/disabled as needed)

1. Run ``cd gcp_services``
2. Run ``gcloud auth application-default login`` to give Terraform access to GCP
3. Update the `project_id` variable in`terraform.auto.tfvars` file and if you want create Firestore and BigQuery instances.
3. Run ``terraform init``
4. Run ``terraform plan``
5. Run ``terraform apply -`` then type ``yes`` when prompted

## Step 3: Configure Kubectl
We need to load the GKE config into our kubeconfig.

1. Run ``gcloud container clusters get-credentials $(terraform output -raw kubernetes_cluster_name) --region $(terraform output -raw region)`` in `gcp_services` folder

## Step 3: Install Featureform
We'll use terraform to install Featureform on our GKE cluster.

1. Run ``cd ../featureform``
2. Update the `featureform_hostname` variable in `terraform.auto.tfvars` file (This is a domain name that you own)
3. Run ``terraform init``
4. Run ``terraform plan``
5. Run ``terraform apply -`` then type ``yes`` when prompted

## Step 4: Direct Your Domain To Featureform

Featureform automatically provisions a public certificate for your domain name. 

To connect, you need to point your domain name at the Featureform GKE Cluster.

We can get the IP Address for the cluster using:
```shell
kubectl get ingress | grep "grpc-ingress" | awk {'print $4'} | column -t
```

Creating an A record for your domain with the outputted IP address. 


## Step 5: Load Demo Data
We can load some demo data into BigQuery that we can transform and serve. 

We need to set:
```shell
export PROJECT_ID=<your-project-id>
export DATASET_ID="featureform"
export BUCKET_NAME="<your-bucket-name>"
```

```shell
# Set our CLI to our current project
gcloud config set project $PROJECT_ID

# Make our featureform dataset
bq mk -d $DATASET_ID

# Load sample data into a bucket in the same project
curl  https://featureform-demo-files.s3.amazonaws.com/transactions.csv | gsutil cp - gs://$BUCKET_NAME/transactions.csv

# Load the bucket data into BigQuery
bq load --autodetect --source_format=CSV $DATASET_ID.transactions gs://$BUCKET_NAME/transactions.csv
```

## Step 5: Install the Featureform SDK

```
pip install featureform
```

## Step 6: Register providers
GCP Registered providers require a GCP Credentials file for a user that has permissions for Firestore and BigQuery.

{% code title="definitions.py" %}
```python
import featureform as ff

redis = ff.register_firestore(
    name="firestore-quickstart",
    description="A Firestore deployment we created for the Featureform quickstart",
    project_id="<your-gcp-project-id>",
    collection="<your-collection-id>",
    credentials_path="<path-to-bigquery-credentials-file>" 
)

bigquery = ff.register_bigquery(
    name="bigquery-quickstart",
    description="A BigQuery deployment we created for the Featureform quickstart",
    project_id="<your-gcp-project-id>",
    dataset_id="featureform",
    credentials_path="<path-to-bigquery-credentials-file>"
)
```
{% endcode %}

Once we create our config file, we can apply it to our Featureform deployment.

```bash
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
transactions = bigquery.register_table(
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
@bigquery.sql_transformation(variant="quickstart")
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
    inference_store=firestore,
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

## Step 8: Serve features for training and inference

Once we have our training set and features registered, we can train our model.

```python
import featureform as ff

client = ff.ServingClient()
dataset = client.training_set("fraud_training", "quickstart")
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
