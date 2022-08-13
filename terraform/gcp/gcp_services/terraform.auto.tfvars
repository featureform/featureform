# GCP Services
project_id = ""

## VPC
create_vpc      = true
vpc_name        = "featureform-vpc"
vpc_subnet_name = "featureform-subnet"
region          = "us-central1"

## BigQuery Variables
create_bigquery_dataset = false
bigquery_dataset_id     = "ff_bigquery_dataset"
bigquery_dataset_name   = "ff_bigquery_dataset"
bigquery_description    = "bigquery dataset for featureform"
bigquery_location       = "US"

## Firestore Variables
enable_firestore_api        = false
firestore_location_id       = "US"
create_firestore_collection = false
firestore_collection_name   = "featureform-collection"

## GKE 
gke_version      = "1.21.14-gke.2100" # do not modify
gke_num_nodes    = 2
gke_machine_type = "e2-standard-2"
