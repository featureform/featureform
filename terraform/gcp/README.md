# Terraform GCP Instructions
The Terraform scripts install Featureform on GCP. 

### Prerequisites
- Terraform
- gcloud SDK
- kubectl


## Create GCP Services
1. Run ``cd gcp_services``
2. Run ``gcloud auth application-default login`` to give Terraform access to GCP
3. Update the `project_id` variable in`terraform.auto.tfvars` file and if you want create Firestore and BigQuery instances. 
3. Run ``terraform init``
4. Run ``terraform plan``
5. Run ``terraform apply -`` then type ``yes`` when prompted

## Configure Kubectl
1. Run ``gcloud container clusters get-credentials $(terraform output -raw kubernetes_cluster_name) --region $(terraform output -raw region)`` in `gcp_services` folder

## Install Featureform
1. Run ``cd ../featureform``
3. Update the `featureform_hostname` variable in `terraform.auto.tfvars` file 
3. Run ``terraform init``
4. Run ``terraform plan``
5. Run ``terraform apply -`` then type ``yes`` when prompted