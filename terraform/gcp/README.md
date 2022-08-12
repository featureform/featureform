# Terraform GCP Instructions
The Terraform scripts install Featureform on GCP. 

### Prerequisites
- Terraform
- gcloud SDK
- kubectl

1. Run ``gcloud init`` to initialize the gcloud SDK
2. Run ``gcloud auth application-default login`` to give Terraform access to GCP
3. Update the `terraform.auto.tfvars` file 
3. Run ``terraform init``
4. Run ``terraform plan``
5. Run ``terraform apply -`` then type ``yes`` when prompted

