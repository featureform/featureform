provider "google" {
  project = var.project_id
}

module "gcp_services" {
  source = "./gcp_services"

  project_id                  = var.project_id
  create_vpc                  = var.create_vpc
  vpc_name                    = var.vpc_name
  vpc_subnet_name             = var.vpc_subnet_name
  region                      = var.region
  gke_version                 = var.gke_version
  gke_num_nodes               = var.gke_num_nodes
  gke_machine_type            = var.gke_machine_type
  create_bigquery_dataset     = var.create_bigquery_dataset
  bigquery_dataset_id         = var.bigquery_dataset_id
  bigquery_dataset_name       = var.bigquery_dataset_name
  bigquery_description        = var.bigquery_description
  bigquery_location           = var.bigquery_location
  enable_firestore_api        = var.enable_firestore_api
  create_firestore_collection = var.create_firestore_collection
  firestore_collection_name   = var.firestore_collection_name
  firestore_location_id       = var.firestore_location_id
}


module "featureform" {
  source = "./featureform"

  cert_manager_version    = var.cert_manager_version
  featureform_version     = var.featureform_version
  featureform_hostname    = var.featureform_hostname
  featureform_public_cert = var.featureform_public_cert
  install_quickstart      = var.install_quickstart
  quickstart_version      = var.quickstart_version
  gcp_services_dependency = module.gcp_services.kubernetes_node_pool
}
