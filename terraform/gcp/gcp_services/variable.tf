# GCP Services Variable
variable "project_id" {
    type = string
    default = ""
    description = "The project used to deploy the Featureform infrastructure"
}

# VPC 
variable "create_vpc" {
  type = bool
  default = true
  description = "Create a new vpc"
}

variable "vpc_name" {
  type = string
  default = "featureform_vpc"
  description = "The name of vpc to use or create"
}

variable "vpc_subnet_name" {
  type = string
  default = "featureform-subnet"
  description = "The name of subnet to use or create"
}

variable "region" {
    type = string
    default = "us-central1"
    description = "The region to deploy GKE"
}


# GKE Variables
variable "gke_version" {
  type = string
  default = "1.21.14-gke.2100"
  description = "The version of GKE to install"
}
variable "gke_num_nodes" {
  type = number 
  default     = 2
  description = "number of gke nodes"
}
variable "gke_machine_type" {
  type = string
  default = "e2-standard-2"
  description = "the machine type to level for GKE nodes"
}
variable "cluster_name" {
  type = string
  default = "featureform_gke"
  description = "the GKE cluster name"
}

# BigQuery Variables
variable "create_bigquery_dataset" {
  type = bool
  default = true 
  description = "To create a new bigquery dataset"
}
variable "bigquery_dataset_id" {
  type = string
  default = "featureform_bigquery_dataset"
  description = "The id of the bigquery dataset"
}
variable "bigquery_dataset_name" {
  type = string
  default = "featureform_bigquery_dataset"
  description = "The name of the bigquery dataset"
}
variable "bigquery_description" {
  type = string
  default = "the featureform dataset"
  description = "The description of the bigquery dataset"
}
variable "bigquery_location" {
  type = string
  default = "US"
  description = "The location of the bigquery dataset"
}

# Firestore Variables
variable "enable_firestore_api" {
  type = bool
  default = false
  description = "Enable Firestore for your project. If it is already enabled, set the value to false"
}
variable "create_firestore_collection" {
  type = bool
  default = true
  description = "To create a new firestore collection to be used by Featureform"
}
variable "firestore_collection_name" {
  type = string
  default = "featureform_collection"
  description = "The name of the firestore collection"
}
variable "firestore_location_id" {
  type = string
  default = "US"
  description = "The location of the firestore"
}

variable "create_storage_bucket" {
  type = bool
  default = true
  description = "If a storage bucket needs to be created"
}

variable "storage_bucket_name" {
  type = string
  description = "The name of a storage bucket to create"
}

variable "storage_bucket_location" {
  type = string
  default = "US"
  description = "The location of a storage bucket to create"
}