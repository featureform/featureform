# Featureform
## Cert Manager
variable "cert_manager_version" {
  type = string
  default = "v1.8.0"
  description = "The version for charts.jetstack.io's cert-manager helm chart"
}
variable "cert_manager_namespace" {
  type = string
  default = "cert-manager"
  description = "the name space of cert-manager"
}

## Featureform Core
variable "featureform_version" {
  type = string
  default = "v0.1.0"
  description = "The version for Featureform helm chart"
}
variable "featureform_hostname" {
  type = string
  default = ""
  description = "The domain name for featureform"
}
variable "featureform_public_cert" {
  type = string
  default = "true"
  description = ""
}
variable "featureform_namespace" {
  type = string
  default = "featureform"
  description = "the namespace for Featureform"
}
variable "gcp_services_dependency" {
  type = string
  default = ""
  description = "dependency variable"
}

## Featureform Quickstart
variable "install_quickstart" {
  type = bool
  default = false
  description = "Install Featureform Quickstart?"
}
variable "quickstart_version" {
  type = string
  default = "v0.1.0"
  description = "The version of Featureform Quickstart helm chart."
}
