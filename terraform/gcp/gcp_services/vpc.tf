provider "google" {
  project = var.project_id
  region  = var.region
}

# VPC
resource "google_compute_network" "vpc" {
  name                    = var.vpc_name
  auto_create_subnetworks = "false"
  count = var.create_vpc == true ? 1 : 0
}

# Subnet
resource "google_compute_subnetwork" "subnet" {
  name          = var.vpc_subnet_name
  region        = var.region
  network       = google_compute_network.vpc[0].name
  ip_cidr_range = "10.10.0.0/24"
  count = var.create_vpc == true ? 1 : 0
  depends_on = [
    google_compute_network.vpc
  ]
}
