# GKE cluster
resource "google_container_cluster" "primary" {
  name                     = "featureform-${var.project_id}-gke"
  location                 = var.region
  min_master_version       = var.gke_version
  # We can't create a cluster with no node pool defined, but we want to only use
  # separately managed node pools. So we create the smallest possible default
  # node pool and immediately delete it.
  remove_default_node_pool = true
  initial_node_count       = 1

  network    = var.vpc_name
  subnetwork = var.vpc_subnet_name

  depends_on = [
    google_compute_network.vpc,
    google_compute_subnetwork.subnet
  ]
}

# Separately Managed Node Pool
resource "google_container_node_pool" "primary_nodes" {
  name       = "${google_container_cluster.primary.name}-node-pool"
  location   = var.region
  cluster    = google_container_cluster.primary.name
  node_count = var.gke_num_nodes

  node_config {
    oauth_scopes = [
      "https://www.googleapis.com/auth/logging.write",
      "https://www.googleapis.com/auth/monitoring",
    ]

    labels = {
      env = var.project_id
    }

    machine_type = var.gke_machine_type
    tags         = ["gke-node", "featureform-${var.project_id}-gke"]
    metadata = {
      disable-legacy-endpoints = "true"
    }
  }

  depends_on = [
    google_compute_network.vpc,
    google_compute_subnetwork.subnet,
    google_container_cluster.primary
  ]
}
