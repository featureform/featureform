resource "google_storage_bucket" "bucket" {
  count         = var.create_storage_bucket == true ? 1 : 0
  project       = var.project_id
  name          = var.storage_bucket_name
  location      = var.storage_bucket_location
}