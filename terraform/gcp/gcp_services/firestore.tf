resource "google_app_engine_application" "app" {
  project     = var.project_id
  location_id = var.firestore_location_id
  database_type = "CLOUD_FIRESTORE"
  count = var.enable_firestore_api == true ? 1 : 0
}

resource google_project_service "firestore" {
  count = var.enable_firestore_api == true ? 1 : 0
  service = "firestore.googleapis.com"
  disable_dependent_services = true
}

resource "google_firestore_document" "mydoc" {
  project     = var.project_id
  collection  = var.firestore_collection_name
  document_id = "welcome-to-featureform"
  fields      = "{\"featureform\":{\"mapValue\":{\"fields\":{\"welcome\":{\"stringValue\":\"to featureform\"}}}}}"

  count = var.create_firestore_collection == true ? 1 : 0
}