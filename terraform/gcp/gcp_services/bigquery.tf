module "bigquery" {
  source  = "terraform-google-modules/bigquery/google"
  version = "~> 4.4"
  count = var.create_bigquery_dataset == true ? 1 : 0

  dataset_id                  = var.bigquery_dataset_id
  dataset_name                = var.bigquery_dataset_name
  description                 = var.bigquery_description
  project_id                  = var.project_id
  location                    = var.bigquery_location
}
