terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.6.0"
    }
  }
}

provider "google" {
  credentials = file(var.credentials)
  project     = var.project
  region      = var.region
}


resource "google_storage_bucket" {
  name          = var.gcs_bucket_name
  location      = var.location
  force_destroy = true


  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}



resource "google_bigquery_dataset" "de_zoomcamp_project_dataset" {
  dataset_id = var.bq_dataset_name1
  location   = var.location
}
resource "google_bigquery_dataset" "dbt_development_dataset" {
  dataset_id = var.bq_dataset_name2
  location   = var.location
}
resource "google_bigquery_dataset" "dbt_production_dataset" {
  dataset_id = var.bq_dataset_name3
  location   = var.location
}
