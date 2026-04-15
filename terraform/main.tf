terraform {
  required_version = ">= 1.14.8"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.0"
    }
  }
}

provider "google" {
  project = var.project
  region  = var.region
}

resource "random_id" "bucket_suffix" {
  byte_length = 2
}

resource "google_storage_bucket" "data_lake" {
  name          = "${var.gcs_bucket_name}-${random_id.bucket_suffix.hex}"
  location      = var.location
  storage_class = var.gcs_storage_class
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type = "Delete"
    }
  }

  public_access_prevention = "enforced"
}

resource "google_storage_bucket" "kestra_storage" {
  name          = "${var.kestra_storage_bucket_name}-${random_id.bucket_suffix.hex}"
  location      = var.location
  storage_class = "STANDARD"
  force_destroy = true

  public_access_prevention = "enforced"
}

resource "google_bigquery_dataset" "gold" {
  dataset_id                 = var.bq_dataset_gold
  location                   = var.location
  delete_contents_on_destroy = true
}

resource "google_bigquery_dataset" "external" {
  dataset_id                 = var.bq_dataset_external
  location                   = var.location
  delete_contents_on_destroy = true
}