variable "project" {
  description = "GCP Project ID"
  type        = string
}

variable "region" {
  description = "GCP Region"
  type        = string
}

variable "location" {
  description = "GCP Location"
  type        = string
}

variable "gcs_bucket_name" {
  description = "GCS Bucket Name (Data Lake)"
  type        = string
}

variable "kestra_storage_bucket_name" {
  description = "GCS Bucket Name (Kestra Internal Storage)"
  type        = string
}

variable "gcs_storage_class" {
  description = "GCS Storage Class"
  type        = string
  default     = "STANDARD"
}

variable "bq_dataset_gold" {
  description = "BigQuery dataset for analytical Gold layer models"
  type        = string
}

variable "bq_dataset_external" {
  description = "BigQuery dataset for external tables over GCS Silver Parquet files"
  type        = string
}