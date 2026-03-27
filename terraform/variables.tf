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
  description = "GCS Bucket Name"
  type        = string
}

variable "gcs_storage_class" {
  description = "GCS Storage Class"
  type        = string
  default     = "STANDARD"
}

variable "bq_dataset_bronze" {
  description = "BigQuery dataset for Raw data (Bronze layer)"
  type        = string
}

variable "bq_dataset_silver" {
  description = "BigQuery dataset for Cleaned/Transformed data (Silver layer)"
  type        = string
}

variable "bq_dataset_gold" {
  description = "BigQuery dataset for Analytics/Reporting data (Gold layer)"
  type        = string
}