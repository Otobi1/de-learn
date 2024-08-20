variable "credentials" {
  description = "My Credentials"
  default     = "./keys/mycred.json"
}

variable "project" {
  description = "Project"
  default     = "tonal-works-***-**"
}

variable "region" {
  description = "Region"
  default     = "europe-west3"
}

variable "location" {
  description = "Project Location"
  default     = "EU"
}

variable "bq_dataset_name" {
  description = "My Bigquery Dataset Name"
  default     = "terrademo_dataset"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  default     = "tonal-works-***-**-terra-bucket"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}