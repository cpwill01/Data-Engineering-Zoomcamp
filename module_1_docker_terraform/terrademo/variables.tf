variable "credentials_path" {
  description = "Full path to credentials file"
  type        = string
  sensitive   = true
}

variable "project" {
  description = "Project ID (from cloud provider)"
  type        = string
  default     = "decamp-450109"
}

variable "location" {
  description = "Project Location"
  type        = string
  default     = "europe-north1"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  type        = string
  default     = "STANDARD"
}
