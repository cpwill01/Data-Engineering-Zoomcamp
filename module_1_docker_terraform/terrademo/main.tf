terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "6.21.0"
    }
  }
}

provider "google" {
  credentials = file(var.credentials_path)
  project     = var.project
  region      = var.location
}

resource "google_storage_bucket" "demo_bucket" {
  name          = "demo-bucket-1"
  location      = var.location
  force_destroy = true
  
  # how long to wait for a multipart (chunked) upload
  lifecycle_rule {
    condition {
      age = 1 #in days
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}

resource "google_bigquery_dataset" "demo_dataset" {
  dataset_id    = "demo_dataset"
  description   = "This is a test description"
  location      = var.location
}
