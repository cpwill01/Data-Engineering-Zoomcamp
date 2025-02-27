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

resource "google_storage_bucket" "taxi_demo_bucket_94857" {
  name          = "demo-bucket-taxi"
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

resource "google_bigquery_dataset" "demo_dataset_2" {
  dataset_id    = "demo_dataset_2"
  description   = "This is a test description"
  location      = var.location
}
