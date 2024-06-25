# -------------------------- PROVIDERs -------------------------- #

provider "google" {
  credentials  = file(var.gcp_credential_path)
  project      = var.gcp_project_id
  region       = var.gcp_project_region
  version = "5.18.0"
  impersonate_service_account = var.owner_sa_name
  scopes = [
    "https://www.googleapis.com/auth/cloud-platform",
  ]
}

provider "google-beta" {
  credentials = file(var.gcp_credential_path)
  project     = var.gcp_project_id
  region      = var.gcp_project_region
  zone        = var.gcp_project_zone
  version = "5.18.0"
  impersonate_service_account = var.owner_sa_name
  scopes = [
    "https://www.googleapis.com/auth/cloud-platform",
  ]
}

# -------------------------- Back-End Configuration --------------------------

terraform {
  backend "azurerm" {
    subscription_id      = "#{SUBSCRIPTION_ID}#"
    use_azuread_auth     = "true"
    resource_group_name  = "#{RG_NAME}#"
    storage_account_name = "#{SA_NAME}#"
    container_name       = "#{CONTAINER_NAME}#"
    key                  = "terraform/#{GCP_PROJECT_ID}#/#{TOOL_TF_PIPELINE_NAME}#/Cloudscheduler/tfstate/v_1_2_9"
  }
}

# -------------------------- Project Configuration --------------------

data "google_project" "project" {
  project_id = var.gcp_project_id
}

# ---------------------------- Locals ---------------------------------- #

locals {
  operation_sa_name = element(split("@", var.operation_sa_name),0)
  #endpoint_url = "https://#{GCP_PROJECT_REGION}#-#{GCP_PROJECT_ID}#.cloudfunctions.net/${module.google_cloud_functions.function_name}"
}

# -------------------- Cloud Storage ---------------------------------- #
module "gcp_storage" {
    source              = "acnciotfregistry.accenture.com/accenture-cio/storage/google"
    version             = "#{TF_STORAGEMODULE_VERSION}#"
    project_id          = var.gcp_project_id
    storage_class       = "REGIONAL"
    location            = var.gcp_project_region
    storage_name        = var.gcp_cs_bucket_name
}

# -------------------- PUB/SUB Topic ---------------------------------- #

module "gcp_pubsub" {
    source = "acnciotfregistry.accenture.com/accenture-cio/pubsub/google"       # Module source.
    version = "#{TF_PUBSUBMODULE_VERSION}#"                                     # Module version.
    project_id = var.gcp_project_id
    topic_name = var.topic_cs_name
    intended_event_type = "Application"                                         #or Security
    resources_to_create = "TOPIC"                                               #or TOPIC or SUBSCRIPTION
    publisher_service_accounts  = [var.operation_sa_name]
    subscriber_service_accounts = [var.operation_sa_name]
}

# -------------------- Secret Manager -----------------------------------

module "secret" {
  source        = "acnciotfregistry.accenture.com/accenture-cio/secretmanager/google"
  project_id    = var.gcp_project_id
  version       = "#{TF_SM_MODULE_VERSION}#"
  secret_name   = var.cs_sm_name
  locations     = ["us-centra1","us-east1"]
  secrets       = {
     AZURE_USER = "#{AZURE_USER}#"
     AZURE_PASS = "#{AZURE_PASS}#"
  }
}

# ------------------------- Cloud Funtion ----------------------------- #

module "google_function_gen2_pubsub"{
    source                            = "#{TF_CF_MODULE_SOURCE}#"
    version                           = "#{TF_CF_MODULE_VERSION}#"
    project_id                        = var.gcp_project_id
    google_function_name              = "#{TRIGGER_FUNCTION_NAME}#"
    google_function_entrypoint        = "#{TRIGGER_FUNCTION_ENTRYPOINT}#"
    function_type                     = "pubsub" # https, pubsub, storage
    runtime                           = "python311"
    event_type                        = "google.cloud.pubsub.topic.v1.messagePublished"
    event_pubsub_topic                = element(module.gcp_pubsub.topic_name,0)
    source_dir                        = "./cloudFunction"
    path_to_data_to_upload            = "./cloudFunction.zip"
    google_storage_bucket_name        = module.gcp_storage.storage_name
    google_storage_bucket_object_name = "python/cloudFunction"
}

module "google_cloud_scheduler_job_pubsub" {
  source = "#{TF_CS_MODULE_SOURCE}#"
  version = "#{TF_CS_MODULE_VERSION}#"
  project_id               = var.gcp_project_id 
  cloud_scheduler_pubsubjob = {
  #{TRIGGER_FUNCTION_NAME}#  = {
      region               = var.gcp_project_region
      description          = var.gcp_cs_description
      schedule             = var.gcp_cs_schedule
      time_zone            = var.gcp_cs_timezone
      attempt_deadline     = "600s"
      retry_count          = "3"
      max_retry_duration   = "60s"
      min_backoff_duration = "5s"
      max_backoff_duration = "30s"
      max_doublings        = "3"
      topic_name           = element(module.gcp_pubsub.topic_name,0)
      payload              = var.gcp_cs_payload
    }
  }
}