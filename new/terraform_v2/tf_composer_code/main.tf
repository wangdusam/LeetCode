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
    key                  = "terraform/#{GCP_PROJECT_ID}#/#{TOOL_TF_PIPELINE_NAME}#/Composer/tfstate/v_1_2_9"
  }
}

# -------------------------- Project Configuration --------------------------#
# Getting project resource

data "google_project" "project" {
  project_id = var.gcp_project_id
}

locals {
  operation_sa_name = element(split("@", var.operation_sa_name),0)
  owner_sa_name_short = element(split("@", var.owner_sa_name),0)
  air_id = lookup(data.google_project.project.labels, "airid")
  pipeline_name = replace("#{TOOL_TF_PIPELINE_NAME}#", ".", "_")
}

# --------------------------  Composer Module ----------------------------- #

module "my_composer"{
 source                      = "#{TF_MODULE_SOURCE}#"
 version                     = "#{TF_COMPOSER_MODULE_VERSION}#"
 environment_service_account = local.operation_sa_name
 region_name                 = var.gcp_project_region
 vpc_name                    = var.gcp_vpc_name
 project_id                  = var.gcp_project_id
 labels                      = {pipeline_id=lower(local.pipeline_name)} # Set of key/value pairs that are assigned to the Composer service labels [default = {}]
 image_version               = var.composer_image_version               # Enter the desired version of the software to be executed in the environment.
 environment_size            = "ENVIRONMENT_SIZE_MEDIUM"
 
}

################### locals ################

 locals {
     bucket_name = trimsuffix(trimprefix(module.my_composer.gcp_composer_dags_prefix, "gs://"), "/dags")
 }

# -------------------------- Dag file upload --------------------------

module "gcp_storage_file_upload" {
  source         = "acnciotfregistry.accenture.com/accenture-cio/storagefileupload/google"
  version        = "#{TF_STORAGEFILEUPLOADMODULE_VERSION}#"
  project_id     = var.gcp_project_id
  storage_name   = trimsuffix(trimprefix(module.my_composer.gcp_composer_dags_prefix, "gs://"), "/dags")
  directory_name = var.azuredevops_libs_dag_path
}

# -------------------- PUB/SUB Topic ------------------------------ #

module "pubsub" {
    source                        = "acnciotfregistry.accenture.com/accenture-cio/pubsub/google"
    version                       = "#{TF_PUBSUBMODULE_VERSION}#"
    project_id                    = var.gcp_project_id
    topic_name                    = var.topic_name
    intended_event_type           = "Application" #or Security
    resources_to_create           = "TOPIC"       #or TOPIC or SUBSCRIPTION
}

# -------------------- Cloud Storage ----------------------------- #

module "gcp_storage" {
    source              = "acnciotfregistry.accenture.com/accenture-cio/storage/google"           # Module source
    version             = "#{TF_STORAGEMODULE_VERSION}#"                                          # Module version
    project_id          = var.gcp_project_id                                                      # Application team should provide the required value for Project ID.
    storage_class       = "REGIONAL"
    location            = var.gcp_project_region
    storage_name        = var.gcp_bucket_name
}

# -------------------- Secret Manager -----------------------------------

module "secret" {
  source        = "acnciotfregistry.accenture.com/accenture-cio/secretmanager/google"
  project_id    = var.gcp_project_id
  version       = "#{TF_SM_MODULE_VERSION}#"
  secret_name   = var.sm_name
  locations     = ["us-centra1","us-east1"]
  secrets       = {
     AZURE_USER = "#{AZURE_USER}#"
     AZURE_PASS = "#{AZURE_PASS}#"
  }
}

# -------------------- Cloud Funtion for Destroy-----------------------

module "google_function_gen2_pubsub" {
  source                            = "#{TF_CF_MODULE_SOURCE}#"
  version                           = "#{TF_CF_MODULE_VERSION}#"
  project_id                        = var.gcp_project_id
  google_function_name              = "#{DESTROY_FUNCTION_NAME}#"
  google_function_entrypoint        = "#{DESTROY_ENTRYPOINT}#"
  function_type                     = "pubsub" # https, pubsub, storage
  runtime                           = "python311"
  event_type                        = "google.cloud.pubsub.topic.v1.messagePublished"
  event_pubsub_topic                = module.pubsub.topic_name[0]
  source_dir                        = "./DestroyCloudFunction"
  path_to_data_to_upload            = "./DestroyCloudFunction.zip"
  google_storage_bucket_name        = module.gcp_storage.storage_name
  google_storage_bucket_object_name = "python/DestroyCloudFunction"
  environment_variables             = {"AZDEVOPS_USR" = "Azure user", "AZDEVOPS_PASS" = "Azure user password"}  
}