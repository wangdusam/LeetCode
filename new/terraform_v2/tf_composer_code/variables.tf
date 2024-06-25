# credentials = "${file("${var.gcp_credential_path}")}"
variable "gcp_credential_path" {
  type        = string
  description = "the gcp credential path"
  default     = ""
}

# project     = "sbx-4985-intpilotproc-dab2aa01"
variable "gcp_project_id" {
  type        = string
  description = "the internal project id of your project "
  default     = ""
}

# region      = "us-east1"
variable "gcp_project_region" {
  type        = string
  description = "the internal project id of your project"
  default     = ""
}

# zone        = "us-east1-d"
variable "gcp_project_zone" {
  type        = string
  description = "the internal project id of your project"
  default     = ""
}

# gcp_vpc_name = "processing-vpc-subnetwork"
variable "gcp_vpc_name" {
  type        = string
  description = "the name of the VPC to be used"
  default     = ""
}

variable "operation_sa_name" {
  description = "ID of the service account to operate composer"
  type        = string
}


variable "owner_sa_name" {
  description = "ID of the service account owner"
  type        = string
}

variable "azuredevops_libs_dag_path" {
  type        = string
  description = "path to the DAG file"
  default     = ""
}

variable "gcp_bucket_name" {
  type        = string
  description = "The bucket to store dataproc files"
}

variable "topic_name" {
  type        = string
  description = "The topic name"
}

variable azuredevops_destroy_dag_path {
  type        = string
  description = "path to the DAG file"
  default     = ""
}

variable ddl_path {
  type        = string
  description = "path to the ddl files"
  default     = ""
}

variable composer_image_version {
  type        = string
  description = "composer_image_version"
  default     = ""
}

variable azuredevops_single_dag_path {
  type        = string
  description = "path to the single DAG file"
  default     = ""
}

variable azuredevops_comm_mod_path {
  type        = string
  description = "path to the commom modules"
  default     = ""
}
variable azuredevops_conf_files_path {
  type        = string
  description = "path to the conf_files"
  default     = ""
}

variable "project_id" {
  type        = string
  description = "The project id to wire-up the logging."
  default     = ""
}

variable "sm_name" {
  type        = string
  description = "Secret manager name"
}