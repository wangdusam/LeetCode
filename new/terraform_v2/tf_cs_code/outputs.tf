#------------------ output for pubsub ------------------------- #
output "topic_name" {
  description = "Name of the created topic"
  value       = "${module.gcp_pubsub.topic_name}"
}

# ------------------ output for bucket --------------------------#
output "storage_name" {
  description = "Name of the bucket"
  value       = "${module.gcp_storage.storage_name}"
}

#--------------------  output for CF  ----------------------------#
output "function_name" {
  description = "Name of the CF"
  value       = "${module.google_function_gen2_pubsub.function_name}"
}

#------------- scheduler job name with pubsub as a target ---------#
output "pubsub_jobs_name" {
  description = "Name of the scheduler job"
  value       = "${module.google_cloud_scheduler_job_pubsub.pubsub_jobs_name}"
}

# --------------------- output for SM  --------------------------------#
output "secret_manager_id" {
  description = "Name of the SM"
  value       = "${module.secret.secret_manager_id}"
}