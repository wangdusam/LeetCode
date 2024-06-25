# ------------------ output for composer --------------------- #
output "gcp_composer_name" {
  description = "Name of the compose env"
  value       = "${module.my_composer.gcp_composer_name}"
}

# ---------------- output for composer bucket -----------------#
output "gcp_composer_dags_prefix" {
  description = "Name of the composer bucket"
  value       = "${module.my_composer.gcp_composer_dags_prefix}"
}

# -------------------- output for pubsub ----------------------- #
output "topic_name" {
  description = "Name of the created topic"
  value       = "${module.pubsub.topic_name}"
}

# ------------------- output for bucket --------------------------#
output "storage_name" {
  description = "Name of the composer bucket"
  value       = "${module.gcp_storage.storage_name}"
}

# --------------------- output for CF  --------------------------------#
output "function_name" {
  description = "Name of the CF"
  value       = "${module.google_function_gen2_pubsub.function_name}"
}

# --------------------- output for SM  --------------------------------#
output "secret_manager_id" {
  description = "Name of the SM"
  value       = "${module.secret.secret_manager_id}"
}