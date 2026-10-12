output "topics" {
  value = { for k, t in google_pubsub_topic.events : k => t.id }
}

output "gateway_subscription" {
  value = google_pubsub_subscription.gateway_command_results.id
}

output "service_accounts" {
  value = {
    gateway         = google_service_account.gateway.email
    vehicle_service = google_service_account.vehicle_service.email
  }
}

output "image_repository" {
  value = "${var.region}-docker.pkg.dev/${var.project}/${google_artifact_registry_repository.images.repository_id}"
}

output "gateway_url" {
  value = var.deploy_services ? google_cloud_run_v2_service.gateway[0].uri : null
}

output "vehicle_service_url" {
  value = var.deploy_services ? google_cloud_run_v2_service.vehicle[0].uri : null
}

output "admin_job" {
  value = var.deploy_services ? google_cloud_run_v2_job.admin[0].name : null
}

output "signing_key_secret" {
  value = google_secret_manager_secret.gateway["token-signing-key"].secret_id
}

output "cloud_sql_instance" {
  value = google_sql_database_instance.main.connection_name
}
