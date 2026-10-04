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
