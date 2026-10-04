# One service account per service, with only the permissions it uses.

resource "google_service_account" "gateway" {
  account_id   = "partner-gateway-${var.env}"
  display_name = "Partner gateway (${var.env})"
}

resource "google_service_account" "vehicle_service" {
  account_id   = "vehicle-service-${var.env}"
  display_name = "Vehicle service (${var.env})"
}

# The vehicle service publishes events; it can't read them.
resource "google_pubsub_topic_iam_member" "vehicle_service_publishes" {
  for_each = google_pubsub_topic.events
  topic    = each.value.id
  role     = "roles/pubsub.publisher"
  member   = "serviceAccount:${google_service_account.vehicle_service.email}"
}

# The gateway consumes command results; it can't publish them.
resource "google_pubsub_subscription_iam_member" "gateway_subscribes" {
  subscription = google_pubsub_subscription.gateway_command_results.id
  role         = "roles/pubsub.subscriber"
  member       = "serviceAccount:${google_service_account.gateway.email}"
}

# Pub/Sub's own service agent must be able to move failing messages to the dead-letter topics.
data "google_project" "this" {}

locals {
  pubsub_agent = "serviceAccount:service-${data.google_project.this.number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}

resource "google_pubsub_topic_iam_member" "dead_letter_publisher" {
  for_each = google_pubsub_topic.dead_letter
  topic    = each.value.id
  role     = "roles/pubsub.publisher"
  member   = local.pubsub_agent
}

resource "google_pubsub_subscription_iam_member" "dead_letter_ack" {
  subscription = google_pubsub_subscription.gateway_command_results.id
  role         = "roles/pubsub.subscriber"
  member       = local.pubsub_agent
}
