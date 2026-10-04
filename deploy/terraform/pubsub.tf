# Event topics, each with a dead-letter topic, and the gateway's subscription.
# Matches the bus semantics in pgw/bus: at-least-once delivery, exponential redelivery
# backoff, and dead-lettering after var.max_delivery_attempts.

locals {
  topics = ["vehicle-command-results", "vehicle-telemetry"]
}

resource "google_pubsub_topic" "events" {
  for_each                   = toset(local.topics)
  name                       = "${each.key}-${var.env}"
  message_retention_duration = "86400s"
}

resource "google_pubsub_topic" "dead_letter" {
  for_each = toset(local.topics)
  name     = "${each.key}-${var.env}-dead-letter"
}

resource "google_pubsub_subscription" "gateway_command_results" {
  name                       = "gateway-command-results-${var.env}"
  topic                      = google_pubsub_topic.events["vehicle-command-results"].id
  ack_deadline_seconds       = 30
  message_retention_duration = "86400s"

  retry_policy {
    minimum_backoff = "1s"
    maximum_backoff = "60s"
  }

  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.dead_letter["vehicle-command-results"].id
    max_delivery_attempts = var.max_delivery_attempts
  }
}

# Keep dead letters around for inspection and replay.
resource "google_pubsub_subscription" "dead_letter_inspect" {
  for_each                   = toset(local.topics)
  name                       = "${each.key}-${var.env}-dead-letter-inspect"
  topic                      = google_pubsub_topic.dead_letter[each.key].id
  message_retention_duration = "604800s"
}
