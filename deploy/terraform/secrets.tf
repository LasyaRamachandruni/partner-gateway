# Secrets are declared here; their values are added out of band (never in Terraform state):
#   gcloud secrets versions add pgw-token-signing-key-dev --data-file=key.pem
# Rotation adds a new version; the gateway stages the new key in its JWKS before signing with it.

locals {
  gateway_secrets         = ["token-signing-key", "mtls-client-key", "mtls-client-cert", "mtls-ca"]
  vehicle_service_secrets = ["mtls-server-key", "mtls-server-cert", "mtls-ca-for-server"]
}

resource "google_secret_manager_secret" "gateway" {
  for_each  = toset(local.gateway_secrets)
  secret_id = "pgw-${each.key}-${var.env}"
  replication {
    auto {}
  }
}

resource "google_secret_manager_secret" "vehicle_service" {
  for_each  = toset(local.vehicle_service_secrets)
  secret_id = "pgw-${each.key}-${var.env}"
  replication {
    auto {}
  }
}

resource "google_secret_manager_secret_iam_member" "gateway_reads_its_secrets" {
  for_each  = google_secret_manager_secret.gateway
  secret_id = each.value.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.gateway.email}"
}

resource "google_secret_manager_secret_iam_member" "vehicle_service_reads_its_secrets" {
  for_each  = google_secret_manager_secret.vehicle_service
  secret_id = each.value.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.vehicle_service.email}"
}
