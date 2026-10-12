resource "google_artifact_registry_repository" "images" {
  repository_id = "partner-gateway-${var.env}"
  location      = var.region
  format        = "DOCKER"
  description   = "Container images for the partner gateway and vehicle service"
  depends_on    = [google_project_service.apis]
}
