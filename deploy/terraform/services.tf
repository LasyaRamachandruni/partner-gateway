# APIs, network, Memorystore, Cloud SQL and the two Cloud Run services plus an admin job.
#
#   partner ──HTTPS──► Cloud Run: partner-gateway ──gRPC (TLS + Google ID token)──► Cloud Run: vehicle-service
#                        │   │   └─ direct VPC egress ─► Memorystore (rate limits, idempotency keys)
#                        │   └─ Cloud SQL connector ──► Cloud SQL Postgres (partners, commands, webhook outbox)
#                        └─ Pub/Sub subscription ◄──────── vehicle-service publishes command results
#
# Cloud Run terminates TLS and can't pass client certificates through, so between the two
# services IAM replaces mTLS: only the gateway's service account may invoke the vehicle service.

locals {
  apis = [
    "artifactregistry.googleapis.com",
    "cloudbuild.googleapis.com",
    "compute.googleapis.com",
    "iam.googleapis.com",
    "pubsub.googleapis.com",
    "redis.googleapis.com",
    "run.googleapis.com",
    "secretmanager.googleapis.com",
    "sqladmin.googleapis.com",
  ]
  image     = "${var.region}-docker.pkg.dev/${var.project}/${google_artifact_registry_repository.images.repository_id}/partner-gateway:${var.image_tag}"
  redis_url = "redis://${google_redis_instance.main.host}:${google_redis_instance.main.port}/0"
  database_url = format(
    "postgresql+asyncpg://%s:%s@/%s?host=/cloudsql/%s",
    google_sql_user.pgw.name,
    random_password.db.result,
    google_sql_database.pgw.name,
    google_sql_database_instance.main.connection_name,
  )
}

resource "google_project_service" "apis" {
  for_each           = toset(local.apis)
  service            = each.value
  disable_on_destroy = false
}

# -- network: Cloud Run reaches Memorystore through direct VPC egress -------------------------

resource "google_compute_network" "main" {
  name                    = "pgw-${var.env}"
  auto_create_subnetworks = false
  depends_on              = [google_project_service.apis]
}

resource "google_compute_subnetwork" "run" {
  name                     = "pgw-run-${var.env}"
  region                   = var.region
  network                  = google_compute_network.main.id
  ip_cidr_range            = var.subnet_cidr
  private_ip_google_access = true
}

# -- Memorystore for Redis ----------------------------------------------------------------------

resource "google_redis_instance" "main" {
  name               = "pgw-${var.env}"
  region             = var.region
  tier               = "BASIC"
  memory_size_gb     = var.redis_memory_gb
  redis_version      = "REDIS_7_2"
  authorized_network = google_compute_network.main.id
  connect_mode       = "DIRECT_PEERING"
  depends_on         = [google_project_service.apis]
}

# -- Cloud SQL for PostgreSQL -------------------------------------------------------------------

resource "google_sql_database_instance" "main" {
  name                = "pgw-${var.env}"
  region              = var.region
  database_version    = "POSTGRES_16"
  deletion_protection = var.deletion_protection

  settings {
    tier              = var.db_tier
    edition           = "ENTERPRISE"
    availability_type = "ZONAL"
    disk_size         = 10
    disk_autoresize   = true

    backup_configuration {
      enabled = true
    }

    # db-f1-micro defaults to 25 connections; each gateway instance holds up to 2 x PGW_DB_POOL_SIZE.
    database_flags {
      name  = "max_connections"
      value = tostring(var.db_max_connections)
    }

    # No authorized networks: the only way in is the Cloud SQL connector, which checks IAM.
    ip_configuration {
      ipv4_enabled = true
      ssl_mode     = "ENCRYPTED_ONLY"
    }
  }

  depends_on = [google_project_service.apis]
}

resource "google_sql_database" "pgw" {
  name     = "pgw"
  instance = google_sql_database_instance.main.name
}

resource "random_password" "db" {
  length  = 32
  special = false
}

resource "google_sql_user" "pgw" {
  name     = "pgw"
  instance = google_sql_database_instance.main.name
  password = random_password.db.result
}

# Connection strings go to Secret Manager, never into the services' plain environment.
# (The database password is in the Terraform state: keep the state in a restricted bucket.)
resource "google_secret_manager_secret" "runtime" {
  for_each  = toset(["database-url", "redis-url"])
  secret_id = "pgw-${each.key}-${var.env}"
  replication {
    auto {}
  }
  depends_on = [google_project_service.apis]
}

resource "google_secret_manager_secret_version" "database_url" {
  secret      = google_secret_manager_secret.runtime["database-url"].id
  secret_data = local.database_url
}

resource "google_secret_manager_secret_version" "redis_url" {
  secret      = google_secret_manager_secret.runtime["redis-url"].id
  secret_data = local.redis_url
}

resource "google_secret_manager_secret_iam_member" "gateway_reads_runtime" {
  for_each  = google_secret_manager_secret.runtime
  secret_id = each.value.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.gateway.email}"
}

resource "google_project_iam_member" "gateway_cloudsql_client" {
  project = var.project
  role    = "roles/cloudsql.client"
  member  = "serviceAccount:${google_service_account.gateway.email}"
}

# -- Cloud Run: vehicle service -----------------------------------------------------------------

resource "google_cloud_run_v2_service" "vehicle" {
  count               = var.deploy_services ? 1 : 0
  name                = "vehicle-service-${var.env}"
  location            = var.region
  ingress             = "INGRESS_TRAFFIC_ALL" # IAM-authenticated: only the gateway may invoke it
  deletion_protection = false

  template {
    service_account = google_service_account.vehicle_service.email

    # The simulated fleet keeps its state in memory, so there is one instance.
    scaling {
      min_instance_count = 0
      max_instance_count = 1
    }

    containers {
      image = local.image
      args  = ["vehicle-service"]

      ports {
        name           = "h2c" # gRPC: HTTP/2 end to end, TLS terminated by Cloud Run
        container_port = 50051
      }

      resources {
        limits = {
          cpu    = "2"
          memory = "1Gi"
        }
        cpu_idle          = false # commands finish in the background after the RPC returns
        startup_cpu_boost = true
      }

      env {
        name  = "PGW_ENV"
        value = var.env
      }
      env {
        name  = "PGW_GRPC_TLS"
        value = "none"
      }
      env {
        name  = "PGW_BUS"
        value = "pubsub"
      }
      env {
        name  = "PGW_PUBSUB_PROJECT"
        value = var.project
      }
      env {
        name  = "PGW_PUBSUB_SUFFIX"
        value = "-${var.env}"
      }
      env {
        name  = "PGW_VEHICLE_BACKEND"
        value = var.vehicle_backend
      }
      env {
        name  = "PGW_VEHICLES"
        value = tostring(var.simulated_vehicles)
      }

      dynamic "env" {
        for_each = var.vehicle_backend == "smartcar" ? toset(["PGW_SMARTCAR_CLIENT_ID"]) : toset([])
        content {
          name  = env.value
          value = var.smartcar_client_id
        }
      }
      dynamic "env" {
        for_each = google_secret_manager_secret.smartcar # empty unless vehicle_backend = "smartcar"
        content {
          name = env.key == "smartcar-client-secret" ? "PGW_SMARTCAR_CLIENT_SECRET" : "PGW_SMARTCAR_REFRESH_TOKEN"
          value_source {
            secret_key_ref {
              secret  = env.value.id
              version = "latest"
            }
          }
        }
      }
    }
  }

  depends_on = [google_project_service.apis, google_secret_manager_secret_iam_member.vehicle_reads_smartcar]
}

resource "google_cloud_run_v2_service_iam_member" "gateway_invokes_vehicle" {
  count    = var.deploy_services ? 1 : 0
  name     = google_cloud_run_v2_service.vehicle[0].name
  location = var.region
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.gateway.email}"
}

# Smartcar credentials, used only with vehicle_backend = "smartcar". Values are added out of band:
#   printf %s "$SECRET" | gcloud secrets versions add pgw-smartcar-client-secret-dev --data-file=-
resource "google_secret_manager_secret" "smartcar" {
  for_each  = var.vehicle_backend == "smartcar" ? toset(["smartcar-client-secret", "smartcar-refresh-token"]) : toset([])
  secret_id = "pgw-${each.key}-${var.env}"
  replication {
    auto {}
  }
}

resource "google_secret_manager_secret_iam_member" "vehicle_reads_smartcar" {
  for_each  = google_secret_manager_secret.smartcar
  secret_id = each.value.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.vehicle_service.email}"
}

# -- Cloud Run: partner gateway -----------------------------------------------------------------

resource "google_cloud_run_v2_service" "gateway" {
  count               = var.deploy_services ? 1 : 0
  name                = "partner-gateway-${var.env}"
  location            = var.region
  ingress             = "INGRESS_TRAFFIC_ALL"
  deletion_protection = false

  template {
    service_account                  = google_service_account.gateway.email
    max_instance_request_concurrency = var.gateway_concurrency

    scaling {
      min_instance_count = var.gateway_min_instances
      max_instance_count = var.gateway_max_instances
    }

    vpc_access {
      network_interfaces {
        network    = google_compute_network.main.id
        subnetwork = google_compute_subnetwork.run.id
      }
      egress = "PRIVATE_RANGES_ONLY" # Memorystore through the VPC; partners' webhooks and Google APIs directly
    }

    volumes {
      name = "cloudsql"
      cloud_sql_instance {
        instances = [google_sql_database_instance.main.connection_name]
      }
    }

    containers {
      image = local.image
      args  = ["gateway"]

      ports {
        container_port = 8080
      }

      resources {
        limits = {
          cpu    = "1"
          memory = "512Mi"
        }
        cpu_idle          = false # webhook delivery, the command sweep and the Pub/Sub consumer run between requests
        startup_cpu_boost = true
      }

      volume_mounts {
        name       = "cloudsql"
        mount_path = "/cloudsql"
      }

      env {
        name  = "PGW_ENV"
        value = var.env
      }
      env {
        name  = "PGW_BUS"
        value = "pubsub"
      }
      env {
        name  = "PGW_PUBSUB_PROJECT"
        value = var.project
      }
      env {
        name  = "PGW_PUBSUB_SUFFIX"
        value = "-${var.env}"
      }
      env {
        name  = "PGW_SIGNING_KEYS_SECRET" # read through the API and re-read every minute, so rotation needs no deploy
        value = google_secret_manager_secret.gateway["token-signing-key"].id
      }
      env {
        name  = "PGW_VEHICLE_SERVICE"
        value = "${trimprefix(google_cloud_run_v2_service.vehicle[0].uri, "https://")}:443"
      }
      env {
        name  = "PGW_VEHICLE_AUTH"
        value = "google-id-token"
      }
      env {
        name  = "PGW_DB_POOL_SIZE"
        value = "2" # x2 with overflow, x gateway_max_instances: stays under db_max_connections
      }
      env {
        name = "PGW_DATABASE_URL"
        value_source {
          secret_key_ref {
            secret  = google_secret_manager_secret.runtime["database-url"].id
            version = "latest"
          }
        }
      }
      env {
        name = "PGW_REDIS_URL"
        value_source {
          secret_key_ref {
            secret  = google_secret_manager_secret.runtime["redis-url"].id
            version = "latest"
          }
        }
      }

      startup_probe {
        http_get {
          path = "/healthz"
        }
        period_seconds    = 2
        failure_threshold = 15
      }
      liveness_probe {
        http_get {
          path = "/healthz"
        }
        period_seconds = 30
      }
    }
  }

  depends_on = [
    google_project_service.apis,
    google_secret_manager_secret_version.database_url,
    google_secret_manager_secret_version.redis_url,
    google_secret_manager_secret_iam_member.gateway_reads_runtime,
    google_secret_manager_secret_iam_member.gateway_reads_its_secrets,
    google_project_iam_member.gateway_cloudsql_client,
  ]
}

resource "google_cloud_run_v2_service_iam_member" "public_gateway" {
  count    = var.deploy_services && var.public_gateway ? 1 : 0
  name     = google_cloud_run_v2_service.gateway[0].name
  location = var.region
  role     = "roles/run.invoker"
  member   = "allUsers"
}

# -- admin job: migrations and partner registration ---------------------------------------------
#   gcloud run jobs execute pgw-admin-dev --region us-west1 --wait                      (pgw db upgrade)
#   gcloud run jobs execute pgw-admin-dev --region us-west1 --wait \
#       --args=partners,create,--name=Load-test,--demo-vins=50,--rate=2000,--burst=4000

resource "google_cloud_run_v2_job" "admin" {
  count               = var.deploy_services ? 1 : 0
  name                = "pgw-admin-${var.env}"
  location            = var.region
  deletion_protection = false

  template {
    template {
      service_account = google_service_account.gateway.email
      max_retries     = 0
      timeout         = "300s"

      volumes {
        name = "cloudsql"
        cloud_sql_instance {
          instances = [google_sql_database_instance.main.connection_name]
        }
      }

      containers {
        image = local.image
        args  = ["db", "upgrade"]

        volume_mounts {
          name       = "cloudsql"
          mount_path = "/cloudsql"
        }

        env {
          name = "PGW_DATABASE_URL"
          value_source {
            secret_key_ref {
              secret  = google_secret_manager_secret.runtime["database-url"].id
              version = "latest"
            }
          }
        }
      }
    }
  }

  depends_on = [
    google_secret_manager_secret_version.database_url,
    google_secret_manager_secret_iam_member.gateway_reads_runtime,
    google_project_iam_member.gateway_cloudsql_client,
  ]
}
