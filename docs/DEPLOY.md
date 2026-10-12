# Deploying to Google Cloud

`make deploy PROJECT=<project-id>` creates everything in `deploy/terraform` and starts the
services:

| Piece | Resource |
|---|---|
| Partner gateway | Cloud Run service `partner-gateway-<env>`, 0 to 10 instances, 1 vCPU each, public (partners authenticate with OAuth) |
| Vehicle service | Cloud Run service `vehicle-service-<env>`, 1 instance at most (the simulated fleet is in memory), invokable only by the gateway's service account |
| Rate limits, idempotency keys | Memorystore for Redis, Basic tier, 1 GB, reached through direct VPC egress |
| Partners, commands, webhook outbox | Cloud SQL for PostgreSQL 16, `db-f1-micro`, reached through the Cloud SQL connector |
| Events | Pub/Sub topics with dead-letter topics (already in the Terraform before this change) |
| Signing keys, connection strings | Secret Manager |
| Migrations, partner registration | Cloud Run job `pgw-admin-<env>` |
| Image | Artifact Registry, built by Cloud Build (no local Docker needed) |

## One-time setup

1. A GCP project with billing enabled. A new account's free trial credit covers this.
2. Tools: the gcloud CLI, Terraform 1.6 or later, k6, and this package (`pip install -e ".[dev]"`).
3. Log in twice, once for gcloud and once for Terraform:

       gcloud auth login
       gcloud auth application-default login

4. If Cloud Build can't push the image (newer projects don't grant its service account
   Artifact Registry access), grant it once:

       PROJECT_NUMBER=$(gcloud projects describe $PROJECT --format='value(projectNumber)')
       gcloud projects add-iam-policy-binding $PROJECT \
         --member=serviceAccount:$PROJECT_NUMBER-compute@developer.gserviceaccount.com \
         --role=roles/artifactregistry.writer
       gcloud projects add-iam-policy-binding $PROJECT \
         --member=serviceAccount:$PROJECT_NUMBER-compute@developer.gserviceaccount.com \
         --role=roles/logging.logWriter

## Deploy and load test

    make deploy PROJECT=my-project          # 15 to 25 minutes the first time (Cloud SQL, Memorystore)
    make partner PROJECT=my-project         # prints client_id and client_secret once
    make loadtest-cloud PROJECT=my-project CLIENT_ID=pc_... CLIENT_SECRET=... RATE=1000 DURATION=5m
    make destroy PROJECT=my-project         # when done: Memorystore and Cloud SQL bill by the hour

`make deploy` runs, in order: `terraform apply` without the services (they need an image),
`make keys` (creates the signing key set in Secret Manager if it has no version yet),
`gcloud builds submit`, `terraform apply` with the services, and the migration job.

Run k6 from a machine with at least 4 CPUs and a good uplink, or from a VM in the same
region; a laptop on home Wi-Fi may not generate 1,000 requests/s. Commit
`loadtest/results/cloud-dev/` afterwards so the README can quote the run.

## Cost

Prices are Google's list prices for us-central1 (Iowa) in October 2026; us-west1 (the
default region here) can differ a little. Check the pricing pages before you rely on them.

| Resource | Price | Free tier |
|---|---|---|
| Memorystore Basic, 1 GB | $0.049 per GB-hour, about $36 a month | none |
| Cloud SQL `db-f1-micro` | about $8 a month, plus about $2 for 10 GB of SSD | none |
| Cloud Run (instance-based billing, used here because work continues between requests) | $0.000018 per vCPU-second plus memory | 240,000 vCPU-seconds and 450,000 GiB-seconds a month |
| Pub/Sub, Secret Manager, Artifact Registry, Cloud Build | cents at this scale | each has a monthly free allowance |

What that means in practice:

- **Left running for a month:** about $45 to $50, almost all of it Memorystore and Cloud SQL.
  Both services scale to zero (`gateway_min_instances = 0`), so idle Cloud Run costs nothing.
- **Deploy, test, destroy in one afternoon:** well under $1 for Memorystore and Cloud SQL.
  A 5-minute 1,000 requests/s test is 300,000 requests and a few thousand vCPU-seconds,
  inside Cloud Run's free tier.
- To keep the database and Redis between sessions without paying for them, there is no
  stop button for Memorystore; `make destroy` and `make deploy` again (about 20 minutes).

## Configuration

Terraform variables (`deploy/terraform/variables.tf`) cover region, instance counts,
database tier and the vehicle backend. The services read only `PGW_*` environment
variables (`pgw/settings.py`), which Terraform sets.

To use Smartcar's Vehicle Simulator instead of the built-in fleet:

1. In the Smartcar dashboard, create an application and a simulated vehicle, and connect it
   through Smartcar Connect in simulated mode to get a refresh token.
2. Deploy with `-var vehicle_backend=smartcar -var smartcar_client_id=...` (add the
   variables to `TF_VARS` in the Makefile or a `terraform.tfvars` file).
3. Add the two secrets:

       printf %s "$SMARTCAR_CLIENT_SECRET" | gcloud secrets versions add pgw-smartcar-client-secret-dev --data-file=-
       printf %s "$SMARTCAR_REFRESH_TOKEN" | gcloud secrets versions add pgw-smartcar-refresh-token-dev --data-file=-

4. Register a partner with the simulated car's VIN: `--args=partners,create,--name=Smartcar,--vin=<VIN>`.

## Limits of this deployment

- The vehicle service runs one instance, because the simulated fleet's state is in memory.
  That is the simulator's limit, not the gateway's.
- Memorystore Basic has no replica. A Redis restart resets rate limits and forgets
  idempotency keys (see `docs/DESIGN.md`).
- The database password is generated by Terraform and is in its state. Keep the state in a
  private bucket (add a `backend "gcs"` block) rather than on a laptop for anything shared.
- Between the two services, IAM replaces mTLS (Cloud Run can't pass client certificates
  through). The mTLS path is still what docker compose and the tests use.
