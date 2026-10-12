# Local checks, the local stack, and deployment to Google Cloud.
#
#   make check                                 lint, type check, tests (Redis/Postgres tests need TEST_* URLs)
#   make deploy PROJECT=my-gcp-project         everything below, in order
#   make partner PROJECT=...                   register a load-test partner; prints its credentials
#   make loadtest-cloud PROJECT=... CLIENT_ID=... CLIENT_SECRET=... RATE=1000 DURATION=5m
#   make destroy PROJECT=...
#
# Needs: gcloud (logged in: gcloud auth login && gcloud auth application-default login),
# terraform >= 1.6, and this package installed (pip install -e ".[dev]") for `pgw keys`.

PROJECT ?=
REGION  ?= us-west1
ENV     ?= dev
TAG     ?= $(shell git rev-parse --short HEAD)$(shell git diff --quiet || echo -dirty)
RATE     ?= 1000
DURATION ?= 5m

TF      = terraform -chdir=deploy/terraform
TF_VARS = -var project=$(PROJECT) -var region=$(REGION) -var env=$(ENV)
REPO    = $(REGION)-docker.pkg.dev/$(PROJECT)/partner-gateway-$(ENV)
IMAGE   = $(REPO)/partner-gateway:$(TAG)
KEYS_SECRET = pgw-token-signing-key-$(ENV)
JOB     = pgw-admin-$(ENV)

.PHONY: check lint test compose deploy require-project tf-init infra-base keys image services migrate partner \
        gateway-url loadtest-cloud keys-stage keys-activate keys-prune destroy

check: lint test

lint:
	ruff check .
	ruff format --check .
	mypy

test:
	pytest --cov --cov-report=term

compose:
	docker compose up --build

# -- Google Cloud ---------------------------------------------------------------------------

deploy: require-project tf-init infra-base keys image services migrate
	@echo "deployed: $$($(TF) output -raw gateway_url)"
	@echo "next: make partner PROJECT=$(PROJECT)"

require-project:
	@test -n "$(PROJECT)" || (echo "set PROJECT=<your GCP project id>" && exit 1)
	gcloud config set project $(PROJECT)

tf-init:
	$(TF) init -input=false

# APIs, network, Memorystore, Cloud SQL, Pub/Sub, secrets, registry: everything except the services,
# which need an image first. Cloud SQL and Memorystore take 5-15 minutes the first time.
infra-base:
	$(TF) apply -input=false -auto-approve $(TF_VARS) -var deploy_services=false

# The token signing key set, created once and kept only in Secret Manager.
keys:
	@if gcloud secrets versions list $(KEYS_SECRET) --limit=1 --format='value(name)' | grep -q .; then \
		echo "signing keys already in $(KEYS_SECRET)"; \
	else \
		tmp=$$(mktemp -d) && pgw keys init $$tmp/keyset.json && \
		gcloud secrets versions add $(KEYS_SECRET) --data-file=$$tmp/keyset.json && rm -rf $$tmp; \
	fi

# Built by Cloud Build, so no local Docker is needed.
image:
	gcloud builds submit --tag $(IMAGE) .

services:
	$(TF) apply -input=false -auto-approve $(TF_VARS) -var deploy_services=true -var image_tag=$(TAG)

migrate:
	gcloud run jobs execute $(JOB) --region $(REGION) --wait

partner:
	gcloud run jobs execute $(JOB) --region $(REGION) --wait \
		--args=partners,create,--name=Load-test,--demo-vins=50,--rate=2000,--burst=4000
	@echo "credentials (shown once):"
	@gcloud logging read 'resource.type="cloud_run_job" AND resource.labels.job_name="$(JOB)" AND textPayload:"client_secret"' \
		--limit=1 --freshness=15m --format='value(textPayload)'

gateway-url:
	@$(TF) output -raw gateway_url

loadtest-cloud:
	@test -n "$(CLIENT_ID)" -a -n "$(CLIENT_SECRET)" || (echo "set CLIENT_ID and CLIENT_SECRET (make partner)" && exit 1)
	mkdir -p loadtest/results/cloud-$(ENV)
	k6 run --no-usage-report -e BASE_URLS=$$($(TF) output -raw gateway_url) -e CLIENT_ID=$(CLIENT_ID) \
		-e CLIENT_SECRET=$(CLIENT_SECRET) -e RATE=$(RATE) -e DURATION=$(DURATION) \
		-e SUMMARY_PATH=loadtest/results/cloud-$(ENV)/k6-summary.json loadtest/gateway.js \
		| tee loadtest/results/cloud-$(ENV)/k6.txt

# Signing key rotation: stage, wait at least 6 minutes (key refresh 60 s + JWKS cache 300 s), activate,
# wait out the token lifetime + leeway (16 minutes), prune.
keys-stage keys-activate keys-prune:
	tmp=$$(mktemp -d) && \
	gcloud secrets versions access latest --secret=$(KEYS_SECRET) > $$tmp/keyset.json && \
	pgw keys $(subst keys-,,$@) $$tmp/keyset.json && \
	gcloud secrets versions add $(KEYS_SECRET) --data-file=$$tmp/keyset.json; \
	rm -rf $$tmp

destroy: require-project
	$(TF) destroy -input=false $(TF_VARS)
