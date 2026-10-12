variable "project" {
  description = "GCP project id"
  type        = string
}

variable "region" {
  description = "Region for regional resources"
  type        = string
  default     = "us-west1"
}

variable "env" {
  description = "Environment name, used as a suffix (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "max_delivery_attempts" {
  description = "Deliveries before a message moves to its dead-letter topic"
  type        = number
  default     = 5
}

variable "image_tag" {
  description = "Tag of the partner-gateway image in Artifact Registry (make deploy uses the git commit)"
  type        = string
  default     = "latest"
}

variable "deploy_services" {
  description = "Create the Cloud Run services and job. False for the first apply, before an image exists."
  type        = bool
  default     = true
}

variable "subnet_cidr" {
  description = "Subnet for Cloud Run's direct VPC egress (reaches Memorystore)"
  type        = string
  default     = "10.10.0.0/24"
}

variable "db_tier" {
  description = "Cloud SQL machine tier. db-f1-micro is the cheapest shared-core tier."
  type        = string
  default     = "db-f1-micro"
}

variable "redis_memory_gb" {
  description = "Memorystore size (Basic tier, no replica)"
  type        = number
  default     = 1
}

variable "deletion_protection" {
  description = "Protect the Cloud SQL instance from terraform destroy"
  type        = bool
  default     = false
}

variable "gateway_min_instances" {
  description = "Gateway instances kept warm. 0 scales to zero when idle (cheapest)."
  type        = number
  default     = 0
}

variable "gateway_max_instances" {
  description = "Upper bound on gateway instances (and on cost during a load test)"
  type        = number
  default     = 10
}

variable "gateway_concurrency" {
  description = "Requests one gateway instance handles at once"
  type        = number
  default     = 80
}

variable "public_gateway" {
  description = "Let anyone reach the partner API (it authenticates partners with OAuth itself)"
  type        = bool
  default     = true
}

variable "vehicle_backend" {
  description = "simulated (built-in fleet) or smartcar (Smartcar API / Vehicle Simulator)"
  type        = string
  default     = "simulated"
  validation {
    condition     = contains(["simulated", "smartcar"], var.vehicle_backend)
    error_message = "vehicle_backend must be simulated or smartcar."
  }
}

variable "simulated_vehicles" {
  description = "Number of simulated vehicles"
  type        = number
  default     = 50
}

variable "smartcar_client_id" {
  description = "Smartcar application client id (the secret and refresh token go in Secret Manager)"
  type        = string
  default     = ""
}
