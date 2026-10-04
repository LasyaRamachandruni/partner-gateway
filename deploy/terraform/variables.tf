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
