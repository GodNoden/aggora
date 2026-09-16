# Variables de la Fase 10 (despliegue a AWS).
#
# Regla de la fase: lo que se puede destruir y recrear lo describe Terraform; lo que
# factura por hora y guarda datos (el broker gestionado) se crea a mano en la consola.
# Por eso aqui no hay ninguna variable de broker: sus datos llegan por SSM Parameter Store.

variable "aws_region" {
  description = "Region de AWS donde vive todo el despliegue."
  type        = string
  default     = "eu-west-1"
}

variable "project" {
  description = "Prefijo de todos los recursos con nombre. Las etiquetas llevan Project = <esto>."
  type        = string
  default     = "aggora"
}

variable "instance_type" {
  description = <<-EOT
    Tipo de la VM. t4g.small (2 vCPU, 2 GB) es el minimo del plan, pero va justo:
    7 JVMs + Postgres + Prometheus + Grafana no caben comodos en 2 GB. Si ves swapping
    o reinicios por OOM, sube a t4g.medium (4 GB) sin tocar nada mas.
  EOT
  type        = string
  default     = "t4g.small"
}

variable "subnet_id" {
  description = "Subred donde se crea la VM. Vacio = la primera subred de la VPC por defecto."
  type        = string
  default     = ""
}

variable "artifacts_bucket_name" {
  description = <<-EOT
    Nombre del bucket con los jars y la configuracion. Tiene que ser unico en todo AWS.
    Vacio = aggora-artifacts-<account_id>. El nombre real se publica en SSM para que el
    user_data lo lea al arrancar (user_data = file() no admite interpolacion).
  EOT
  type        = string
  default     = ""
}

# --- Credenciales del broker gestionado -------------------------------------------
# Sin valor por defecto a proposito: se pasan por TF_VAR_ (ver deploy/README.md) para
# que no queden ni en el historial ni en el repositorio. Ojo: van al state de Terraform,
# que tambien es un secreto.

variable "kafka_bootstrap_servers" {
  description = "Bootstrap servers del broker gestionado, separados por comas."
  type        = string
  sensitive   = true
}

variable "kafka_schema_registry_url" {
  description = "URL del Schema Registry del proveedor (https://...)."
  type        = string
  sensitive   = true
}

variable "kafka_sasl_username" {
  description = "Usuario SASL (la API key en Confluent Cloud)."
  type        = string
  sensitive   = true
}

variable "kafka_sasl_password" {
  description = "Clave SASL (el API secret en Confluent Cloud)."
  type        = string
  sensitive   = true
}

variable "kafka_sasl_mechanism" {
  description = "Mecanismo SASL del proveedor: PLAIN (Confluent) o SCRAM-SHA-256 / SCRAM-SHA-512 (Redpanda)."
  type        = string
  default     = "PLAIN"

  validation {
    condition     = contains(["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"], var.kafka_sasl_mechanism)
    error_message = "kafka_sasl_mechanism tiene que ser PLAIN, SCRAM-SHA-256 o SCRAM-SHA-512."
  }
}

variable "postgres_password" {
  description = "Clave del Postgres que corre en la VM (lo usa el audit-log y el compose)."
  type        = string
  sensitive   = true
}

variable "kafka_sr_username" {
  description = <<-EOT
    Usuario del Schema Registry. En Confluent Cloud es una API key propia, distinta de la
    del cluster. Vacio = el Schema Registry no pide autenticacion (Redpanda Cloud, o un
    SR interno ya protegido por red) y no se crea el parametro SSM.
  EOT
  type        = string
  default     = ""
  sensitive   = true
}

variable "kafka_sr_password" {
  description = "Clave del Schema Registry. Vacio = sin autenticacion."
  type        = string
  default     = ""
  sensitive   = true
}

# --- Lambda ------------------------------------------------------------------------

variable "lambda_source_topic" {
  description = "Topic que consume la Lambda (el crudo del simulador)."
  type        = string
  default     = "market.ticks.raw"
}

variable "lambda_target_topic" {
  description = "Topic canonico en el que publica la Lambda."
  type        = string
  default     = "market.ticks.canonical"
}

variable "lambda_jar_path" {
  description = "Jar gordo de la Lambda, relativo al modulo de Terraform. Hay que compilarlo antes."
  type        = string
  default     = "../../services/lambda/ingestion-normalizer-lambda/target/ingestion-normalizer-lambda-0.1.0-SNAPSHOT-shaded.jar"
}

# --- Alarmas y logs ----------------------------------------------------------------

variable "alarm_email" {
  description = "Correo para las alarmas. Vacio = no se crea la suscripcion al topic SNS."
  type        = string
  default     = ""
}

variable "log_retention_days" {
  description = "Dias que se guardan los logs de la Lambda en CloudWatch."
  type        = number
  default     = 14
}

# --- Claves de los proveedores de datos (opcionales) -------------------------------
# Sin clave, market-data-simulator arranca igual con precios semilla y ticks sinteticos.

variable "twelvedata_api_key" {
  description = "API key de Twelve Data. Vacio = no se crea el parametro SSM y el simulador va en sintetico."
  type        = string
  default     = ""
  sensitive   = true
}

variable "alphavantage_api_key" {
  description = "API key de Alpha Vantage. Vacio = no se crea el parametro SSM y el simulador va en sintetico."
  type        = string
  default     = ""
  sensitive   = true
}
