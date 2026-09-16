# Fase 10: la infraestructura que rodea al broker gestionado.
#
# Lo que NO esta aqui, a proposito: el broker y el Schema Registry. Se crean a mano en
# la consola del proveedor (Confluent Cloud o Redpanda Cloud) porque facturan por hora y
# guardan datos: Terraform es para recrear lo que se puede destruir sin perder nada.
# Ver el capitulo 19 de docs/kafka-101.md y deploy/README.md.
#
# Reparto: una VM siempre encendida con lo que tiene estado (los 3 de Streams) y el resto
# del pipeline, y AWS Lambda para el unico servicio sin estado (ingestion-normalizer).
# Nada de EKS: el plano de control se cobra por hora aunque no haya nodos.

terraform {
  required_version = ">= 1.6"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = local.tags
  }
}

locals {
  tags = {
    Project   = var.project
    ManagedBy = "terraform"
    Phase     = "10"
  }

  ssm_prefix = "/${var.project}"

  account_id = data.aws_caller_identity.current.account_id

  # Bucket: si no lo fija la variable, uno derivado de la cuenta (que ya es unico).
  artifacts_bucket = var.artifacts_bucket_name != "" ? var.artifacts_bucket_name : "${var.project}-artifacts-${local.account_id}"

  # Red: siempre la VPC por defecto. Si no se elige subred, la primera que devuelva AWS.
  subnet_id = var.subnet_id != "" ? var.subnet_id : data.aws_subnets.default.ids[0]

  # Jars compilados en el host por deploy/collect-artifacts.sh. Si el directorio esta
  # vacio, la condicion de terraform_data.artifacts_present corta el plan con un mensaje
  # claro en vez de desplegar una VM sin nada que arrancar.
  jars = fileset("${path.module}/../artifacts", "*.jar")

  # Configuracion que la VM necesita en disco y que no puede salir del repo (alli no hay
  # checkout): el compose de la VM, la unidad de systemd y lo que monta el compose desde
  # infra/. Se sube al bucket y el user_data lo baja con `aws s3 sync`.
  infra_files = toset(flatten([
    for pattern in ["infra/prometheus/**", "infra/grafana/**"] :
    fileset("${path.module}/../..", pattern)
  ]))

  config_objects = merge(
    { for f in local.infra_files : "config/${f}" => "${path.module}/../../${f}" },
    {
      "config/deploy/docker-compose.vm.yml" = "${path.module}/../docker-compose.vm.yml"
      "config/systemd/aggora@.service"      = "${path.module}/../systemd/aggora@.service"
    },
  )

  lambda_jar = "${path.module}/${var.lambda_jar_path}"

  # El event source mapping de Kafka no usa `function_response_types` (el proveedor lo
  # documenta solo para SQS y streams de DynamoDB/Kinesis) y tampoco entra en la VPC:
  # el broker es gestionado y publico, y meter la Lambda en la VPC obligaria a montar
  # NAT o endpoints para salir a internet.
  esm_sasl_type = (
    var.kafka_sasl_mechanism == "PLAIN" ? "BASIC_AUTH" :
    var.kafka_sasl_mechanism == "SCRAM-SHA-512" ? "SASL_SCRAM_512_AUTH" : "SASL_SCRAM_256_AUTH"
  )
}

# --- Data sources -------------------------------------------------------------------

data "aws_caller_identity" "current" {}

data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

# AMI de Amazon Linux 2023 para ARM64 (Graviton). Es el parametro publico que mantiene
# AWS: no hay que averiguar el ID a mano ni caduca como una AMI fijada.
data "aws_ssm_parameter" "al2023_arm64" {
  name = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-arm64"
}

# --- Bucket de artefactos -----------------------------------------------------------

resource "aws_s3_bucket" "artifacts" {
  bucket = local.artifacts_bucket
}

resource "aws_s3_bucket_public_access_block" "artifacts" {
  bucket                  = aws_s3_bucket.artifacts.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Un objeto por jar. `fileset` es lo que hace que anadir un servicio sea compilar y
# volver a aplicar, sin tocar Terraform.
resource "aws_s3_object" "jars" {
  for_each = local.jars

  bucket      = aws_s3_bucket.artifacts.id
  key         = "jars/${each.value}"
  source      = "${path.module}/../artifacts/${each.value}"
  source_hash = filemd5("${path.module}/../artifacts/${each.value}")
}

# El codigo de la Lambda no lo sube Terraform (va en el zip/jar de despliegue), pero si
# la configuracion que la VM baja al arrancar.
resource "aws_s3_object" "config" {
  for_each = local.config_objects

  bucket      = aws_s3_bucket.artifacts.id
  key         = each.key
  source      = each.value
  source_hash = filemd5(each.value)
}

# Corta el plan si deploy/artifacts/ esta vacio. Sin esto, Terraform crearia la VM, el
# bucket y la Lambda, y el user_data no encontraria ni un jar: fallo tardio y confuso.
resource "terraform_data" "artifacts_present" {
  input = length(local.jars)

  lifecycle {
    precondition {
      condition     = length(local.jars) > 0
      error_message = "No hay jars en deploy/artifacts/. Compilalos antes: bash deploy/collect-artifacts.sh"
    }
  }
}

# --- Parametros SSM (los secretos entran por aqui, no por el repo) -------------------
# SecureString para todos: el user_data los lee igual con --with-decryption y asi no hay
# que decidir caso por caso que es "secreto". Los que no lo son (bootstrap, topic) son
# SecureString por uniformidad, no porque el valor lo sea.

resource "aws_ssm_parameter" "kafka_bootstrap_servers" {
  name  = "${local.ssm_prefix}/kafka/bootstrap-servers"
  type  = "SecureString"
  value = var.kafka_bootstrap_servers
}

resource "aws_ssm_parameter" "kafka_schema_registry_url" {
  name  = "${local.ssm_prefix}/kafka/schema-registry-url"
  type  = "SecureString"
  value = var.kafka_schema_registry_url
}

resource "aws_ssm_parameter" "kafka_sasl_username" {
  name  = "${local.ssm_prefix}/kafka/sasl-username"
  type  = "SecureString"
  value = var.kafka_sasl_username
}

resource "aws_ssm_parameter" "kafka_sasl_password" {
  name  = "${local.ssm_prefix}/kafka/sasl-password"
  type  = "SecureString"
  value = var.kafka_sasl_password
}

# El mecanismo no es secreto, pero va aqui para que la VM lo lea del mismo sitio que el
# resto y no haya dos fuentes de verdad sobre la configuracion del broker.
resource "aws_ssm_parameter" "kafka_sasl_mechanism" {
  name  = "${local.ssm_prefix}/kafka/sasl-mechanism"
  type  = "String"
  value = var.kafka_sasl_mechanism
}

# El nombre del topic de entrada queda escrito una vez. Lo lee la Lambda (variable de
# entorno) y lo documenta el runbook para crear los topics a mano.
resource "aws_ssm_parameter" "lambda_source_topic" {
  name  = "${local.ssm_prefix}/topics/lambda-source"
  type  = "SecureString"
  value = var.lambda_source_topic
}

resource "aws_ssm_parameter" "postgres_password" {
  name  = "${local.ssm_prefix}/postgres/password"
  type  = "SecureString"
  value = var.postgres_password
}

# Opcionales: Confluent Cloud da al Schema Registry su propia API key. Si no se pasan, el
# user_data no escribe la autenticacion basica y el SR se usa tal cual.
resource "aws_ssm_parameter" "kafka_sr_username" {
  count = var.kafka_sr_username != "" ? 1 : 0

  name  = "${local.ssm_prefix}/kafka/schema-registry-username"
  type  = "SecureString"
  value = var.kafka_sr_username
}

resource "aws_ssm_parameter" "kafka_sr_password" {
  count = var.kafka_sr_password != "" ? 1 : 0

  name  = "${local.ssm_prefix}/kafka/schema-registry-password"
  type  = "SecureString"
  value = var.kafka_sr_password
}

# El user_data no puede interpolar el nombre del bucket (es `file()`, no `templatefile()`),
# asi que Terraform lo publica aqui y la VM lo lee con `aws ssm get-parameter`.
resource "aws_ssm_parameter" "artifacts_bucket" {
  name  = "${local.ssm_prefix}/deploy/artifacts-bucket"
  type  = "String"
  value = aws_s3_bucket.artifacts.bucket
}

# Opcionales: solo existen si se pasa la clave. Sin ellas el simulador va en sintetico.
resource "aws_ssm_parameter" "twelvedata_api_key" {
  count = var.twelvedata_api_key != "" ? 1 : 0

  name  = "${local.ssm_prefix}/providers/twelvedata-api-key"
  type  = "SecureString"
  value = var.twelvedata_api_key
}

resource "aws_ssm_parameter" "alphavantage_api_key" {
  count = var.alphavantage_api_key != "" ? 1 : 0

  name  = "${local.ssm_prefix}/providers/alphavantage-api-key"
  type  = "SecureString"
  value = var.alphavantage_api_key
}

# --- Rol e instance profile de la VM ------------------------------------------------
# AmazonSSMManagedInstanceCore es lo que permite entrar por Session Manager sin abrir el
# puerto 22 (y sin key pair que perder). Lo demas es lo minimo para arrancar: leer los
# parametros, leer el bucket y escribir logs.

data "aws_iam_policy_document" "ec2_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "instance" {
  name               = "${var.project}-vm"
  assume_role_policy = data.aws_iam_policy_document.ec2_assume_role.json
}

resource "aws_iam_role_policy_attachment" "instance_ssm_core" {
  role       = aws_iam_role.instance.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

data "aws_iam_policy_document" "instance" {
  statement {
    sid       = "ReadArtifacts"
    effect    = "Allow"
    actions   = ["s3:GetObject", "s3:ListBucket"]
    resources = [aws_s3_bucket.artifacts.arn, "${aws_s3_bucket.artifacts.arn}/*"]
  }

  statement {
    sid       = "ReadDeployParameters"
    effect    = "Allow"
    actions   = ["ssm:GetParameter", "ssm:GetParameters", "ssm:GetParametersByPath"]
    resources = ["arn:aws:ssm:${var.aws_region}:${local.account_id}:parameter${local.ssm_prefix}/*"]
  }

  # Los SecureString se cifran con la clave gestionada aws/ssm; para descifrarlos hace
  # falta kms:Decrypt sobre esa clave. En vez de abrir KMS entero, se limita a las
  # llamadas que vienen de SSM.
  statement {
    sid       = "DecryptParameters"
    effect    = "Allow"
    actions   = ["kms:Decrypt"]
    resources = ["*"]

    condition {
      test     = "StringEquals"
      variable = "kms:ViaService"
      values   = ["ssm.${var.aws_region}.amazonaws.com"]
    }
  }

  statement {
    sid       = "WriteLogs"
    effect    = "Allow"
    actions   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents", "logs:DescribeLogStreams"]
    resources = ["arn:aws:logs:${var.aws_region}:${local.account_id}:log-group:/${var.project}/*"]
  }
}

resource "aws_iam_role_policy" "instance" {
  name   = "${var.project}-vm"
  role   = aws_iam_role.instance.id
  policy = data.aws_iam_policy_document.instance.json
}

resource "aws_iam_instance_profile" "instance" {
  name = "${var.project}-vm"
  role = aws_iam_role.instance.name
}

# --- Security group y VM ------------------------------------------------------------

# Sin reglas de entrada: a la VM se entra por SSM Session Manager y todo lo que hay
# dentro (actuator, Grafana, el gateway) se mira por un tunel de Session Manager. Un
# puerto abierto en internet es la forma mas rapida de perder un broker con credenciales.
resource "aws_security_group" "vm" {
  name        = "${var.project}-vm"
  description = "Salida a internet (broker gestionado, S3, SSM) y nada de entrada"
  vpc_id      = data.aws_vpc.default.id

  egress {
    description = "Todo el trafico de salida"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_instance" "vm" {
  ami                         = data.aws_ssm_parameter.al2023_arm64.value
  instance_type               = var.instance_type
  subnet_id                   = local.subnet_id
  iam_instance_profile        = aws_iam_instance_profile.instance.name
  vpc_security_group_ids      = [aws_security_group.vm.id]
  associate_public_ip_address = true

  # IMDSv2 obligatorio: sin token no se lee ni la region. El user_data lo usa.
  metadata_options {
    http_endpoint = "enabled"
    http_tokens   = "required"
  }

  root_block_device {
    volume_size = 20
    volume_type = "gp3"
    encrypted   = true
  }

  user_data                   = file("${path.module}/../user-data.sh")
  user_data_replace_on_change = true

  tags = merge(local.tags, {
    Name = "${var.project}-vm"
  })

  # Los jars, la configuracion y los parametros SSM tienen que existir ANTES de que
  # arranque la VM: el user_data los lee en el primer minuto de vida. Terraform los crea
  # en paralelo por defecto, asi que sin este depends_on hay carrera.
  depends_on = [
    aws_s3_object.jars,
    aws_s3_object.config,
    aws_iam_role_policy_attachment.instance_ssm_core,
    aws_iam_role_policy.instance,
    aws_ssm_parameter.kafka_bootstrap_servers,
    aws_ssm_parameter.kafka_schema_registry_url,
    aws_ssm_parameter.kafka_sasl_username,
    aws_ssm_parameter.kafka_sasl_password,
    aws_ssm_parameter.kafka_sasl_mechanism,
    aws_ssm_parameter.kafka_sr_username,
    aws_ssm_parameter.kafka_sr_password,
    aws_ssm_parameter.postgres_password,
    aws_ssm_parameter.lambda_source_topic,
    aws_ssm_parameter.artifacts_bucket,
    aws_ssm_parameter.twelvedata_api_key,
    aws_ssm_parameter.alphavantage_api_key,
  ]
}

# --- Lambda: ingestion-normalizer ---------------------------------------------------
# El unico servicio sin estado. No es "la misma app en otro sitio": Lambda hace el bucle
# de consumo, invoca con un lote ya leido (el value en base64) y confirma los offsets el.
# Por eso el handler manda el veneno al .DLT sin lanzar, y solo la caida de
# infraestructura acaba en la cola SQS de fallos.

data "aws_iam_policy_document" "lambda_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "lambda" {
  name               = "${var.project}-ingestion-normalizer"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
}

resource "aws_iam_role_policy_attachment" "lambda_basic" {
  role       = aws_iam_role.lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

data "aws_iam_policy_document" "lambda" {
  statement {
    sid       = "ReadKafkaSaslSecret"
    effect    = "Allow"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [aws_secretsmanager_secret.kafka_sasl.arn]
  }

  # El event source mapping describe la red del cliente para montar la conexion. Aunque
  # la Lambda no viva en la VPC, AWS pide estos permisos de lectura al configurar el ESM.
  statement {
    sid       = "DescribeNetworkForEventSourceMapping"
    effect    = "Allow"
    actions   = ["ec2:DescribeVpcs", "ec2:DescribeSubnets", "ec2:DescribeSecurityGroups", "ec2:DescribeNetworkInterfaces"]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "lambda" {
  name   = "${var.project}-ingestion-normalizer"
  role   = aws_iam_role.lambda.id
  policy = data.aws_iam_policy_document.lambda.json
}

# El event source mapping no lee las credenciales del entorno de la funcion: AWS las lee
# de Secrets Manager. El formato es {"username": ..., "password": ...} y vale tanto para
# SASL/PLAIN (BASIC_AUTH) como para SCRAM.
resource "aws_secretsmanager_secret" "kafka_sasl" {
  name = "${var.project}/lambda-kafka-sasl"
}

resource "aws_secretsmanager_secret_version" "kafka_sasl" {
  secret_id = aws_secretsmanager_secret.kafka_sasl.id
  secret_string = jsonencode({
    username = var.kafka_sasl_username
    password = var.kafka_sasl_password
  })
}

resource "aws_cloudwatch_log_group" "lambda" {
  name              = "/aws/lambda/${var.project}-ingestion-normalizer"
  retention_in_days = var.log_retention_days
}

# Donde acaba el lote que falla por infraestructura (el broker no responde, no el mensaje
# venenoso: ese se va al .DLT desde el handler). Es la on-failure destination del ESM.
resource "aws_sqs_queue" "lambda_failures" {
  name                       = "${var.project}-lambda-failures"
  message_retention_seconds  = 1209600 # 14 dias: tiempo de sobra para mirarlo
  sqs_managed_sse_enabled    = true
  visibility_timeout_seconds = 300
}

resource "aws_lambda_function" "normalizer" {
  function_name = "${var.project}-ingestion-normalizer"
  role          = aws_iam_role.lambda.arn
  runtime       = "java21"
  handler       = "com.aggora.normalizer.lambda.AggoraNormalizerHandler::handleRequest"

  # El jar gordo del modulo de la Lambda. Hay que compilarlo antes (ver el runbook).
  filename         = local.lambda_jar
  source_code_hash = filebase64sha256(local.lambda_jar)

  memory_size = 1024
  timeout     = 60

  # Variables de entorno de la funcion. El handler lee los nombres de topic y las
  # credenciales de aqui; el ESM no las necesita para nada (las suyas van por Secrets
  # Manager, arriba).
  environment {
    variables = {
      KAFKA_BOOTSTRAP_SERVERS  = var.kafka_bootstrap_servers
      KAFKA_SECURITY_PROTOCOL  = "SASL_SSL"
      KAFKA_SASL_MECHANISM     = var.kafka_sasl_mechanism
      KAFKA_SASL_USERNAME      = var.kafka_sasl_username
      KAFKA_SASL_PASSWORD      = var.kafka_sasl_password
      SCHEMA_REGISTRY_URL      = var.kafka_schema_registry_url
      KAFKA_SOURCE_TOPIC       = var.lambda_source_topic
      KAFKA_TARGET_TOPIC       = var.lambda_target_topic
      KAFKA_FX_REFERENCE_TOPIC = "market.fx.reference"
      KAFKA_DEAD_LETTER_TOPIC  = "${var.lambda_source_topic}.DLT"
      KAFKA_CONSUMER_GROUP     = "ingestion-normalizer-lambda"
    }
  }

  depends_on = [
    aws_iam_role_policy.lambda,
    aws_cloudwatch_log_group.lambda,
  ]
}

resource "aws_lambda_event_source_mapping" "normalizer" {
  function_name     = aws_lambda_function.normalizer.arn
  topics            = [var.lambda_source_topic]
  starting_position = "LATEST"

  # Lotes pequenos: la funcion normaliza y publica, y un lote grande solo alarga el
  # reintento cuando el broker no responde.
  batch_size = 10

  self_managed_event_source {
    endpoints = {
      KAFKA_BOOTSTRAP_SERVERS = var.kafka_bootstrap_servers
    }
  }

  self_managed_kafka_event_source_config {
    consumer_group_id = "ingestion-normalizer-lambda"
  }

  # Solo autenticacion: el broker es gestionado y publico, asi que no se declaran
  # VPC_SUBNET ni VPC_SECURITY_GROUP y la Lambda se queda fuera de la VPC. Meterla
  # dentro (solo tendria sentido con un broker privado) obliga a NAT o endpoints para
  # que la funcion siga saliendo a internet.
  source_access_configuration {
    type = local.esm_sasl_type
    uri  = aws_secretsmanager_secret.kafka_sasl.arn
  }

  # El lote que falla de verdad (infraestructura, no veneno) acaba aqui.
  destination_config {
    on_failure {
      destination_arn = aws_sqs_queue.lambda_failures.arn
    }
  }
}

# --- Alarmas ------------------------------------------------------------------------

resource "aws_sns_topic" "alarms" {
  name = "${var.project}-alarms"
}

resource "aws_sns_topic_subscription" "email" {
  count = var.alarm_email != "" ? 1 : 0

  topic_arn = aws_sns_topic.alarms.arn
  protocol  = "email"
  endpoint  = var.alarm_email
}

resource "aws_cloudwatch_metric_alarm" "lambda_errors" {
  alarm_name          = "${var.project}-lambda-errors"
  alarm_description   = "La Lambda de ingestion-normalizer esta fallando invocaciones."
  namespace           = "AWS/Lambda"
  metric_name         = "Errors"
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"

  dimensions = {
    FunctionName = aws_lambda_function.normalizer.function_name
  }

  alarm_actions = [aws_sns_topic.alarms.arn]
  ok_actions    = [aws_sns_topic.alarms.arn]
}

resource "aws_cloudwatch_metric_alarm" "instance_status" {
  alarm_name          = "${var.project}-vm-status"
  alarm_description   = "La comprobacion de estado de la VM ha fallado (el sistema o la instancia no responden)."
  namespace           = "AWS/EC2"
  metric_name         = "StatusCheckFailed"
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 2
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "breaching"

  dimensions = {
    InstanceId = aws_instance.vm.id
  }

  alarm_actions = [aws_sns_topic.alarms.arn]
  ok_actions    = [aws_sns_topic.alarms.arn]
}

# Una cola de fallos sin alarma es una cola que nadie mira.
resource "aws_cloudwatch_metric_alarm" "lambda_failure_backlog" {
  alarm_name          = "${var.project}-lambda-failure-backlog"
  alarm_description   = "Hay lotes en la cola de fallos de la Lambda: el broker no responde o el handler propaga excepciones."
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateNumberOfMessagesVisible"
  statistic           = "Maximum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"

  dimensions = {
    QueueName = aws_sqs_queue.lambda_failures.name
  }

  alarm_actions = [aws_sns_topic.alarms.arn]
}

# --- Sobre la sonda de salud de los servicios de Streams -----------------------------
# No hay alarma que sustituya a la sonda de salud, y conviene saber por que: un motor de
# Kafka Streams que se queda en ERROR NO se cae. El proceso sigue vivo, consume CPU
# normal y las metricas de EC2 (StatusCheckFailed) dicen que todo va bien, pero no
# procesa nada. Por eso systemd mira /actuator/health (unidad aggora@.service) y reinicia
# lo que esta en DOWN: es la unica senal que distingue "vivo" de "trabajando".
# La alarma de instancia de arriba cubre otra cosa (que la VM entera desaparezca), no
# esta. Y solo los servicios con servidor web exponen /actuator/health: en este
# despliegue, analytics-streams, market-data-simulator y gateway-ws. Para portfolio-risk
# y alerting-service, que no lo tienen, la senal de que se han quedado atras es el lag
# del grupo en kafka-exporter (Grafana).
