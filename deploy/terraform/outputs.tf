# Salidas: lo que hace falta para entrar, mirar y depurar sin abrir la consola.

output "instance_id" {
  description = "ID de la VM (lo pide aws ssm start-session)."
  value       = aws_instance.vm.id
}

output "instance_public_ip" {
  description = "IP publica de la VM. No hay puertos abiertos: se usa desde dentro del tunel de SSM."
  value       = aws_instance.vm.public_ip
}

output "ssm_start_session" {
  description = "Comando exacto para entrar en la VM por Session Manager (sin abrir el 22)."
  value       = "aws ssm start-session --target ${aws_instance.vm.id} --region ${var.aws_region}"
}

output "ssm_port_forward_gateway" {
  description = "Tunel para ver la pagina del gateway en http://localhost:8089 sin abrir puertos."
  value       = "aws ssm start-session --target ${aws_instance.vm.id} --region ${var.aws_region} --document-name AWS-StartPortForwardingSession --parameters '{\"portNumber\":[\"8089\"],\"localPortNumber\":[\"8089\"]}'"
}

output "lambda_function_name" {
  description = "Nombre de la funcion Lambda del ingestion-normalizer."
  value       = aws_lambda_function.normalizer.function_name
}

output "sqs_queue_url" {
  description = "URL de la cola SQS donde caen los lotes que fallan."
  value       = aws_sqs_queue.lambda_failures.url
}

output "artifacts_bucket" {
  description = "Bucket con los jars y la configuracion que baja la VM."
  value       = aws_s3_bucket.artifacts.bucket
}

output "ssm_parameter_prefix" {
  description = "Prefijo de los parametros que lee el user_data al arrancar."
  value       = local.ssm_prefix
}
