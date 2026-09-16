#!/bin/bash
#
# cloud-init / user_data de la VM de Aggora (Amazon Linux 2023, ARM64).
#
# Deja la maquina lista para arrancar el pipeline sin que nadie entre por SSH:
#   1. Java 21 y Docker.
#   2. El usuario aggora y los directorios /opt/aggora y /etc/aggora.
#   3. Los jars y la configuracion, bajados del bucket S3.
#   4. Los ficheros de entorno, con las credenciales leidas de SSM Parameter Store.
#   5. La unidad de systemd aggora@.service, y los servicios en orden.
#
# Es idempotente: volver a ejecutarlo no rompe nada (todo es install/mkdir/sync, y
# systemctl start sobre algo que ya corre no hace nada). Los comentarios van sin acentos
# a proposito, como el resto del HCL y del shell del proyecto.

set -euo pipefail

log() { echo "[aggora-user-data] $*"; }

PROJECT="${AGGORA_PROJECT:-aggora}"
SSM_PREFIX="/${PROJECT}"

# --- Region por IMDSv2 (la instancia tiene IMDSv2 obligatorio) ----------------------
# El CLI no adivina la region, y meterla a mano seria una interpolacion que user_data no
# puede hacer (Terraform lo pasa con file(), no con templatefile()).
IMDS_TOKEN="$(curl -sS -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 300")"
AWS_REGION="$(curl -sS -H "X-aws-ec2-metadata-token: ${IMDS_TOKEN}" http://169.254.169.254/latest/meta-data/placement/region)"
export AWS_REGION
log "region: ${AWS_REGION}"

param() {
  aws ssm get-parameter --name "$1" --with-decryption \
    --query Parameter.Value --output text --region "${AWS_REGION}"
}

# Para los parametros que pueden no existir (las claves de los proveedores de datos).
param_optional() {
  aws ssm get-parameter --name "$1" --with-decryption \
    --query Parameter.Value --output text --region "${AWS_REGION}" 2>/dev/null || true
}

# --- 1. Paquetes --------------------------------------------------------------------
log "instalando Java 21 y Docker"
dnf install -y java-21-amazon-corretto-headless docker
systemctl enable --now docker

# El AMI de AL2023 trae el AWS CLI v2; si algun dia no lo trae, esto lo dice claro.
if ! command -v aws >/dev/null 2>&1; then
  dnf install -y awscli-2 || dnf install -y aws-cli
fi

# El plugin de compose no viene en el paquete docker de AL2023.
if ! docker compose version >/dev/null 2>&1; then
  log "instalando el plugin docker compose"
  COMPOSE_VERSION="${COMPOSE_VERSION:-v2.29.1}"
  mkdir -p /usr/libexec/docker/cli-plugins
  curl -fsSL "https://github.com/docker/compose/releases/download/${COMPOSE_VERSION}/docker-compose-linux-aarch64" \
    -o /usr/libexec/docker/cli-plugins/docker-compose
  chmod +x /usr/libexec/docker/cli-plugins/docker-compose
fi

# --- 2. Usuario y directorios -------------------------------------------------------
if ! id -u aggora >/dev/null 2>&1; then
  useradd --system --create-home --home-dir /opt/aggora --shell /sbin/nologin aggora
fi
mkdir -p /opt/aggora/jars /etc/aggora /var/lib/aggora/streams
chown -R aggora:aggora /var/lib/aggora

# --- 3. Jars y configuracion desde S3 ----------------------------------------------
# El nombre del bucket lo publica Terraform en SSM: user_data no admite interpolacion.
ARTIFACTS_BUCKET="${AGGORA_ARTIFACTS_BUCKET:-$(param "${SSM_PREFIX}/deploy/artifacts-bucket")}"
log "bucket de artefactos: ${ARTIFACTS_BUCKET}"

aws s3 cp "s3://${ARTIFACTS_BUCKET}/jars/" /opt/aggora/jars/ --recursive --region "${AWS_REGION}"
# sync (no cp) para la configuracion: en el segundo arranque solo copia lo que cambio.
aws s3 sync "s3://${ARTIFACTS_BUCKET}/config/" /opt/aggora/ --region "${AWS_REGION}"
chown -R aggora:aggora /opt/aggora/jars

install -m 0644 /opt/aggora/systemd/aggora@.service /etc/systemd/system/aggora@.service
systemctl daemon-reload

# --- 4. Ficheros de entorno ---------------------------------------------------------
log "leyendo credenciales de SSM"
KAFKA_BOOTSTRAP="$(param "${SSM_PREFIX}/kafka/bootstrap-servers")"
KAFKA_SR_URL="$(param "${SSM_PREFIX}/kafka/schema-registry-url")"
KAFKA_USER="$(param "${SSM_PREFIX}/kafka/sasl-username")"
KAFKA_PASSWORD="$(param "${SSM_PREFIX}/kafka/sasl-password")"
KAFKA_MECHANISM="$(param "${SSM_PREFIX}/kafka/sasl-mechanism")"
POSTGRES_PASSWORD="$(param "${SSM_PREFIX}/postgres/password")"

# El listado directo del topic no lo usa la VM (lo consume la Lambda), pero se lee para
# dejar constancia en el log de que apunta al mismo sitio que el resto del pipeline.
KAFKA_SOURCE_TOPIC="$(param "${SSM_PREFIX}/topics/lambda-source")"
log "topic de entrada: ${KAFKA_SOURCE_TOPIC}"

case "${KAFKA_MECHANISM}" in
  PLAIN) LOGIN_MODULE="org.apache.kafka.common.security.plain.PlainLoginModule" ;;
  SCRAM-SHA-256 | SCRAM-SHA-512) LOGIN_MODULE="org.apache.kafka.common.security.scram.ScramLoginModule" ;;
  *)
    log "mecanismo SASL desconocido: ${KAFKA_MECHANISM}"
    exit 1
    ;;
esac

# common.env: lo comun a todos los servicios (broker, Schema Registry y SASL). El valor
# de sasl.jaas.config va entre comillas simples porque lleva espacios y comillas dobles.
cat > /etc/aggora/common.env << EOF
# Generado por deploy/user-data.sh. No editar a mano: el proximo arranque lo reescribe.
SPRING_KAFKA_BOOTSTRAP_SERVERS=${KAFKA_BOOTSTRAP}
SPRING_KAFKA_PROPERTIES_SCHEMA_REGISTRY_URL=${KAFKA_SR_URL}
SPRING_KAFKA_PROPERTIES_SECURITY_PROTOCOL=SASL_SSL
SPRING_KAFKA_PROPERTIES_SASL_MECHANISM=${KAFKA_MECHANISM}
SPRING_KAFKA_PROPERTIES_SASL_JAAS_CONFIG='${LOGIN_MODULE} required username="${KAFKA_USER}" password="${KAFKA_PASSWORD}";'
EOF

# El Schema Registry de Confluent Cloud tiene su propia API key, distinta de la del
# cluster. Si existe en SSM, se anade; si no (Redpanda, o un SR sin auth), no se toca.
SR_USER="$(param_optional "${SSM_PREFIX}/kafka/schema-registry-username")"
SR_PASSWORD="$(param_optional "${SSM_PREFIX}/kafka/schema-registry-password")"
if [ -n "${SR_USER}" ] && [ -n "${SR_PASSWORD}" ]; then
  cat >> /etc/aggora/common.env << EOF
SPRING_KAFKA_PROPERTIES_BASIC_AUTH_CREDENTIALS_SOURCE=USER_INFO
SPRING_KAFKA_PROPERTIES_SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO=${SR_USER}:${SR_PASSWORD}
EOF
fi

chown root:aggora /etc/aggora/common.env
chmod 640 /etc/aggora/common.env

# Un fichero por servicio. Todos existen SIEMPRE, aunque solo tengan comentarios: si falta
# el EnvironmentFile, systemd se niega a arrancar la unidad.
#
# Ninguno repite JAVA_OPTS ni las claves comunes: eso esta en la unidad y en common.env,
# que es lo que hace que una sola plantilla valga para los siete.

cat > /etc/aggora/market-data-simulator.env << EOF
# El unico que habla con las APIs de datos. Sin clave arranca igual (precios semilla y
# ticks sinteticos), por eso las dos pueden estar vacias.
HEALTH_URL=http://localhost:8080/actuator/health
TWELVEDATA_API_KEY=$(param_optional "${SSM_PREFIX}/providers/twelvedata-api-key")
ALPHAVANTAGE_API_KEY=$(param_optional "${SSM_PREFIX}/providers/alphavantage-api-key")
EOF

cat > /etc/aggora/analytics-streams.env << EOF
HEALTH_URL=http://localhost:8085/actuator/health
SPRING_KAFKA_STREAMS_STATE_DIR=/var/lib/aggora/streams/analytics-streams
EOF

# Sin servidor web no hay /actuator/health: este servicio no tiene sonda HTTP y su
# reinicio depende de Restart=always (caida) y del lag en kafka-exporter (atasco).
cat > /etc/aggora/order-matching-engine.env << 'EOF'
# Solo hereda common.env.
EOF

cat > /etc/aggora/portfolio-risk.env << EOF
# Sin servidor web: el state store va a disco, no a /tmp (que en systemd puede ser tmpfs).
SPRING_KAFKA_STREAMS_STATE_DIR=/var/lib/aggora/streams/portfolio-risk
EOF

cat > /etc/aggora/alerting-service.env << EOF
SPRING_KAFKA_STREAMS_STATE_DIR=/var/lib/aggora/streams/alerting-service
EOF

cat > /etc/aggora/audit-log.env << EOF
# Postgres corre en Docker, en la misma maquina.
SPRING_DATASOURCE_URL=jdbc:postgresql://localhost:5432/aggora
SPRING_DATASOURCE_USERNAME=aggora
SPRING_DATASOURCE_PASSWORD=${POSTGRES_PASSWORD}
EOF

cat > /etc/aggora/gateway-ws.env << EOF
HEALTH_URL=http://localhost:8089/actuator/health
EOF

chown root:aggora /etc/aggora/*.env
chmod 640 /etc/aggora/*.env

# --- 5. Docker de la VM (Postgres, Prometheus, Grafana, kafka-exporter) -------------
# Sin Kafka ni Schema Registry: son gestionados y no se levantan aqui.
# El kafka-exporter no acepta una lista con comas, asi que se le da un solo broker.
EXPORTER_MECHANISM="$(echo "${KAFKA_MECHANISM}" | tr '[:upper:]' '[:lower:]')"

cat > /opt/aggora/deploy/.env << EOF
POSTGRES_USER=aggora
POSTGRES_PASSWORD=${POSTGRES_PASSWORD}
POSTGRES_DB=aggora
KAFKA_BOOTSTRAP_SERVERS=${KAFKA_BOOTSTRAP}
KAFKA_EXPORTER_SERVER=${KAFKA_BOOTSTRAP%%,*}
KAFKA_SASL_USERNAME=${KAFKA_USER}
KAFKA_SASL_PASSWORD=${KAFKA_PASSWORD}
KAFKA_EXPORTER_MECHANISM=${EXPORTER_MECHANISM}
EOF
chmod 600 /opt/aggora/deploy/.env

# Si esto falla, el pipeline arranca igual: audit-log se cae, systemd lo reintenta y en
# cuanto el compose levante, entra. Preferimos una VM a medio arrancar y que se arregle
# sola a un user_data que se corta aqui y no arranca ni un servicio.
cd /opt/aggora/deploy
docker compose --env-file /opt/aggora/deploy/.env -f docker-compose.vm.yml up -d ||
  log "AVISO: docker compose ha fallado; los servicios arrancan igual y audit-log reintentara"

# --- 6. Servicios, en orden ---------------------------------------------------------
# El orden importa: el simulador es el que crea market.ticks.raw y orders.incoming, y los
# consumidores de Streams fallan si su topic de origen no existe. Aqui no se espera: si uno
# arranca demasiado pronto falla, y Restart=always lo reintenta a los 15 s (es lo que
# resuelve la carrera). Por eso el `|| true`: que un servicio no confirme el arranque a la
# primera no puede cortar el resto del user_data.
SERVICES=(
  market-data-simulator
  analytics-streams
  order-matching-engine
  portfolio-risk
  alerting-service
  audit-log
  gateway-ws
)

for service in "${SERVICES[@]}"; do
  log "habilitando aggora@${service}"
  systemctl enable "aggora@${service}.service"
  systemctl start "aggora@${service}.service" || true
done

log "listo. Estado:"
systemctl --no-pager --lines=0 status 'aggora@*' || true
