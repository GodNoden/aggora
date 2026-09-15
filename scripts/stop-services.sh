#!/usr/bin/env bash
#
# Para todos los servicios de Aggora. Solo los servicios: la infraestructura (Kafka,
# Schema Registry, Postgres) se para con docker compose, y normalmente NO quieres pararla
# porque ahi vive el estado.
#
#   bash scripts/stop-services.sh

set -euo pipefail

SERVICES=(market-data-simulator ingestion-normalizer analytics-streams order-matching-engine portfolio-risk alerting-service audit-log)

for service in "${SERVICES[@]}"; do
  if pgrep -f "[j]ava -jar target/$service" >/dev/null; then
    # Se para uno a uno y con SIGTERM (no -9): asi los servicios cierran bien, los
    # consumidores se despiden del grupo y no hay que esperar al timeout de sesion.
    pkill -TERM -f "[j]ava -jar target/$service"
    echo "  $service: parado"
  else
    echo "  $service: no estaba en marcha"
  fi
done
