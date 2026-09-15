#!/usr/bin/env bash
#
# Arranca los servicios de Aggora (dentro del devcontainer, que es donde esta Java).
#
#   bash scripts/start-services.sh
#
# Dos cosas que aprendimos a golpes y que este script resuelve:
#
# 1. Los servicios son stateless a proposito: todo el estado vive en Kafka, en Postgres o
#    en los state stores con su changelog. Arrancarlos de cero no pierde nada.
# 2. EL ORDEN IMPORTA, y no basta con lanzarlos seguidos. Kafka Streams FALLA al arrancar
#    si un topic de origen no existe todavia ("MissingSourceTopicException"), y los topics
#    los crea el servicio que escribe en ellos. Si las alertas arrancan antes de que la
#    analitica haya creado market.analytics, se caen. Por eso se espera a que cada servicio
#    confirme su arranque antes de lanzar el siguiente.
#
# Las claves de los proveedores se leen del entorno. Si existe ~/.secrets/alphavantage se
# usa para ALPHAVANTAGE_API_KEY, asi no hay que exportarla cada vez.

set -euo pipefail

# "servicio:texto que aparece en el log cuando esta listo"
SERVICES=(
  "market-data-simulator:Started MarketDataSimulatorApplication"
  "ingestion-normalizer:Started IngestionNormalizerApplication"
  "analytics-streams:Started AnalyticsStreamsApplication"
  "order-matching-engine:Started OrderMatchingEngineApplication"
  "portfolio-risk:Started PortfolioRiskApplication"
  "alerting-service:Started AlertingServiceApplication"
  "audit-log:Started AuditLogApplication"
)
SERVICES_DIR=/workspaces/aggora/services
LOG_DIR=${AGGORA_LOG_DIR:-/tmp}

if [ -z "${ALPHAVANTAGE_API_KEY:-}" ] && [ -f "$HOME/.secrets/alphavantage" ]; then
  ALPHAVANTAGE_API_KEY="$(cat "$HOME/.secrets/alphavantage")"
fi
export ALPHAVANTAGE_API_KEY="${ALPHAVANTAGE_API_KEY:-}"

esta_vivo() {
  pgrep -f "[j]ava -jar target/$1" >/dev/null
}

# Espera a que el servicio confirme el arranque (o se caiga), con un tope de tiempo.
esperar_listo() {
  local service="$1" expected="$2" log="$LOG_DIR/$1.log"
  for _ in $(seq 1 45); do
    if grep -q "$expected" "$log" 2>/dev/null; then
      echo "  $service: listo"
      return 0
    fi
    if ! esta_vivo "$service"; then
      echo "  $service: SE HA CAIDO. Ultimas lineas de $log:"
      tail -3 "$log" | sed 's/^/      /'
      return 1
    fi
    sleep 1
  done
  echo "  $service: no confirmo el arranque en 45 s (mira $log)"
  return 1
}

echo "Arrancando servicios (logs en $LOG_DIR/<servicio>.log)"
for entry in "${SERVICES[@]}"; do
  service="${entry%%:*}"
  expected="${entry#*:}"

  if esta_vivo "$service"; then
    echo "  $service: ya estaba en marcha"
    continue
  fi
  if [ ! -f "$SERVICES_DIR/$service/target/$service-0.1.0-SNAPSHOT.jar" ]; then
    echo "  $service: FALTA el jar. Compila antes: cd services && mvn -q -DskipTests package"
    continue
  fi

  (
    cd "$SERVICES_DIR/$service"
    nohup java -jar "target/$service-0.1.0-SNAPSHOT.jar" > "$LOG_DIR/$service.log" 2>&1 &
  )
  # Se espera antes de seguir: es lo que evita la carrera de topics.
  esperar_listo "$service" "$expected" || true
done

echo
echo "Comprobaciones utiles:"
echo "  tail -f $LOG_DIR/market-data-simulator.log"
echo "  curl -s localhost:8081/subjects | head -c 200"
echo "  curl -s 'localhost:8085/analytics?symbol=EUR/USD&minutes=3' | head -c 300"
