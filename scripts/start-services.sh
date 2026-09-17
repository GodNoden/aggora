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
  # El gateway va el ultimo: necesita que los topics de origen ya existan.
  "gateway-ws:Started GatewayWsApplication"
)
SERVICES_DIR=/workspaces/aggora/services/spring
LOG_DIR=${AGGORA_LOG_DIR:-/tmp}

# Guarda de tamano de log. Es la leccion del incidente: los servicios se lanzan con nohup y el
# log NO rotaba, asi que un bucle de errores del normalizer escribio 17,4 GB en
# /tmp/ingestion-normalizer.log en seis minutos y dejo la particion atascada (ver
# docs/decisions.md). Dos cinturones:
#   1. Al arrancar, si el log del servicio pasa del tope se mueve a <servicio>.log.1, guardando
#      solo la vuelta anterior.
#   2. La subshell que lanza la JVM lleva un ulimit -f de seguridad: si un log se descontrola
#      durante la ejecucion, el proceso muere al pasar del tope en vez de llenar el disco.
#      El tope de ulimit es mucho mas alto que el de rotacion porque ulimit -f vale para TODOS
#      los ficheros de la JVM y en /tmp tambien vive el estado de Kafka Streams.
LOG_MAX_BYTES=${AGGORA_LOG_MAX_BYTES:-52428800}          # 50 MB: tope para rotar al arrancar
LOG_ULIMIT_BLOQUES=${AGGORA_LOG_ULIMIT_BLOQUES:-2097152} # 2 GB en bloques de 1 KB: cinturon

# Limite de heap por servicio, el MISMO que pone la unidad de systemd del despliegue
# (deploy/systemd/aggora@.service). Sin esto, cada JVM se coge por defecto un cuarto de la RAM de
# la maquina: con siete servicios de Spring mas seis de Quarkus, los tres brokers y Grafana, la
# maquina se queda sin memoria, el GC se pone a dar vueltas y los procesos se quedan colgados SIN
# decir nada. Lo midio el test de estres de throughput (ver docs/throughput-lab.md).
SPRING_JAVA_OPTS=${SPRING_JAVA_OPTS:--Xms128m -Xmx384m}

if [ -z "${ALPHAVANTAGE_API_KEY:-}" ] && [ -f "$HOME/.secrets/alphavantage" ]; then
  ALPHAVANTAGE_API_KEY="$(cat "$HOME/.secrets/alphavantage")"
fi
export ALPHAVANTAGE_API_KEY="${ALPHAVANTAGE_API_KEY:-}"

esta_vivo() {
  # El patron es el jar, no "java -jar": los servicios arrancan con $SPRING_JAVA_OPTS en medio
  # ("java -Xms128m ... -jar target/..."), y con el patron viejo esta_vivo decia que no habia
  # nadie y el script lanzaba una SEGUNDA instancia de cada servicio.
  pgrep -f "target/$1-spring-0.1.0-SNAPSHOT.jar" >/dev/null
}

# Rota el log de un servicio si pasa del tope: mueve el actual a <log>.1 (el anterior se pisa,
# solo se guarda una vuelta) para que el arranque nuevo empiece con el fichero a cero.
rotar_log() {
  local log="$1"
  [ -f "$log" ] || return 0
  local tamano
  tamano=$(stat -c %s "$log" 2>/dev/null || echo 0)
  if [ "$tamano" -gt "$LOG_MAX_BYTES" ]; then
    mv -f "$log" "$log.1"
    echo "  ($(basename "$log") pasaba de $((LOG_MAX_BYTES / 1048576)) MB: guardado como $(basename "$log").1)"
  fi
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
  if [ ! -f "$SERVICES_DIR/$service/target/$service-spring-0.1.0-SNAPSHOT.jar" ]; then
    echo "  $service: FALTA el jar. Compila antes: cd services && mvn -q -DskipTests package"
    continue
  fi

  rotar_log "$LOG_DIR/$service.log"
  (
    cd "$SERVICES_DIR/$service"
    # Cinturon de seguridad del incidente de los 17,4 GB: esta subshell (y la JVM que lanza)
    # no puede escribir un fichero mas grande que el tope. Ver la nota de arriba.
    ulimit -f "$LOG_ULIMIT_BLOQUES" 2>/dev/null || true
    nohup java $SPRING_JAVA_OPTS -jar "target/$service-spring-0.1.0-SNAPSHOT.jar" > "$LOG_DIR/$service.log" 2>&1 &
  )
  # Se espera antes de seguir: es lo que evita la carrera de topics.
  esperar_listo "$service" "$expected" || true
done

echo
echo "Comprobaciones utiles:"
echo "  tail -f $LOG_DIR/market-data-simulator.log"
echo "  curl -s localhost:8081/subjects | head -c 200"
echo "  curl -s 'localhost:8085/analytics?symbol=EUR/USD&minutes=3' | head -c 300"
