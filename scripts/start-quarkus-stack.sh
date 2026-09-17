#!/usr/bin/env bash
#
# Arranca la implementacion de QUARKUS **a la vez** que la de Spring, que es el experimento que
# cierra la Fase 8: los dos stacks procesando la MISMA entrada, cada uno con su grupo de consumo y
# sus topics de salida, para poder compararlos con carga real.
#
#   bash scripts/start-quarkus-stack.sh          # desde el devcontainer (donde esta Java)
#   bash scripts/stop-quarkus-stack.sh
#
# Como se evita que se pisen:
#
#   - La ENTRADA es la misma: los dos leen market.ticks.raw y orders.incoming. El simulador corre
#     solo en la version Spring, a proposito: dos simuladores duplicarian el trafico y la
#     comparacion dejaria de ser A/B.
#   - El GRUPO de consumo es distinto en cada consumidor (-q), asi que Kafka reparte las particiones
#     DENTRO de cada grupo y ninguno se queda sin datos.
#   - La SALIDA va a topics propios (sufijo .q) y cada motor de Streams tiene su propio
#     application-id y su propio directorio de estado: dos aplicaciones de Streams con el mismo
#     application-id se repartirian las particiones y ninguna de las dos tendria el estado completo.
#   - Los PUERTOS son los de siempre (8185 la analitica, 8189 el gateway): son los que Prometheus
#     scrapea y los que el devcontainer reenvia al host. Un puerto distinto deja el target caido en
#     Prometheus y el dashboard sin datos de ese stack.
#
# Todo se pasa como propiedades del sistema (-D) y no como variables de entorno: los nombres de
# canal llevan guiones y el mapeo de Quarkus para el entorno es exacto, mientras que `-D` usa el
# nombre literal. Menos sorpresas.

set -euo pipefail

SERVICES_DIR=/workspaces/aggora/services/quarkus
LOG_DIR=${AGGORA_LOG_DIR:-/tmp}

# Guarda de tamano de log, la misma que en start-services.sh y por el mismo incidente: sin
# rotacion, un bucle de errores del normalizer escribio 17,4 GB en seis minutos (docs/decisions.md).
# Al arrancar se rota el log que pase del tope a <servicio>.log.1, y la subshell lleva un
# ulimit -f de seguridad para que un log descontrolado mate al proceso antes que al disco.
# El tope de ulimit es mas alto que el de rotacion porque tambien alcanza al estado de Streams.
LOG_MAX_BYTES=${AGGORA_LOG_MAX_BYTES:-52428800}          # 50 MB: tope para rotar al arrancar
LOG_ULIMIT_BLOQUES=${AGGORA_LOG_ULIMIT_BLOQUES:-2097152} # 2 GB en bloques de 1 KB: cinturon

# El mismo limite de heap que la version Spring y que la unidad de systemd del despliegue: sin el,
# trece JVM compitiendo por la memoria de la maquina acaban con el GC dando vueltas y los procesos
# colgados (medido en el test de estres: ver docs/throughput-lab.md).
QUARKUS_JAVA_OPTS=${QUARKUS_JAVA_OPTS:--Xms128m -Xmx320m}
SUFIJO=${AGGORA_SUFIJO:-q}

# "servicio|propiedades -D separadas por espacios"
STACK=(
  "ingestion-normalizer|-Dmp.messaging.incoming.ticks-raw.group.id=ingestion-normalizer-$SUFIJO -Dmp.messaging.outgoing.ticks-canonical.topic=market.ticks.canonical.$SUFIJO -Dmp.messaging.outgoing.fx-reference.topic=market.fx.reference.$SUFIJO -Dmp.messaging.outgoing.ticks-raw-dlt.topic=market.ticks.raw.DLT.$SUFIJO -Dmp.messaging.outgoing.ticks-raw-dlt-bytes.topic=market.ticks.raw.DLT.$SUFIJO -Daggora.topics.ticks-canonical=market.ticks.canonical.$SUFIJO -Daggora.topics.fx-reference=market.fx.reference.$SUFIJO -Daggora.topics.ticks-raw-dlt=market.ticks.raw.DLT.$SUFIJO"
  "analytics-streams|-Dquarkus.http.port=8185 -Dquarkus.kafka-streams.application-id=analytics-streams-$SUFIJO -Dquarkus.kafka-streams.topics=market.ticks.canonical.$SUFIJO,market.fx.reference.$SUFIJO -Dkafka-streams.state.dir=/tmp/aggora-streams-state-$SUFIJO -Daggora.topics.ticks-canonical=market.ticks.canonical.$SUFIJO -Daggora.topics.fx-reference=market.fx.reference.$SUFIJO -Daggora.topics.analytics=market.analytics.$SUFIJO -Daggora.topics.arbitrage=market.arbitrage.$SUFIJO"
  "order-matching-engine|-Dmp.messaging.incoming.orders-incoming.group.id=order-matching-engine-$SUFIJO -Dmp.messaging.outgoing.executions.topic=orders.executions.$SUFIJO -Dmp.messaging.outgoing.orders-incoming-dlt.topic=orders.incoming.DLT.$SUFIJO -Daggora.topics.executions=orders.executions.$SUFIJO"
  "portfolio-risk|-Dquarkus.kafka-streams.application-id=portfolio-risk-$SUFIJO -Dquarkus.kafka-streams.topics=orders.executions.$SUFIJO -Dkafka-streams.state.dir=/tmp/aggora-portfolio-state-$SUFIJO -Daggora.topics.executions=orders.executions.$SUFIJO -Daggora.topics.portfolio-updates=portfolio.updates.$SUFIJO"
  "alerting-service|-Dquarkus.kafka-streams.application-id=alerting-service-$SUFIJO -Dquarkus.kafka-streams.topics=market.analytics.$SUFIJO,portfolio.updates.$SUFIJO -Dkafka-streams.state.dir=/tmp/aggora-alerting-state-$SUFIJO -Daggora.topics.analytics=market.analytics.$SUFIJO -Daggora.topics.portfolio-updates=portfolio.updates.$SUFIJO -Daggora.topics.alerts-raised=alerts.raised.$SUFIJO"
  "gateway-ws|-Dquarkus.http.port=8189 -Daggora.topics.ticks-canonical=market.ticks.canonical.$SUFIJO -Daggora.topics.portfolio-updates=portfolio.updates.$SUFIJO -Daggora.topics.alerts=alerts.raised.$SUFIJO -Dmp.messaging.incoming.ticks-canonical.group.id=gateway-ws-quarkus-$SUFIJO -Dmp.messaging.incoming.portfolio-updates.group.id=gateway-ws-quarkus-$SUFIJO -Dmp.messaging.incoming.alerts.group.id=gateway-ws-quarkus-$SUFIJO"
  "audit-log|-Dmp.messaging.incoming.executions.group.id=audit-log-$SUFIJO -Dmp.messaging.incoming.portfolio-updates.group.id=audit-log-$SUFIJO -Dmp.messaging.incoming.alerts.group.id=audit-log-$SUFIJO -Dmp.messaging.incoming.executions.topic=orders.executions.$SUFIJO -Dmp.messaging.incoming.portfolio-updates.topic=portfolio.updates.$SUFIJO -Dmp.messaging.incoming.alerts.topic=alerts.raised.$SUFIJO -Dmp.messaging.outgoing.audit-events.topic=audit.events.$SUFIJO -Daggora.topics.executions=orders.executions.$SUFIJO -Daggora.topics.portfolio-updates=portfolio.updates.$SUFIJO -Daggora.topics.alerts=alerts.raised.$SUFIJO -Daggora.topics.audit-events=audit.events.$SUFIJO"
)

# esta_vivo por el DIRECTORIO de trabajo, no por el cmdline: los seis servicios de Quarkus se
# lanzan con el MISMO "-jar target/quarkus-app/quarkus-run.jar", asi que el cmdline no distingue
# uno de otro y el patron viejo buscaba una ruta absoluta que no aparece. Se mira /proc/<pid>/cwd,
# que es el directorio del servicio, y asi no se arrancan duplicados.
esta_vivo() {
  local pid
  for pid in $(pgrep -f "quarkus-app/quarkus-run.jar" 2>/dev/null || true); do
    if [ "$(readlink -f "/proc/$pid/cwd" 2>/dev/null)" = "$SERVICES_DIR/$1" ]; then
      return 0
    fi
  done
  return 1
}

# Rota el log de un servicio si pasa del tope: el actual pasa a <log>.1 y se guarda solo uno.
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

echo "Arrancando la implementacion de Quarkus EN PARALELO (sufijo .$SUFIJO, logs en $LOG_DIR/<servicio>-$SUFIJO.log)"
for entrada in "${STACK[@]}"; do
  servicio="${entrada%%|*}"
  propiedades="${entrada#*|}"
  if esta_vivo "$servicio"; then
    echo "  $servicio-$SUFIJO: ya estaba en marcha"
    continue
  fi
  if [ ! -f "$SERVICES_DIR/$servicio/target/quarkus-app/quarkus-run.jar" ]; then
    echo "  $servicio: FALTA el runner. Compila antes: cd services && mvn -q -DskipTests package"
    continue
  fi
  rotar_log "$LOG_DIR/$servicio-$SUFIJO.log"
  (
    cd "$SERVICES_DIR/$servicio"
    # Cinturon de seguridad: esta subshell (y la JVM) no escribe un fichero mayor que el tope.
    ulimit -f "$LOG_ULIMIT_BLOQUES" 2>/dev/null || true
    # shellcheck disable=SC2086
    nohup java $QUARKUS_JAVA_OPTS $propiedades -jar target/quarkus-app/quarkus-run.jar > "$LOG_DIR/$servicio-$SUFIJO.log" 2>&1 &
  )
  # Se espera a que arranque: los motores de Streams de este stack leen topics que crea el
  # normalizer de este mismo stack, asi que el orden importa igual que en el de Spring.
  sleep 12
  echo "  $servicio-$SUFIJO: lanzado"
done

echo
echo "Comprobaciones:"
echo "  grep -h 'started in' $LOG_DIR/*-$SUFIJO.log"
echo "  # los topics del stack de Quarkus, avanzando en paralelo a los de Spring:"
echo "  docker exec aggora-kafka-1 /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic market.analytics.$SUFIJO"
