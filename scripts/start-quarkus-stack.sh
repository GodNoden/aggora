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
#
# Todo se pasa como propiedades del sistema (-D) y no como variables de entorno: los nombres de
# canal llevan guiones y el mapeo de Quarkus para el entorno es exacto, mientras que `-D` usa el
# nombre literal. Menos sorpresas.

set -euo pipefail

SERVICES_DIR=/workspaces/aggora/services/quarkus
LOG_DIR=${AGGORA_LOG_DIR:-/tmp}
SUFIJO=${AGGORA_SUFIJO:-q}

# "servicio|propiedades -D separadas por espacios"
STACK=(
  "ingestion-normalizer|-Dmp.messaging.incoming.ticks-raw.group.id=ingestion-normalizer-$SUFIJO -Dmp.messaging.outgoing.ticks-canonical.topic=market.ticks.canonical.$SUFIJO -Dmp.messaging.outgoing.fx-reference.topic=market.fx.reference.$SUFIJO -Dmp.messaging.outgoing.ticks-raw-dlt.topic=market.ticks.raw.DLT.$SUFIJO -Daggora.topics.ticks-canonical=market.ticks.canonical.$SUFIJO -Daggora.topics.fx-reference=market.fx.reference.$SUFIJO -Daggora.topics.ticks-raw-dlt=market.ticks.raw.DLT.$SUFIJO"
  "analytics-streams|-Dquarkus.http.port=8185 -Dquarkus.kafka-streams.application-id=analytics-streams-$SUFIJO -Dquarkus.kafka-streams.topics=market.ticks.canonical.$SUFIJO,market.fx.reference.$SUFIJO -Dkafka-streams.state.dir=/tmp/aggora-streams-state-$SUFIJO -Daggora.topics.ticks-canonical=market.ticks.canonical.$SUFIJO -Daggora.topics.fx-reference=market.fx.reference.$SUFIJO -Daggora.topics.analytics=market.analytics.$SUFIJO -Daggora.topics.arbitrage=market.arbitrage.$SUFIJO"
  "order-matching-engine|-Dmp.messaging.incoming.orders-incoming.group.id=order-matching-engine-$SUFIJO -Dmp.messaging.outgoing.executions.topic=orders.executions.$SUFIJO -Dmp.messaging.outgoing.orders-incoming-dlt.topic=orders.incoming.DLT.$SUFIJO -Daggora.topics.executions=orders.executions.$SUFIJO"
  "portfolio-risk|-Dquarkus.kafka-streams.application-id=portfolio-risk-$SUFIJO -Dquarkus.kafka-streams.topics=orders.executions.$SUFIJO -Dkafka-streams.state.dir=/tmp/aggora-portfolio-state-$SUFIJO -Daggora.topics.executions=orders.executions.$SUFIJO -Daggora.topics.portfolio-updates=portfolio.updates.$SUFIJO"
  "alerting-service|-Dquarkus.kafka-streams.application-id=alerting-service-$SUFIJO -Dquarkus.kafka-streams.topics=market.analytics.$SUFIJO,portfolio.updates.$SUFIJO -Dkafka-streams.state.dir=/tmp/aggora-alerting-state-$SUFIJO -Daggora.topics.analytics=market.analytics.$SUFIJO -Daggora.topics.portfolio-updates=portfolio.updates.$SUFIJO -Daggora.topics.alerts-raised=alerts.raised.$SUFIJO"
  "gateway-ws|-Dquarkus.http.port=8289 -Daggora.topics.ticks-canonical=market.ticks.canonical.$SUFIJO -Daggora.topics.portfolio-updates=portfolio.updates.$SUFIJO -Daggora.topics.alerts=alerts.raised.$SUFIJO -Dmp.messaging.incoming.ticks-canonical.group.id=gateway-ws-quarkus-$SUFIJO -Dmp.messaging.incoming.portfolio-updates.group.id=gateway-ws-quarkus-$SUFIJO -Dmp.messaging.incoming.alerts.group.id=gateway-ws-quarkus-$SUFIJO"
  "audit-log|-Dmp.messaging.incoming.executions.group.id=audit-log-$SUFIJO -Dmp.messaging.incoming.portfolio-updates.group.id=audit-log-$SUFIJO -Dmp.messaging.incoming.alerts.group.id=audit-log-$SUFIJO -Dmp.messaging.incoming.executions.topic=orders.executions.$SUFIJO -Dmp.messaging.incoming.portfolio-updates.topic=portfolio.updates.$SUFIJO -Dmp.messaging.incoming.alerts.topic=alerts.raised.$SUFIJO -Dmp.messaging.outgoing.audit-events.topic=audit.events.$SUFIJO -Daggora.topics.executions=orders.executions.$SUFIJO -Daggora.topics.portfolio-updates=portfolio.updates.$SUFIJO -Daggora.topics.alerts=alerts.raised.$SUFIJO -Daggora.topics.audit-events=audit.events.$SUFIJO"
)

esta_vivo() { pgrep -f "[j]ava .*$SERVICES_DIR/$1/target/quarkus-app/quarkus-run.jar" >/dev/null; }

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
  (
    cd "$SERVICES_DIR/$servicio"
    # shellcheck disable=SC2086
    nohup java $propiedades -jar target/quarkus-app/quarkus-run.jar > "$LOG_DIR/$servicio-$SUFIJO.log" 2>&1 &
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
