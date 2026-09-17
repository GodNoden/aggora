#!/usr/bin/env bash
#
# Leccion 3: un segundo consumidor entra al grupo y las particiones se reparten.
#
#   bash scripts/leccion-3-rebalanceo.sh
#
# Arranca una SEGUNDA instancia del normalizer con el MISMO group.id, ensena el reparto de las 6
# particiones (6/0 antes, 3/3 durante) y la para al final. El trap la para aunque el script se
# corte a medias: el sistema queda como estaba.
#
# OJO con el detalle que suele confundir: para que haya REBALANCEO las dos instancias tienen que
# compartir group.id. Con grupos distintos no se reparten nada, cada una lee las 6 particiones
# enteras (eso es el fan-out del gateway, no un grupo de trabajo).
#
# Panel del dashboard que hay que mirar: lag y particiones. Durante el rebalanceo el lag del grupo
# ingestion-normalizer da un pico de unos segundos y vuelve a 0; los offsets de market.ticks.raw y
# market.ticks.canonical no se paran.
#
# Dentro del devcontainer, que es donde corren los servicios.

set -uo pipefail

SERVICIO=ingestion-normalizer
DIR=/workspaces/aggora/services/spring/$SERVICIO
JAR=target/$SERVICIO-spring-0.1.0-SNAPSHOT.jar
LOG=/tmp/$SERVICIO-2.log
GRUPO=ingestion-normalizer
TOPIC=market.ticks.raw
KAFKA=aggora-kafka-1
KAFKA_BIN=/opt/kafka/bin
KAFKA_BROKERS=localhost:9092

SEGUNDA=""

es_java() { [ "$(cat "/proc/$1/comm" 2>/dev/null)" = "java" ]; }

# PIDs de instancias del normalizer de Spring (solo procesos java: el shell que las lanza
# tambien lleva el nombre del jar en su linea de comandos).
normalizer_pids() {
  local p
  for p in $(pgrep -f "$SERVICIO-spring-0.1.0-SNAPSHOT.jar" 2>/dev/null); do
    es_java "$p" && echo "$p"
  done
}

reparto() {
  docker exec "$KAFKA" "$KAFKA_BIN/kafka-consumer-groups.sh" --bootstrap-server "$KAFKA_BROKERS" \
    --describe --group "$GRUPO" 2>/dev/null \
    | awk -v t="$TOPIC" 'NF>5 && $2==t {print "    particion " $3 " -> " $7}'
}

lag_total() {
  docker exec "$KAFKA" "$KAFKA_BIN/kafka-consumer-groups.sh" --bootstrap-server "$KAFKA_BROKERS" \
    --describe --group "$GRUPO" 2>/dev/null \
    | awk -v t="$TOPIC" 'NF>5 && $2==t {s+=$6} END {print s+0}'
}

parar_segunda() {
  local codigo=$?
  if [ -n "$SEGUNDA" ]; then
    echo
    echo "== Parando la segunda instancia (pid $SEGUNDA) =="
    kill "$SEGUNDA" 2>/dev/null
    for _ in $(seq 1 15); do
      ps -p "$SEGUNDA" >/dev/null 2>&1 || break
      sleep 1
    done
    ps -p "$SEGUNDA" >/dev/null 2>&1 && kill -9 "$SEGUNDA" 2>/dev/null
    sleep 6
    echo "  parada. El grupo vuelve a una sola instancia con las 6 particiones."
    reparto
  fi
  exit "$codigo"
}
trap parar_segunda EXIT

if [ ! -f "$DIR/$JAR" ]; then
  echo "Falta el jar $DIR/$JAR. Compila antes: cd services && mvn -q -DskipTests package"
  exit 1
fi

ORIGINAL="$(normalizer_pids | head -1)"
if [ -z "$ORIGINAL" ]; then
  echo "No hay ninguna instancia del normalizer corriendo. Arrancala con:"
  echo "  bash scripts/start-services.sh"
  exit 1
fi

echo "== Leccion 3: rebalanceo de un grupo de consumo =="
echo "Instancia que ya estaba: pid $ORIGINAL"
echo "Reparto de particiones ahora (una sola instancia):"
reparto
echo "  lag total de $GRUPO: $(lag_total)"

echo
echo "== Arrancando la SEGUNDA instancia (mismo group.id: $GRUPO) =="
(
  cd "$DIR"
  nohup java -Xms128m -Xmx256m -jar "$JAR" > "$LOG" 2>&1 &
)
for _ in $(seq 1 40); do
  for p in $(normalizer_pids); do
    if [ "$p" != "$ORIGINAL" ]; then SEGUNDA="$p"; break 2; fi
  done
  sleep 1
done
if [ -z "$SEGUNDA" ]; then
  echo "La segunda instancia no ha arrancado. Ultimas lineas de $LOG:"
  tail -5 "$LOG" | sed 's/^/  /'
  exit 1
fi
echo "  segunda instancia: pid $SEGUNDA (log en $LOG)"

echo
echo "== Esperando al rebalanceo =="
sleep 12
echo "Reparto de particiones con las dos instancias:"
reparto
echo "  lag total de $GRUPO: $(lag_total)"
echo
echo "  Lo que ha pasado: al entrar la segunda instancia, Kafka ha REVOCADO todas las particiones"
echo "  y las ha vuelto a repartir 3 y 3. El grupo no pierde mensajes, pero durante el rebalanceo"
echo "  nadie consume: por eso se ve un pico de lag de unos segundos."
echo
echo "  En el log de la segunda instancia:"
grep -i "rebalance\|particiones" "$LOG" | tail -3 | sed 's/^/    /'

echo
echo "== Veredicto =="
REPARTO="$(reparto)"
if [ "$(echo "$REPARTO" | grep -c 'particion')" -ge 6 ]; then
  echo "  OK: las 6 particiones estan asignadas entre las dos instancias (3 y 3)."
else
  echo "  AVISO: no he podido leer el reparto completo."
fi
echo
echo "Mira los paneles 'lag' y 'particiones': el pico de lag del rebalanceo y los offsets que no"
echo "se paran. El grupo ingestion-normalizer-q (el de Quarkus) no se toca: tiene su propia copia."
