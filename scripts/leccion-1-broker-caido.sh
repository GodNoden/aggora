#!/usr/bin/env bash
#
# Leccion 1: se cae un broker y el pipeline ni se entera.
#
#   bash scripts/leccion-1-broker-caido.sh [broker]     # kafka-3 por defecto
#
# Para UN broker con aviso (docker stop), ensena como cambian el ISR, el lider y el lag, y lo
# vuelve a levantar. El trap deja el cluster como estaba aunque el script se corte a medias.
#
# Panel del dashboard que hay que mirar:
#   salud      -> las particiones under-replicated pasan de 0 a N mientras el broker esta parado
#   lag        -> el lag del normalizer NO se dispara: con 3 replicas y min.insync.replicas=2
#                 se sigue leyendo y escribiendo con una copia menos
#   particiones-> los offsets de market.ticks.raw siguen subiendo (el log no se para)
#
# Dentro del devcontainer, que es donde corren los servicios y donde hay docker.

set -uo pipefail

BROKER="${1:-kafka-3}"
CONTENEDOR="aggora-$BROKER"
TOPIC="market.ticks.raw"
GRUPO="ingestion-normalizer"
KAFKA=aggora-kafka-1
KAFKA_BIN=/opt/kafka/bin
BROKERS_KAFKA=localhost:9092

PARADO="no"

# El cluster vuelve como estaba, pase lo que pase.
restaurar() {
  local codigo=$?
  if [ "$PARADO" = "si" ]; then
    echo
    echo "== Volviendo a levantar $CONTENEDOR =="
    docker start "$CONTENEDOR" >/dev/null 2>&1
    for _ in $(seq 1 40); do
      if [ "$(isr_completas)" = "6" ]; then break; fi
      sleep 1
    done
    echo "  $CONTENEDOR levantado. ISR de $TOPIC:"
    describe | grep -E "Configs|Partition:" | sed 's/^/    /'
  fi
  exit "$codigo"
}
trap restaurar EXIT

describe() {
  docker exec "$KAFKA" "$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BROKERS_KAFKA" \
    --describe --topic "$TOPIC" 2>/dev/null
}

# Cuantas particiones tienen las 3 replicas al dia.
isr_completas() {
  describe | grep -c "Isr: [0-9],[0-9],[0-9]"
}

lag_total() {
  docker exec "$KAFKA" "$KAFKA_BIN/kafka-consumer-groups.sh" --bootstrap-server "$BROKERS_KAFKA" \
    --describe --group "$GRUPO" 2>/dev/null \
    | awk 'NF>5 && $2=="'"$TOPIC"'" {s+=$6} END {print s+0}'
}

offsets() {
  docker exec "$KAFKA" "$KAFKA_BIN/kafka-get-offsets.sh" --bootstrap-server "$BROKERS_KAFKA" \
    --topic "$TOPIC" 2>/dev/null | awk -F: '{s+=$3} END {print s+0}'
}

if ! docker ps --format '{{.Names}}' | grep -qx "$CONTENEDOR"; then
  echo "No encuentro el contenedor $CONTENEDOR. Brokers disponibles:"
  docker ps --format '{{.Names}}' | grep '^aggora-kafka' | sed 's/^/  /'
  exit 1
fi

echo "== Leccion 1: broker caido =="
echo "Se va a PARAR el contenedor $CONTENEDOR (broker $BROKER) unos 15 segundos y se volvera a"
echo "levantar solo. El cluster tiene 3 brokers, 3 replicas por particion y min.insync.replicas=2,"
echo "asi que con uno menos se sigue escribiendo."

echo
echo "== 1. Estado de partida =="
describe | grep -E "Configs|Partition:" | sed 's/^/  /'
echo "  offset total de $TOPIC: $(offsets)"
echo "  lag total de $GRUPO:    $(lag_total)"
echo

echo "== 2. Cayendo el broker $BROKER =="
PARADO="si"
docker stop "$CONTENEDOR" >/dev/null
sleep 12
describe | grep "Partition:" | sed 's/^/  /'

echo
echo "== 3. Que ha cambiado =="
echo "  ISR: las particiones que tenian al broker $BROKER bajan de 3 copias a 2 (una copia fuera)."
echo "  Lider: las particiones que lideraba $BROKER eligen lider nuevo solas."
echo "  offset total de $TOPIC: $(offsets)  <- sigue subiendo: se escribe con 2 copias"
echo "  lag total de $GRUPO:    $(lag_total)  <- no se dispara: se sigue leyendo"
echo
echo "  Con DOS brokers caidos el ISR bajaria a 1 y el productor se negaria a confirmar"
echo "  (NOT_ENOUGH_REPLICAS): eso no es un fallo, es la garantia funcionando. No se prueba"
echo "  aqui para no dejar el cluster sin quorum."

echo
echo "== 4. Vuelve el broker =="
docker start "$CONTENEDOR" >/dev/null
PARADO="no"
for _ in $(seq 1 40); do
  if [ "$(isr_completas)" = "6" ]; then break; fi
  sleep 1
done
describe | grep -E "Partition:" | sed 's/^/  /'
echo "  ISR de vuelta a 3 copias en las 6 particiones: el broker se pone al dia solo."
echo
echo "Mira el panel 'salud' del dashboard: las particiones under-replicated bajan a 0."
echo "El panel 'lag' se queda plano todo el rato, que es exactamente la leccion."
