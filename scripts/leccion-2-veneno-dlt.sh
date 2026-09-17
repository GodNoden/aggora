#!/usr/bin/env bash
#
# Leccion 2: lo que no se puede procesar acaba en el DLT, con su motivo.
#
#   bash scripts/leccion-2-veneno-dlt.sh
#
# Manda a market.ticks.raw un tick que es Avro PERFECTO pero invalido de contenido (precio 0) y
# comprueba que el normalizer lo manda a market.ticks.raw.DLT con la cabecera x-dlt-reason.
#
# Panel del dashboard que hay que mirar: descartes. El offset de market.ticks.raw.DLT sube de uno
# en uno con cada veneno; el de market.ticks.raw.DLT.q sube tambien porque el normalizer de Quarkus
# recibe su propia copia del mensaje.
#
# AVISO MEDIDO EN VIVO (y la razon de que este script NO lo haga por defecto): un mensaje que NO
# es Avro NO llega al DLT. El deserializador falla ANTES que el codigo, asi que el unico DLT que
# hay (el que escribe TickConsumer/TickNormalizer para lo que falla VALIDACION) no lo ve. Y en el
# normalizer de Spring eso mete al consumidor en un bucle de reintentos que escribio 17,4 GB de log
# en seis minutos y dejo la particion atascada. Con el stack de Quarkus el consumidor revoca las
# particiones y no vuelve.
#
# Para verlo a mano (a sabiendas del destrozo):
#   printf 'esto no es avro\n' | docker exec -i aggora-kafka-1 \
#     /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic market.ticks.raw
# y la recuperacion, con los servicios parados:
#   rm -f /tmp/ingestion-normalizer.log /tmp/ingestion-normalizer-q.log
#   docker exec aggora-kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
#     --group ingestion-normalizer   --topic market.ticks.raw --reset-offsets --to-latest --execute
#   docker exec aggora-kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
#     --group ingestion-normalizer-q --topic market.ticks.raw --reset-offsets --to-latest --execute
#   bash scripts/start-services.sh
#
# Dentro del devcontainer, que es donde corren los servicios y donde hay docker.

set -uo pipefail

TOPIC_RAW="${TOPIC_RAW:-market.ticks.raw}"
TOPIC_DLT="${TOPIC_DLT:-market.ticks.raw.DLT}"
KAFKA=aggora-kafka-1
KAFKA_BIN=/opt/kafka/bin
KAFKA_BROKERS=localhost:9092
# La URL que ve el contenedor del registro desde dentro de si mismo.
REGISTRY_INTERNO="${REGISTRY_INTERNO:-http://localhost:8081}"

buscar_registro() {
  if [ -n "${SCHEMA_REGISTRY_URL:-}" ]; then echo "$SCHEMA_REGISTRY_URL"; return; fi
  for candidato in http://localhost:8081 http://schema-registry:8081; do
    if curl -sf -m 3 "$candidato/subjects" >/dev/null 2>&1; then echo "$candidato"; return; fi
  done
  echo ""
}

dlt_offset_total() {
  docker exec "$KAFKA" "$KAFKA_BIN/kafka-get-offsets.sh" --bootstrap-server "$KAFKA_BROKERS" \
    --topic "$TOPIC_DLT" 2>/dev/null | awk -F: '{s+=$3} END {print s+0}'
}

REGISTRO="$(buscar_registro)"
if [ -z "$REGISTRO" ]; then
  echo "No encuentro el Schema Registry. Arranca la infraestructura con:"
  echo "  docker compose -f infra/docker-compose.yml up -d"
  exit 1
fi

ID="$(curl -s "$REGISTRO/subjects/$TOPIC_RAW-value/versions/latest" | jq -r '.id')"
if [ -z "$ID" ] || [ "$ID" = "null" ]; then
  echo "El subject $TOPIC_RAW-value no esta registrado todavia: arranca el simulador y espera unos segundos."
  exit 1
fi

echo "== Leccion 2: el veneno va al DLT con su motivo =="
echo "Topic:   $TOPIC_RAW (Avro, esquema id $ID)"
echo "DLT:     $TOPIC_DLT"
echo "Se va a mandar UN tick con symbol EUR/USD y precio 0. El tick es Avro valido (lo escribimos"
echo "con el serializador de Confluent y el MISMO esquema del registro), asi que el normalizer lo"
echo "lee bien y lo rechaza por contenido: 'precio ausente o no positivo'."

ANTES="$(dlt_offset_total)"
echo
echo "== 1. Descartes antes: $ANTES =="

AHORA=$(( $(date +%s) * 1000 ))
# price "\u0000" es el decimal 0.0000 con escala 4: un byte 0x00, que es como Avro mete un decimal
# en JSON. El resto del tick es el contrato real de market.ticks.raw.
JSON='{"eventId":"leccion-2-precio-cero","symbol":"EUR/USD","assetClass":"FX","exchange":"FX","currency":"USD","price":"\u0000","size":1,"eventTime":'"$AHORA"',"source":"SYNTHETIC","sequence":-1}'

timeout 30 docker exec -i aggora-schema-registry kafka-avro-console-producer \
  --bootstrap-server kafka-1:9092 --topic "$TOPIC_RAW" \
  --property schema.registry.url="$REGISTRY_INTERNO" \
  --property value.schema.id="$ID" >/dev/null 2>&1 <<< "$JSON"
CODIGO=$?
if [ "$CODIGO" != "0" ]; then
  echo "El productor fallo (codigo $CODIGO). No se ha mandado nada."
  exit 1
fi
echo "  tick con precio 0 mandado a $TOPIC_RAW"

echo
echo "== 2. Esperando a que el DLT avance =="
DESPUES="$ANTES"
for _ in $(seq 1 15); do
  DESPUES="$(dlt_offset_total)"
  if [ "$DESPUES" -gt "$ANTES" ]; then break; fi
  sleep 1
done
echo "  descartes ahora: $DESPUES"

echo
echo "== 3. El mensaje descartado, con su cabecera =="
# El consumidor de Avro pinta las cabeceras como "null" (es un detalle suyo): para ver el valor de
# x-dlt-reason hay que leerlo con el consumidor de bytes y quedarse con la cabecera, que es texto.
LINEA="$(timeout 40 docker exec "$KAFKA" "$KAFKA_BIN/kafka-console-consumer.sh" \
  --bootstrap-server "$KAFKA_BROKERS" --topic "$TOPIC_DLT" --from-beginning --timeout-ms 6000 \
  --property print.headers=true 2>/dev/null \
  | grep -a 'x-dlt-reason' | tail -1 | cut -f1)"
if [ -n "$LINEA" ]; then
  echo "  $LINEA"
else
  echo "  (no he podido leer el DLT)"
fi

echo
echo "== 4. Veredicto =="
if [ "$DESPUES" -gt "$ANTES" ] && echo "$LINEA" | grep -q "x-dlt-reason:precio ausente o no positivo"; then
  echo "  OK: el tick invalido esta en el DLT y con el motivo en la cabecera."
  echo "  El offset subio de $ANTES a $DESPUES: no se pierde nada de lo que falla validacion."
  echo
  echo "Mira el panel 'descartes': cada veneno suma uno al offset de market.ticks.raw.DLT (y al de"
  echo "market.ticks.raw.DLT.q). Si el offset se queda quieto, el que esta mal es el consumidor."
  exit 0
fi
echo "  FALLO: el tick invalido no aparece en el DLT con su cabecera."
echo "  Mira el log del normalizer: /tmp/ingestion-normalizer.log"
exit 1
