#!/usr/bin/env bash
#
# Leccion 2: lo que no se puede procesar acaba en el DLT, con su motivo.
#
#   bash scripts/leccion-2-veneno-dlt.sh
#
# Manda a market.ticks.raw los DOS venenos y comprueba que los dos acaban en el DLT de los dos
# stacks sin tumbar a ningun consumidor:
#
#   1. un tick que es Avro PERFECTO pero invalido de contenido (precio 0): falla al VALIDAR;
#   2. bytes que NO son Avro: falla al DESERIALIZAR (el agujero que se cerro en la Fase 11).
#
# En los dos casos se mira:
#   - que market.ticks.raw.DLT y market.ticks.raw.DLT.q crezcan y lleven x-dlt-reason;
#   - que los normalizers sigan vivos y al dia (lag bajo, sin rebalanceos nuevos);
#   - que sigan publicando canonicos (market.ticks.canonical y .q crecen).
#
# Panel del dashboard que hay que mirar: descartes.
#
# HISTORIA (por que el script ahora si manda el veneno no-Avro): un mensaje que no es Avro no
# llegaba al DLT porque el deserializador falla ANTES que el codigo. Medido en vivo: el normalizer
# de Spring entro en un bucle de reintentos que escribio 17,4 GB de log en seis minutos y dejo la
# particion atascada; el de Quarkus revocaba las particiones y no volvia. Ya esta arreglado
# (ErrorHandlingDeserializer + DeadLetterPublishingRecoverer en Spring; DeserializationFailureHandler
# en Quarkus: ver docs/decisions.md). Si ejecutas esto contra un jar VIEJO, el script lo detecta,
# para el normalizer que se descontrole y te deja abajo el procedimiento de recuperacion.
#
# SEGURIDAD DEL EXPERIMENTO: mientras espera, el script vigila el tamano de los dos logs. Si uno
# pasa del tope (AGGORA_LOG_ABORTO_BYTES, 200 MB por defecto) mata a ese normalizer y sale: asi no
# se puede repetir el incidente de los 17,4 GB aunque el jar sea el antiguo.
#
# Dentro del devcontainer, que es donde corren los servicios y donde hay docker.

set -uo pipefail

TOPIC_RAW="${TOPIC_RAW:-market.ticks.raw}"
TOPIC_DLT="${TOPIC_DLT:-market.ticks.raw.DLT}"
TOPIC_DLT_Q="${TOPIC_DLT_Q:-market.ticks.raw.DLT.q}"
TOPIC_CANONICAL="${TOPIC_CANONICAL:-market.ticks.canonical}"
TOPIC_CANONICAL_Q="${TOPIC_CANONICAL_Q:-market.ticks.canonical.q}"
KAFKA=aggora-kafka-1
KAFKA_BIN=/opt/kafka/bin
KAFKA_BROKERS=localhost:9092
LOG_DIR="${AGGORA_LOG_DIR:-/tmp}"
LOG_SPRING="$LOG_DIR/ingestion-normalizer.log"
LOG_QUARKUS="$LOG_DIR/ingestion-normalizer-q.log"
# Tope de aborto: mucho mas alto que el de rotacion de los scripts de arranque, porque aqui no
# se rota, se vigila. 200 MB ya es un log imposible para un servicio sano.
LOG_ABORTO_BYTES=${AGGORA_LOG_ABORTO_BYTES:-209715200}
# La URL que ve el contenedor del registro desde dentro de si mismo.
REGISTRY_INTERNO="${REGISTRY_INTERNO:-http://localhost:8081}"

buscar_registro() {
  if [ -n "${SCHEMA_REGISTRY_URL:-}" ]; then echo "$SCHEMA_REGISTRY_URL"; return; fi
  for candidato in http://schema-registry:8081 http://localhost:8081; do
    if curl -sf -m 3 "$candidato/subjects" >/dev/null 2>&1; then echo "$candidato"; return; fi
  done
  echo ""
}

# Offset total (suma de las particiones) de un topic.
offsets_topic() {
  docker exec "$KAFKA" "$KAFKA_BIN/kafka-get-offsets.sh" --bootstrap-server "$KAFKA_BROKERS" \
    --topic "$1" 2>/dev/null | awk -F: '{s+=$3} END {print s+0}'
}

# Suma del LAG de un grupo de consumo.
lag_grupo() {
  docker exec "$KAFKA" "$KAFKA_BIN/kafka-consumer-groups.sh" --bootstrap-server "$KAFKA_BROKERS" \
    --describe --group "$1" 2>/dev/null | tail -n +2 | awk '{s+=$6} END {print s+0}'
}

# Suma de los offsets ya confirmados de un grupo (para ver que el consumidor AVANZA).
offset_grupo() {
  docker exec "$KAFKA" "$KAFKA_BIN/kafka-consumer-groups.sh" --bootstrap-server "$KAFKA_BROKERS" \
    --describe --group "$1" 2>/dev/null | tail -n +2 | awk '{s+=$4} END {print s+0}'
}

# Motivos x-dlt-reason de un topic de descartes, uno por linea. Se busca la cabecera alli donde
# este (Spring anade sus kafka_dlt-* antes, asi que no tiene posicion fija) y se corta en el
# tabulador que el consumidor pone entre cabeceras. El tabulador va literal ($'...'), no como
# "\t": dentro de una clase de caracteres, grep lee "\t" como "barra o t" y corta el motivo.
motivos_dlt() {
  timeout 20 docker exec "$KAFKA" "$KAFKA_BIN/kafka-console-consumer.sh" \
    --bootstrap-server "$KAFKA_BROKERS" --topic "$1" --from-beginning --timeout-ms 4000 \
    --property print.headers=true 2>/dev/null \
    | grep -ao $'x-dlt-reason:[^\t]*' | sed 's/^x-dlt-reason://' | tr -d '\r'
}

# Cuenta de rebalanceos registrados en un log (revocaciones + perdidas).
rebalanceos() {
  grep -acE 'REVOCADAS|PERDIDAS' "$1" 2>/dev/null || true
}

tamano_log() {
  stat -c %s "$1" 2>/dev/null || echo 0
}

# Devuelve 1 (y para el normalizer que corresponda) si algun log se ha descontrolado.
vigilar_logs() {
  local s q
  s=$(tamano_log "$LOG_SPRING"); q=$(tamano_log "$LOG_QUARKUS")
  if [ "$s" -le "$LOG_ABORTO_BYTES" ] && [ "$q" -le "$LOG_ABORTO_BYTES" ]; then
    return 0
  fi
  echo
  echo "  ABORTO: un log ha pasado de $((LOG_ABORTO_BYTES / 1048576)) MB."
  echo "  Log de Spring: $s bytes | log de Quarkus: $q bytes"
  echo "  Esto es lo que hacia el jar VIEJO: bucle de RecordDeserializationException. Paro los"
  echo "  normalizers para que no siga escribiendo."
  pkill -f "ingestion-normalizer-spring-0.1.0-SNAPSHOT.jar" 2>/dev/null || true
  pkill -f "ticks-raw.group.id=ingestion-normalizer-q" 2>/dev/null || true
  echo
  procedimiento_recuperacion
  exit 2
}

procedimiento_recuperacion() {
  cat <<'FIN'

  RECUPERACION (la que se uso en el incidente de los 17,4 GB):
    # 1. parar los servicios que escriben el log
    pkill -f ingestion-normalizer-spring-0.1.0-SNAPSHOT.jar
    pkill -f "ticks-raw.group.id=ingestion-normalizer-q"
    # 2. borrar (o rotar) el log gigante
    rm -f /tmp/ingestion-normalizer.log /tmp/ingestion-normalizer-q.log
    # 3. dejar los dos grupos en el final del topic, que el veneno ya no interesa
    docker exec aggora-kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
      --group ingestion-normalizer   --topic market.ticks.raw --reset-offsets --to-latest --execute
    docker exec aggora-kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
      --group ingestion-normalizer-q --topic market.ticks.raw --reset-offsets --to-latest --execute
    # 4. volver a arrancar (los scripts ya rotan el log si hacia falta)
    bash scripts/start-services.sh
    bash scripts/start-quarkus-stack.sh
FIN
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

echo "== Leccion 2: los dos venenos acaban en el DLT con su motivo =="
echo "Topic:   $TOPIC_RAW (Avro, esquema id $ID)"
echo "DLT:     $TOPIC_DLT (Spring) y $TOPIC_DLT_Q (Quarkus)"
echo "Caso a:  tick Avro valido con precio 0 -> falla VALIDACION"
echo "Caso b:  bytes que no son Avro      -> falla DESERIALIZACION"

# --- Estado inicial -------------------------------------------------------------
DLT_S0="$(offsets_topic "$TOPIC_DLT")"
DLT_Q0="$(offsets_topic "$TOPIC_DLT_Q")"
CANON_S0="$(offsets_topic "$TOPIC_CANONICAL")"
CANON_Q0="$(offsets_topic "$TOPIC_CANONICAL_Q")"
LAG_S0="$(lag_grupo ingestion-normalizer)"
LAG_Q0="$(lag_grupo ingestion-normalizer-q)"
GRUPO_S0="$(offset_grupo ingestion-normalizer)"
GRUPO_Q0="$(offset_grupo ingestion-normalizer-q)"
REB_S0="$(rebalanceos "$LOG_SPRING")"
REB_Q0="$(rebalanceos "$LOG_QUARKUS")"

echo
echo "== 1. Antes =="
echo "  DLT:      spring=$DLT_S0  quarkus=$DLT_Q0"
echo "  canonico: spring=$CANON_S0  quarkus=$CANON_Q0"
echo "  lag:      spring=$LAG_S0  quarkus=$LAG_Q0"

# --- Caso a: Avro con precio 0 --------------------------------------------------
AHORA=$(( $(date +%s) * 1000 ))
# price "\u0000" es el decimal 0.0000 con escala 4: un byte 0x00, que es como Avro mete un decimal
# en JSON. El resto del tick es el contrato real de market.ticks.raw.
JSON='{"eventId":"leccion-2-precio-cero","symbol":"EUR/USD","assetClass":"FX","exchange":"FX","currency":"USD","price":"\u0000","size":1,"eventTime":'"$AHORA"',"source":"SYNTHETIC","sequence":-1}'

echo
echo "== 2. Caso a: tick Avro con precio 0 =="
timeout 30 docker exec -i aggora-schema-registry kafka-avro-console-producer \
  --bootstrap-server kafka-1:9092 --topic "$TOPIC_RAW" \
  --property schema.registry.url="$REGISTRY_INTERNO" \
  --property value.schema.id="$ID" >/dev/null 2>&1 <<< "$JSON"
if [ "$?" != "0" ]; then
  echo "  El productor fallo. No se ha mandado nada."
  exit 1
fi
echo "  enviado"

for _ in $(seq 1 20); do
  vigilar_logs
  [ "$(offsets_topic "$TOPIC_DLT")" -gt "$DLT_S0" ] && [ "$(offsets_topic "$TOPIC_DLT_Q")" -gt "$DLT_Q0" ] && break
  sleep 1
done
DLT_S1="$(offsets_topic "$TOPIC_DLT")"
DLT_Q1="$(offsets_topic "$TOPIC_DLT_Q")"
echo "  DLT ahora: spring=$DLT_S1  quarkus=$DLT_Q1"

# --- Caso b: bytes que no son Avro ----------------------------------------------
echo
echo "== 3. Caso b: bytes que no son Avro (el agujero que se cerro) =="
printf 'esto no es avro, es basura\n' | timeout 20 docker exec -i "$KAFKA" \
  "$KAFKA_BIN/kafka-console-producer.sh" --bootstrap-server "$KAFKA_BROKERS" --topic "$TOPIC_RAW" >/dev/null 2>&1
if [ "$?" != "0" ]; then
  echo "  El productor de bytes fallo. No se ha mandado nada."
  exit 1
fi
echo "  enviado"

for _ in $(seq 1 20); do
  vigilar_logs
  [ "$(offsets_topic "$TOPIC_DLT")" -gt "$DLT_S1" ] && [ "$(offsets_topic "$TOPIC_DLT_Q")" -gt "$DLT_Q1" ] && break
  sleep 1
done

# --- Estado final, con un margen para que el lag se asiente ---------------------
sleep 6
vigilar_logs

DLT_S2="$(offsets_topic "$TOPIC_DLT")"
DLT_Q2="$(offsets_topic "$TOPIC_DLT_Q")"
CANON_S2="$(offsets_topic "$TOPIC_CANONICAL")"
CANON_Q2="$(offsets_topic "$TOPIC_CANONICAL_Q")"
LAG_S2="$(lag_grupo ingestion-normalizer)"
LAG_Q2="$(lag_grupo ingestion-normalizer-q)"
GRUPO_S2="$(offset_grupo ingestion-normalizer)"
GRUPO_Q2="$(offset_grupo ingestion-normalizer-q)"
REB_S2="$(rebalanceos "$LOG_SPRING")"
REB_Q2="$(rebalanceos "$LOG_QUARKUS")"

echo
echo "== 4. Los motivos en el DLT =="
MOTIVOS_S="$(motivos_dlt "$TOPIC_DLT")"
MOTIVOS_Q="$(motivos_dlt "$TOPIC_DLT_Q")"
echo "  $TOPIC_DLT:"
echo "$MOTIVOS_S" | sed 's/^/    x-dlt-reason: /'
echo "  $TOPIC_DLT_Q:"
echo "$MOTIVOS_Q" | sed 's/^/    x-dlt-reason: /'

# --- Veredicto ------------------------------------------------------------------
echo
echo "== 5. Veredicto =="
FALLOS=0
comprobar() { # $1 = descripcion, $2 = condicion (0/1)
  if [ "$2" = "0" ]; then echo "  OK: $1"; else echo "  FALLO: $1"; FALLOS=$((FALLOS + 1)); fi
}

[ "$DLT_S2" -gt "$DLT_S1" ]; comprobar "el veneno no-Avro llego al DLT de Spring ($DLT_S1 -> $DLT_S2)" "$?"
[ "$DLT_Q2" -gt "$DLT_Q1" ]; comprobar "el veneno no-Avro llego al DLT de Quarkus ($DLT_Q1 -> $DLT_Q2)" "$?"
echo "$MOTIVOS_S" | grep -q "precio ausente o no positivo"; comprobar "Spring: motivo de validacion en la cabecera" "$?"
echo "$MOTIVOS_Q" | grep -q "precio ausente o no positivo"; comprobar "Quarkus: motivo de validacion en la cabecera" "$?"
echo "$MOTIVOS_S" | grep -q "fallo de deserializacion"; comprobar "Spring: motivo de deserializacion en la cabecera" "$?"
echo "$MOTIVOS_Q" | grep -q "fallo de deserializacion"; comprobar "Quarkus: motivo de deserializacion en la cabecera" "$?"
[ "$GRUPO_S2" -gt "$GRUPO_S0" ]; comprobar "Spring sigue consumiendo (offset del grupo $GRUPO_S0 -> $GRUPO_S2)" "$?"
[ "$GRUPO_Q2" -gt "$GRUPO_Q0" ]; comprobar "Quarkus sigue consumiendo (offset del grupo $GRUPO_Q0 -> $GRUPO_Q2)" "$?"
[ "$LAG_S2" -lt 500 ]; comprobar "Spring sin lag disparado (lag=$LAG_S2)" "$?"
[ "$LAG_Q2" -lt 500 ]; comprobar "Quarkus sin lag disparado (lag=$LAG_Q2)" "$?"
[ "$REB_S2" = "$REB_S0" ]; comprobar "Spring sin rebalanceos nuevos ($REB_S0 -> $REB_S2)" "$?"
[ "$REB_Q2" = "$REB_Q0" ]; comprobar "Quarkus sin rebalanceos nuevos ($REB_Q0 -> $REB_Q2)" "$?"
[ "$CANON_S2" -gt "$CANON_S0" ]; comprobar "Spring sigue publicando canonicos ($CANON_S0 -> $CANON_S2)" "$?"
[ "$CANON_Q2" -gt "$CANON_Q0" ]; comprobar "Quarkus sigue publicando canonicos ($CANON_Q0 -> $CANON_Q2)" "$?"

echo
echo "  Tamanos de log: spring=$(tamano_log "$LOG_SPRING") quarkus=$(tamano_log "$LOG_QUARKUS") bytes"

echo
if [ "$FALLOS" = "0" ]; then
  echo "  TODO OK: los dos venenos estan en los dos DLT con su motivo y ningun consumidor se ha"
  echo "  caido. El agujero del DLT por fallo de deserializacion esta cerrado."
  echo
  echo "Mira el panel 'descartes': cada veneno suma uno al offset de market.ticks.raw.DLT y de"
  echo "market.ticks.raw.DLT.q. Si el offset se queda quieto, el que esta mal es el consumidor."
  exit 0
fi
echo "  FALLO: algo no cuadra. Si el jar es viejo, esto es lo que hay que hacer:"
procedimiento_recuperacion
exit 1
