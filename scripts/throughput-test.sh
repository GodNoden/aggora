#!/usr/bin/env bash
#
# Test de estres de throughput (Fase 10): empuja LOS DOS pipelines a la vez, con la MISMA entrada,
# por encima de lo que da el simulador, y mide quien aguanta y a que coste.
#
#   bash scripts/throughput-test.sh                 # 60 s por tasa y las tasas 300, 1500, 5000
#   bash scripts/throughput-test.sh 90 500 2000     # 90 s por tasa
#
# Se corre DENTRO del devcontainer (es donde esta Java y donde corren los servicios).
#
# Que mide y por que asi:
#
#   - La entrada es comun a proposito: los dos stacks leen market.ticks.raw con grupos distintos, asi
#     que la comparacion es con la misma carga y en la misma maquina.
#   - El simulador se PARA durante el experimento: si sigue produciendo, la tasa de entrada es
#     desconocida y el resultado no vale.
#   - El resultado no es solo "mensajes por segundo". A esta escala lo normal es que los dos
#     frameworks aguanten, y entonces lo que separa a los dos es el COSTE: milisegundos de CPU por
#     mensaje procesado, leidos de /proc. Por eso se mide CPU antes y despues de cada tasa.
#   - Y hay una comprobacion de validez: si el topic de descartes crece, el generador esta mandando
#     algo que no pasa el validador y lo que se ha medido es el camino del DLT. Ese resultado se
#     marca como NO VALIDO en vez de publicarse.
#
# Lo que NO puede decir este experimento, y hay que decirlo: el generador, los dos stacks, los tres
# brokers y el Schema Registry comparten la misma CPU y el mismo disco. Los numeros son comparables
# ENTRE SI (misma maquina, misma carga, a la vez) y no son absolutos de produccion.

set -uo pipefail

# Desde la raiz del repo, se llame desde donde se llame: las rutas de abajo son relativas a ella.
cd "$(dirname "$0")/.."

SEGUNDOS=${1:-60}
shift || true
TASAS=("$@")
if [ ${#TASAS[@]} -eq 0 ]; then
  TASAS=(300 1500 5000)
fi

KAFKA=aggora-kafka-1
BOOTSTRAP="kafka-1:9092,kafka-2:9092,kafka-3:9092"
REGISTRY="http://schema-registry:8081"
CP_FILE=/tmp/aggora-throughput-cp.txt
CLASSES=/workspaces/aggora/services/spring/order-matching-engine/target/classes
SIMULADOR=services/spring/market-data-simulator
RESULTADOS=/tmp/aggora-throughput.tsv
DTOPE_DRENAJE=${DTOPE_DRENAJE:-120}

RAW=market.ticks.raw
DLT_SPRING=market.ticks.raw.DLT
DLT_QUARKUS=market.ticks.raw.DLT.q
CAN_SPRING=market.ticks.canonical
CAN_QUARKUS=market.ticks.canonical.q
ANA_SPRING=market.analytics
ANA_QUARKUS=market.analytics.q

# --- herramientas de Kafka (viven en el contenedor del broker) -------------------
kafka_tool() { docker exec "$KAFKA" /opt/kafka/bin/"$@"; }

offset_de() {
  kafka_tool kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic "$1" 2>/dev/null |
    awk -F: '{s+=$3} END {print s+0}'
}

# Suma de LAG de un grupo; "-" (sin consumidor asignado) no cuenta.
lag_de() {
  kafka_tool kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group "$1" 2>/dev/null |
    awk '$6 ~ /^[0-9]+$/ {s+=$6} END {print s+0}'
}

# Un grupo SIN consumidores tiene lag 0 en la tabla (la columna LAG sale "-"), y confundir eso con
# "va al dia" es el error clasico: un pipeline muerto parece sano. Se comprueba aparte.
grupo_vivo() {
  kafka_tool kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group "$1" 2>/dev/null |
    awk 'NR>1 && $7 != "" && $7 != "-" {n++} END {print (n>0) ? "si" : "no"}'
}

lag_total() {
  echo $(( $(lag_de ingestion-normalizer) + $(lag_de analytics-streams) \
         + $(lag_de ingestion-normalizer-q) + $(lag_de analytics-streams-q) ))
}

# --- CPU por proceso, desde /proc ------------------------------------------------
# El nombre del servicio sale del directorio de trabajo (los runners de Quarkus no llevan el nombre
# en la linea de comandos) y el stack, de la ruta. Los servidores de lenguaje del editor tambien son
# java: se descartan porque su cwd no esta bajo services/.
cpu_snapshot() {
  local salida="$1"
  : > "$salida"
  local pid exe cwd stack servicio ticks
  for pid in $(pgrep -f "[j]ava" 2>/dev/null); do
    # Solo procesos cuya EJECUTABLE es java: el envoltorio `bash -lc "... java ..."` tambien sale en
    # pgrep y comparte directorio de trabajo con el servicio, asi que contarlo dos veces cruzaba las
    # filas y sacaba deltas de CPU negativos.
    exe=$(readlink -f "/proc/$pid/exe" 2>/dev/null) || continue
    case "$exe" in */java) ;; *) continue ;; esac
    cwd=$(readlink -f "/proc/$pid/cwd" 2>/dev/null) || continue
    case "$cwd" in
      */services/spring/*) stack=spring ;;
      */services/quarkus/*) stack=quarkus ;;
      *) continue ;;
    esac
    servicio=$(basename "$cwd")
    ticks=$(awk '{print $14+$15}' "/proc/$pid/stat" 2>/dev/null) || continue
    [ -n "$ticks" ] && echo "$stack/$servicio $ticks" >> "$salida"
  done
  # Se agrega por etiqueta antes de comparar: dos procesos del mismo servicio no deben cruzarse.
  sort "$salida" | awk '{s[$1]+=$2} END {for (k in s) print k, s[k]}' | sort > "$salida.tmp"
  mv "$salida.tmp" "$salida"
}

# Imprime "clave ms" de la diferencia entre dos snapshots (en milisegundos de CPU).
cpu_delta() {
  local antes="$1" despues="$2"
  local clk
  clk=$(getconf CLK_TCK)
  join -j 1 <(sort "$antes") <(sort "$despues") 2>/dev/null |
    awk -v clk="$clk" '{printf "%s %.0f\n", $1, ($3-$2)*1000/clk}'
}

cpu_de_stack() { # stack servicio -> ms
  local etiqueta="$1" fichero="$2"
  awk -v e="$etiqueta" '$1==e {print $2}' "$fichero" | head -1
}

suma_cpu_stack() { # stack -> ms de todos sus procesos
  local stack="$1" fichero="$2"
  # Ojo: el acumulador NO puede llamarse igual que la variable del patron (awk la reescribe y a
  # partir de ahi deja de casar). Costo un informe entero con la CPU por mensaje mal.
  awk -v prefijo="$stack/" '$1 ~ "^"prefijo {total+=$2} END {print total+0}' "$fichero"
}

# --- preparacion ----------------------------------------------------------------
if ! docker exec "$KAFKA" true 2>/dev/null; then
  echo "ERROR: no se llega al contenedor $KAFKA. ¿Esta levantada la infraestructura?" >&2
  exit 1
fi

if [ ! -f "$CP_FILE" ]; then
  echo "==> Calculando el classpath (una vez)"
  ( cd services && mvn -q -pl spring/order-matching-engine dependency:build-classpath \
      -Dmdep.outputFile="$CP_FILE" -DincludeScope=test >/dev/null 2>&1 )
fi
if [ ! -f "$CP_FILE" ]; then
  echo "ERROR: no se pudo calcular el classpath con Maven." >&2
  exit 1
fi

if ! pgrep -f "[i]ngestion-normalizer" >/dev/null || ! pgrep -f "[i]ngestion-normalizer-q" >/dev/null; then
  echo "AVISO: no estan los dos normalizers en marcha. Arranca los dos stacks antes:"
  echo "  bash scripts/start-services.sh && bash scripts/start-quarkus-stack.sh"
  exit 1
fi

# El simulador, parado mientras dure el experimento; se vuelve a arrancar pase lo que pase.
simulador_estaba=0
if pgrep -f "[m]arket-data-simulator-spring" >/dev/null; then
  simulador_estaba=1
  echo "==> Parando el simulador (la entrada la manda el generador)"
  pkill -TERM -f "[m]arket-data-simulator-spring"
  sleep 5
fi
rearrancar_simulador() {
  if [ "$simulador_estaba" = "1" ] && ! pgrep -f "[m]arket-data-simulator-spring" >/dev/null; then
    echo "==> Arrancando otra vez el simulador"
    # `setsid` + `< /dev/null`: el script se quedaba esperando a este hijo al salir (bash espera a
    # los trabajos que ha lanzado), y con la salida en una tuberia eso es un cuelgue de diez minutos
    # que no tiene nada que ver con la medicion. Con setsid el proceso pasa a otra sesion y el script
    # termina cuando tiene que terminar.
    ( cd "$SIMULADOR" && setsid nohup java $SPRING_JAVA_OPTS \
        -jar target/market-data-simulator-spring-0.1.0-SNAPSHOT.jar \
        > /tmp/market-data-simulator.log 2>&1 < /dev/null & )
  fi
}
trap rearrancar_simulador EXIT

echo "==> Test de estres: ${SEGUNDOS}s por tasa, tasas: ${TASAS[*]}"
echo "    (los dos pipelines, la misma entrada, en la misma maquina)"
printf 'tasa\tinsertados\ttasa_real\tcan_spring\tana_spring\tcan_quarkus\tana_quarkus\tlag_max\tdrenaje_s\tcpu_spring_ms\tcpu_quarkus_ms\tmsg_spring\tmsg_quarkus\tpico_norm_spring\tpico_ana_spring\tpico_norm_quarkus\tpico_ana_quarkus\tvalido\n' > "$RESULTADOS"

for tasa in "${TASAS[@]}"; do
  echo
  echo "=== Tasa objetivo: $tasa msg/s"

  o_raw0=$(offset_de "$RAW")
  o_dlt0=$(( $(offset_de "$DLT_SPRING") + $(offset_de "$DLT_QUARKUS") ))
  o_can_s0=$(offset_de "$CAN_SPRING"); o_ana_s0=$(offset_de "$ANA_SPRING")
  o_can_q0=$(offset_de "$CAN_QUARKUS"); o_ana_q0=$(offset_de "$ANA_QUARKUS")
  cpu_snapshot /tmp/cpu-antes.txt

  inicio=$(date +%s)
  salida_generador=$(java -cp "$CLASSES:$(cat "$CP_FILE")" scripts/AvroLoadGenerator.java \
      --topic "$RAW" --rate "$tasa" --seconds "$SEGUNDOS" \
      --bootstrap "$BOOTSTRAP" --registry "$REGISTRY" 2>/dev/null | tail -1)
  tasa_real=$(echo "$salida_generador" | sed -n 's/.*= \([0-9]*\) msg\/s reales/\1/p')
  echo "    generador: $salida_generador"

  # Drenaje: se espera a que los dos pipelines vacien su lag (dos lecturas a cero seguidas).
  # Se guarda el PICO de cada grupo por separado: sin ese reparto no se sabe si el que no da abasto
  # es el normalizer (transformar y publicar) o el motor de Streams (ventanas, joins y estado).
  lag_max=0; ceros=0
  pico_ns=0; pico_as=0; pico_nq=0; pico_aq=0
  while [ $(( $(date +%s) - inicio )) -lt $(( SEGUNDOS + DTOPE_DRENAJE )) ]; do
    l_ns=$(lag_de ingestion-normalizer); l_as=$(lag_de analytics-streams)
    l_nq=$(lag_de ingestion-normalizer-q); l_aq=$(lag_de analytics-streams-q)
    lag=$(( l_ns + l_as + l_nq + l_aq ))
    [ "$l_ns" -gt "$pico_ns" ] && pico_ns=$l_ns
    [ "$l_as" -gt "$pico_as" ] && pico_as=$l_as
    [ "$l_nq" -gt "$pico_nq" ] && pico_nq=$l_nq
    [ "$l_aq" -gt "$pico_aq" ] && pico_aq=$l_aq
    [ "$lag" -gt "$lag_max" ] && lag_max=$lag
    if [ "$lag" -eq 0 ]; then
      ceros=$((ceros+1))
      [ "$ceros" -ge 2 ] && break
    else
      ceros=0
    fi
    sleep 2
  done
  fin=$(date +%s)

  o_raw1=$(offset_de "$RAW")
  o_dlt1=$(( $(offset_de "$DLT_SPRING") + $(offset_de "$DLT_QUARKUS") ))
  o_can_s1=$(offset_de "$CAN_SPRING"); o_ana_s1=$(offset_de "$ANA_SPRING")
  o_can_q1=$(offset_de "$CAN_QUARKUS"); o_ana_q1=$(offset_de "$ANA_QUARKUS")
  cpu_snapshot /tmp/cpu-despues.txt
  cpu_delta /tmp/cpu-antes.txt /tmp/cpu-despues.txt > /tmp/cpu-delta.txt

  insertados=$(( o_raw1 - o_raw0 ))
  can_s=$(( o_can_s1 - o_can_s0 )); can_q=$(( o_can_q1 - o_can_q0 ))
  ana_s=$(( o_ana_s1 - o_ana_s0 )); ana_q=$(( o_ana_q1 - o_ana_q0 ))
  dlt=$(( o_dlt1 - o_dlt0 ))
  cpu_s=$(suma_cpu_stack spring /tmp/cpu-delta.txt)
  cpu_q=$(suma_cpu_stack quarkus /tmp/cpu-delta.txt)

  # La prueba de que el pipeline aguanta NO es que el lag llegue a cero (el de Streams fluctua y una
  # lectura en mal momento da un falso "no aguanta"): es que al final haya procesado TODO lo que
  # entro. Eso es lo que se comprueba.
  valido="si"
  [ "$dlt" -gt 0 ] && valido="NO (DLT +$dlt: el generador no produce ticks validos)"
  [ "$can_s" -lt "$insertados" ] && valido="NO (el canonico de Spring proceso $can_s de $insertados)"
  [ "$can_q" -lt "$insertados" ] && valido="NO (el canonico de Quarkus proceso $can_q de $insertados)"
  for grupo in ingestion-normalizer analytics-streams ingestion-normalizer-q analytics-streams-q; do
    if [ "$(grupo_vivo "$grupo")" != "si" ]; then
      valido="NO (el grupo $grupo no tiene consumidores: el pipeline esta caido)"
    fi
  done

  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$tasa" "$insertados" "${tasa_real:-?}" "$can_s" "$ana_s" "$can_q" "$ana_q" \
    "$lag_max" "$(( fin - inicio ))" "$cpu_s" "$cpu_q" "$can_s" "$can_q" \
    "$pico_ns" "$pico_as" "$pico_nq" "$pico_aq" "$valido" >> "$RESULTADOS"

  echo "    canonical:  spring=$can_s  quarkus=$can_q"
  echo "    analytics:  spring=$ana_s  quarkus=$ana_q"
  echo "    lag maximo=$lag_max  drenaje=$(( fin - inicio ))s  DLT=+$dlt"
  echo "    pico por grupo: normalizer spring=$pico_ns analytics spring=$pico_as | quarkus=$pico_nq / $pico_aq"
  cpu_delta /tmp/cpu-antes.txt /tmp/cpu-despues.txt | sort
done

echo
echo "=== RESUMEN (esta en $RESULTADOS)"
awk -F'\t' 'NR==1 {next}
  { printf "  %5s msg/s objetivo | entrada %7s (real %5s/s) | canonical spring %7s quarkus %7s | analytics spring %7s quarkus %7s | drenaje %4ss | CPU/mensaje spring %5.2f ms quarkus %5.2f ms | pico de lag: normalizer %6s/%6s  analytics %5s/%5s | %s\n",
      $1, $2, $3, $4, $6, $5, $7, $9,
      ($12>0 ? $10/$12 : 0), ($13>0 ? $11/$13 : 0),
      $14, $16, $15, $17, $18 }' "$RESULTADOS"

echo
echo "Como se lee esto:"
echo "  - 'valido=si' significa que el pipeline proceso TODO lo que entro (el DLT no crecio y los dos"
echo "    canonicos cuadran con la entrada). Es la unica prueba de que no se pierde nada."
echo "  - El PICO de lag por grupo dice quien no da abasto: el normalizer (transformar y publicar) o"
echo "    el motor de Streams (ventanas, joins, estado)."
echo "  - La columna que compara frameworks es CPU/mensaje: mismos mensajes, misma maquina."
echo "  - Una rafaga de 20 s mide cuanto atraso ABSORBE el pipeline y si luego se recupera, no la"
echo "    tasa sostenible: para eso, rafagas largas (p. ej. 'bash scripts/throughput-test.sh 300 1000')."
