#!/usr/bin/env bash
#
# Mide el arranque y la memoria de los servicios que estan en marcha (Fase 8).
#
#   bash scripts/measure-service.sh                 # todos los que encuentre
#   bash scripts/measure-service.sh analytics-streams market-data-simulator
#
# Es la linea base para comparar las dos implementaciones: los mismos numeros, sacados de la
# misma forma, para Spring y para Quarkus (y para la JVM y para el binario nativo).
#
#   - Arranque: lo que dice el propio log. Spring escribe DOS cifras y las dos importan:
#     "Started XApplication in A seconds (process running for B)". A es lo que tardo el
#     contexto de Spring y B es lo que lleva vivo el PROCESO, que es lo que de verdad espera
#     un contenedor hasta que el servicio esta listo. Quarkus escribe una sola ("started in
#     A s"), que en un binario nativo es practicamente el arranque del proceso.
#   - Memoria: el RSS del proceso tal cual (columna RES de ps), en MB. El RSS es la memoria
#     REAL, y es la unica cifra que existe tambien en una imagen nativa: una nativa no tiene
#     heap de la JVM, asi que las metricas jvm_* no sirven para comparar.
#   - Hilos y descriptores abiertos: se leen de /proc, que es lo unico que hay en este
#     contenedor (su ps no soporta el formato nfiles).
#
# Debe ejecutarse DENTRO del devcontainer, que es donde corren los servicios.

set -euo pipefail

SERVICIOS=("$@")
if [ ${#SERVICIOS[@]} -eq 0 ]; then
  SERVICIOS=(market-data-simulator ingestion-normalizer analytics-streams order-matching-engine
             portfolio-risk alerting-service audit-log)
fi

LOG_DIR=${AGGORA_LOG_DIR:-/tmp}

arranque() { # $1 = servicio
  local log="$LOG_DIR/$1.log"
  [ -f "$log" ] || { echo "?"; return; }
  local spring
  spring="$(grep -h "Started .*Application in" "$log" | tail -1 \
    | sed -nE 's/.* in ([0-9]+\.[0-9]+) seconds \(process running for ([0-9]+\.[0-9]+)\).*/\1 ctx, \2 proceso/p')"
  if [ -n "$spring" ]; then echo "$spring"; return; fi
  # Quarkus: "... started in 0.045s. Listening on: ..."
  sed -nE 's/.*started in ([0-9]+\.[0-9]+)s.*/\1/p' "$log" | tail -1 | grep . || echo "?"
}

pid_de() { # $1 = servicio; sirve para el jar de Spring y para el runner de Quarkus
  pgrep -f "target/$1-.*\.jar" 2>/dev/null | head -1 \
    || pgrep -f "/$1-[0-9.]*-runner" 2>/dev/null | head -1 \
    || true
}

printf '%-24s %-22s %9s %7s %10s\n' "servicio" "arranque (s)" "RSS(MB)" "hilos" "ficheros"
printf '%-24s %-22s %9s %7s %10s\n' "------------------------" "----------------------" "--------" "------" "---------"

for servicio in "${SERVICIOS[@]}"; do
  pid="$(pid_de "$servicio")"
  if [ -z "$pid" ]; then
    printf '%-24s %-22s %9s %7s %10s\n' "$servicio" "-" "(no corre)" "-" "-"
    continue
  fi
  lee() { ps -o "$1=" -p "$pid" 2>/dev/null | tr -d ' '; }
  rss_kb="$(lee rss)"
  printf '%-24s %-22s %9s %7s %10s\n' \
    "$servicio" "$(arranque "$servicio")" "$((rss_kb / 1024))" "$(lee nlwp)" "$(ls /proc/"$pid"/fd 2>/dev/null | wc -l)"
done

echo
echo "El RSS de aqui es el de reposo. Para el de bajo carga, lanza la carga y repite."
echo "Los mismos numeros en vivo y en el tiempo, en Grafana (panel 'Memoria de la JVM en uso'):"
echo "  http://localhost:3000/d/aggora-kafka"
