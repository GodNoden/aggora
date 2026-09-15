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

# El log mas reciente del servicio, sea de la implementacion que sea. Las dos no corren a la
# vez para el mismo servicio (comparten topics y grupo), asi que el mas nuevo es el que vale.
log_de() { # $1 = servicio
  ls -t "$LOG_DIR/$1-spring.log" "$LOG_DIR/$1-quarkus.log" "$LOG_DIR/$1.log" 2>/dev/null | head -1
}

arranque() { # $1 = servicio
  local log
  log="$(log_de "$1")"
  [ -n "$log" ] && [ -f "$log" ] || { echo "?"; return; }
  local spring
  spring="$(grep -h "Started .*Application in" "$log" | tail -1 \
    | sed -nE 's/.* in ([0-9]+\.[0-9]+) seconds \(process running for ([0-9]+\.[0-9]+)\).*/\1 ctx, \2 proceso/p')"
  if [ -n "$spring" ]; then echo "$spring"; return; fi
  # Quarkus: "... started in 0.045s. Listening on: ..."
  sed -nE 's/.*started in ([0-9]+\.[0-9]+)s.*/\1/p' "$log" | tail -1 | grep . || echo "?"
}

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# El proceso tiene que ser un java de verdad: si no, se cuela el shell que lanzo el servicio
# (su linea de comandos tambien lleva el nombre del jar y su directorio de trabajo es el mismo).
es_java() { [ "$(cat "/proc/$1/comm" 2>/dev/null)" = "java" ]; }

pid_de() { # $1 = servicio
  local pid candidato
  # Spring: el nombre del jar lleva el del servicio.
  for candidato in $(pgrep -f "target/$1-spring-.*\.jar" 2>/dev/null); do
    if es_java "$candidato"; then echo "$candidato"; return; fi
  done
  # Quarkus en modo JVM: el runner se llama igual en todos los servicios, asi que se
  # identifica por el directorio de trabajo del proceso (/proc/<pid>/cwd).
  for candidato in $(pgrep -f "[j]ava -jar .*quarkus-app/quarkus-run.jar" 2>/dev/null); do
    if es_java "$candidato" && [ "$(readlink "/proc/$candidato/cwd" 2>/dev/null)" = "$ROOT/services/quarkus/$1" ]; then
      echo "$candidato"; return
    fi
  done
  # Quarkus en binario nativo: el ejecutable se llama <servicio>-quarkus-...-runner.
  for candidato in $(pgrep -f "/$1-quarkus-[0-9.]*-runner" 2>/dev/null); do
    if es_java "$candidato"; then echo "$candidato"; return; fi
  done
  true
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
