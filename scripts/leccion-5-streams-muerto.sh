#!/usr/bin/env bash
#
# Leccion 5: un motor de Streams muerto con el proceso vivo (el fallo silencioso).
#
#   bash scripts/leccion-5-streams-muerto.sh
#
# Rompe a proposito el checkpoint del GlobalKTable de la analitica (se le pone un offset que ya no
# existe en market.fx.reference, que es un topic COMPACTADO) y reinicia el servicio. El motor se
# queda en ERROR... y el proceso sigue vivo, escuchando y devolviendo 503. Es el fallo que se
# encontro operando: desde fuera parece sano.
#
# Lo que se ensena, que es lo importante: quien levanta un proceso caido es el SUPERVISOR, no el
# propio servicio; y quien avisa de que esta roto es la SONDA. Aqui la sonda es /actuator/health a
# DOWN y la metrica aggora_kafka_streams_running a 0, que es lo que pinta el panel 'salud' del
# dashboard.
#
# Paneles que hay que mirar: salud (spring/analytics-streams a 0) y lag (el grupo analytics-streams
# se queda quieto). /analytics deja de dar 200 (503 cuando el motor ya se declaro en ERROR, 500
# mientras el fallo llega por otro camino): el servicio no ha muerto, es que su motor no atiende.
#
# El trap deja el checkpoint como estaba y reinicia el servicio: el sistema queda como estaba.
#
# Dentro del devcontainer, que es donde corren los servicios.

set -uo pipefail

SERVICIO=analytics-streams
DIR=/workspaces/aggora/services/spring/$SERVICIO
JAR=target/$SERVICIO-spring-0.1.0-SNAPSHOT.jar
LOG=/tmp/$SERVICIO.log
PUERTO=8085
GATEWAY=http://localhost:8089
CHECKPOINT=/tmp/aggora-streams-state/$SERVICIO/global/.checkpoint
BACKUP=/tmp/aggora-checkpoint-global.bak
OFFSET_IMPOSIBLE=999999999

FICHERO_ROTO="no"

es_java() { [ "$(cat "/proc/$1/comm" 2>/dev/null)" = "java" ]; }

pid_analitica() {
  local p
  for p in $(pgrep -f "$SERVICIO-spring-0.1.0-SNAPSHOT.jar" 2>/dev/null); do
    es_java "$p" && { echo "$p"; return; }
  done
}

arrancar() {
  ( cd "$DIR" && nohup java -Xms128m -Xmx384m -jar "$JAR" >> "$LOG" 2>&1 & )
  for _ in $(seq 1 60); do
    [ -n "$(pid_analitica)" ] && return 0
    sleep 1
  done
  return 1
}

parar() {
  local pid
  pid="$(pid_analitica)"
  [ -z "$pid" ] && return 0
  kill "$pid" 2>/dev/null
  for _ in $(seq 1 20); do
    ps -p "$pid" >/dev/null 2>&1 || return 0
    sleep 1
  done
  kill -9 "$pid" 2>/dev/null
  sleep 2
}

sonda() { curl -s -o /dev/null -m 5 -w '%{http_code}' "localhost:$PUERTO/actuator/health"; }
analitica() { curl -s -o /dev/null -m 5 -w '%{http_code}' "localhost:$PUERTO/analytics?symbol=EUR/USD&minutes=3"; }

# La metrica del motor, leida por el panel 'salud' del gateway (el mismo camino que el dashboard).
streams_running() {
  curl -s -m 5 "$GATEWAY/api/metrics?panel=salud" 2>/dev/null \
    | jq -r '.series[]? | select(.label=="spring/analytics-streams") | .points[0][1]' 2>/dev/null | head -1
}

restaurar() {
  local codigo=$?
  if [ "$FICHERO_ROTO" = "si" ]; then
    echo
    echo "== Restaurando el checkpoint y el servicio =="
    parar
    if [ -d "$(dirname "$CHECKPOINT")" ]; then
      cp "$BACKUP" "$CHECKPOINT" && echo "  checkpoint restaurado"
    else
      # Kafka Streams borro el estado global al no poder restaurarlo: lo reconstruye solo desde
      # market.fx.reference. No hay nada que restaurar a mano.
      echo "  Kafka Streams limpio el estado global: se reconstruye solo desde market.fx.reference"
    fi
    arrancar >/dev/null 2>&1
    for _ in $(seq 1 60); do
      [ "$(sonda)" = "200" ] && break
      sleep 2
    done
    # El estado se reconstruye desde los topics de changelog: puede tardar un par de minutos. Se
    # espera a que /analytics vuelva a 200 para dejar el sistema como estaba de verdad.
    for _ in $(seq 1 90); do
      [ "$(analitica)" = "200" ] && break
      sleep 2
    done
    echo "  Sonda: $(sonda) | streams_running: $(streams_running) | /analytics: $(analitica)"
    rm -f "$BACKUP"
  fi
  exit "$codigo"
}
trap restaurar EXIT

if [ ! -f "$DIR/$JAR" ]; then
  echo "Falta el jar $DIR/$JAR. Compila antes: cd services && mvn -q -DskipTests package"
  exit 1
fi
if [ -z "$(pid_analitica)" ]; then
  echo "La analitica no esta corriendo. Arrancala con: bash scripts/start-services.sh"
  exit 1
fi
if [ ! -f "$CHECKPOINT" ]; then
  echo "No encuentro el checkpoint $CHECKPOINT."
  echo "Ese GlobalKTable se crea al arrancar la analitica; espera unos segundos y repite."
  exit 1
fi

echo "== Leccion 5: el motor muere y el proceso sigue vivo =="
echo "Servicio: $SERVICIO (pid $(pid_analitica))"
echo "Checkpoint que se va a romper: $CHECKPOINT"
echo "Antes:"
echo "  $(cat "$CHECKPOINT" | tail -1)"
echo "  sonda: $(sonda) | /analytics: $(analitica) | aggora_kafka_streams_running: $(streams_running)"

echo
echo "== 1. Parando la analitica y rompiendo el checkpoint =="
parar
cp "$CHECKPOINT" "$BACKUP"
# Un offset por delante del final del log de market.fx.reference: al reconstruir el estado global
# Kafka Streams pide un offset que ya no existe. El topic es compactado, asi que su principio se
# mueve solo y un checkpoint viejo puede quedar fuera de rango. Es exactamente el incidente real.
printf '0\n1\nmarket.fx.reference 0 %s\n' "$OFFSET_IMPOSIBLE" > "$CHECKPOINT"
FICHERO_ROTO="si"
echo "  checkpoint roto: $(tail -1 "$CHECKPOINT")"

echo
echo "== 2. Arrancando otra vez =="
arrancar >/dev/null 2>&1
echo "  proceso arrancado (pid $(pid_analitica)): el proceso VIVE"

echo
echo "== 3. Muestreando la sonda y la metrica =="
ESTADO_MAL="no"
for i in $(seq 1 20); do
  SONDA="$(sonda)"
  METRICA="$(streams_running)"
  printf '  t+%02ds  sonda=%s  streams_running=%s  /analytics=%s\n' "$((i * 2))" "$SONDA" "$METRICA" "$(analitica)"
  if [ "$SONDA" = "503" ]; then ESTADO_MAL="si"; fi
  [ "$ESTADO_MAL" = "si" ] && [ "$i" -ge 6 ] && break
  sleep 2
done

echo
echo "== 4. Veredicto =="
if [ "$ESTADO_MAL" = "si" ] && [ -n "$(pid_analitica)" ]; then
  echo "  OK: el proceso sigue vivo (pid $(pid_analitica)), la sonda esta abajo y /analytics no da 200."
  echo "  La causa esta en el log:"
  grep -i "OffsetOutOfRange" "$LOG" | tail -1 | cut -c1-160 | sed 's/^/    /'
  echo
  echo "  Ojo con el codigo de /analytics: 503 cuando el motor ya se declaro en ERROR (el servicio"
  echo "  contesta 'no puedo atender') y 500 mientras el fallo llega por otro camino. Ninguno de los"
  echo "  dos es un 200, que es lo que importa; la sonda es la que decide."
  echo
  echo "Mira el panel 'salud': spring/analytics-streams pasa a 0 mientras el target sigue 'up'. Por"
  echo "eso la sonda y la metrica son lo que se vigila, y no el 'esta el proceso vivo'."
  echo
  echo "Al salir, este script RESTAURA el checkpoint y reinicia el servicio. La leccion no es"
  echo "'rompe cosas': es que un servicio no deberia decidir suicidarse; lo levanta el supervisor."
else
  echo "  No he pillado la sonda abajo. Mira el log: $LOG"
fi
