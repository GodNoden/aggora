#!/usr/bin/env bash
#
# Chequeo ejecutable del contrato del dashboard, contra los dos stacks EN MARCHA.
#
#   bash scripts/dashboard-smoke.sh
#
# Comprueba, en verde/rojo:
#   1. GET /api/metrics?panel=<todos> en 8089 (Spring) y 8189 (Quarkus): 200 y forma esperada.
#   2. El modo lado a lado: /api/metrics?panel=comparativa&de=lag en los dos.
#   3. El WebSocket entrega un snapshot con v, stack, symbols y las tasas (usa GatewayLiveCheck).
#   4. El CORS responde con el origen permitido en una peticion con Origin: (gateways, analitica
#      y simulador).
#   5. La pagina vieja sigue sirviendose en los dos gateways.
#
# Sale con codigo != 0 si algo falla. Pensado para correr DENTRO del devcontainer, que es donde
# estan los servicios y donde hay java y jq.

set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PUERTOS_GATEWAY=(8089 8189)
PANELES=(pulso lag particiones transacciones descartes salud)
ORIGEN=http://localhost:4200
SEGUNDOS_WS="${SEGUNDOS_WS:-6}"

FALLOS=0
ok() { echo "  [OK] $*"; }
ko() { echo "  [KO] $*"; FALLOS=$((FALLOS + 1)); }

paso() { echo; echo "== $* =="; }

# --- 1 y 2. El catalogo de metricas ---------------------------------------------

paso "Catalogo de metricas (catalogo cerrado, sin PromQL libre)"
for puerto in "${PUERTOS_GATEWAY[@]}"; do
  for panel in "${PANELES[@]}"; do
    cuerpo="$(curl -s -m 8 "localhost:$puerto/api/metrics?panel=$panel")"
    codigo="$(curl -s -m 8 -o /dev/null -w '%{http_code}' "localhost:$puerto/api/metrics?panel=$panel")"
    if [ "$codigo" != "200" ]; then
      ko ":$puerto panel=$panel -> HTTP $codigo"
      continue
    fi
    if jq -e --arg p "$panel" '.panel==$p and (.ts|type=="string") and (.stack|type=="string") and (.series|type=="array")' >/dev/null 2>&1 <<< "$cuerpo"; then
      n="$(jq -r '.series | length' <<< "$cuerpo")"
      ok ":$puerto panel=$panel -> 200, $n series"
    else
      ko ":$puerto panel=$panel -> JSON con forma inesperada: ${cuerpo:0:120}"
    fi
  done
done

paso "Modo lado a lado (comparativa)"
for puerto in "${PUERTOS_GATEWAY[@]}"; do
  cuerpo="$(curl -s -m 8 "localhost:$puerto/api/metrics?panel=comparativa&de=lag")"
  if jq -e '.panel=="comparativa" and .de=="lag" and (.series|type=="array")' >/dev/null 2>&1 <<< "$cuerpo"; then
    ok ":$puerto panel=comparativa&de=lag -> $(jq -r '.series | length' <<< "$cuerpo") series"
  else
    ko ":$puerto panel=comparativa&de=lag -> forma inesperada: ${cuerpo:0:120}"
  fi
done

paso "El catalogo es cerrado: un panel que no existe da 400 con la lista"
cuerpo="$(curl -s -m 8 "localhost:8089/api/metrics?panel=libre")"
codigo="$(curl -s -m 8 -o /dev/null -w '%{http_code}' "localhost:8089/api/metrics?panel=libre")"
if [ "$codigo" = "400" ] && jq -e 'has("detalle")' >/dev/null 2>&1 <<< "$cuerpo"; then
  ok "panel desconocido -> 400 y lista de paneles"
else
  ko "panel desconocido -> HTTP $codigo (esperaba 400 con detalle)"
fi

# --- 3. El WebSocket ------------------------------------------------------------

paso "WebSocket: snapshot con v, stack, symbols y tasas"
if ! command -v java >/dev/null 2>&1; then
  ko "no hay java en este shell; el chequeo del WebSocket necesita el devcontainer"
else
  for puerto in "${PUERTOS_GATEWAY[@]}"; do
    if java "$ROOT/scripts/GatewayLiveCheck.java" localhost "$puerto" "$SEGUNDOS_WS" >/tmp/smoke-ws-$puerto.log 2>&1; then
      ok ":$puerto $(grep -m1 'snapshots validos' /tmp/smoke-ws-$puerto.log | sed 's/^ *//')"
    else
      ko ":$puerto el WebSocket no cumple el contrato:"
      tail -4 /tmp/smoke-ws-$puerto.log | sed 's/^/       /'
    fi
  done
fi

# --- 4. CORS --------------------------------------------------------------------

cors() { # $1 = url, $2 = descripcion
  cabeceras="$(curl -s -m 8 -D- -o /dev/null -H "Origin: $ORIGEN" "$1")"
  if grep -qi "^access-control-allow-origin: *$ORIGEN" <<< "$cabeceras"; then
    ok "$2 -> allow-origin $ORIGEN"
  else
    ko "$2 -> no devuelve allow-origin para $ORIGEN"
  fi
}

paso "CORS de solo lectura (con Origin: $ORIGEN)"
for puerto in "${PUERTOS_GATEWAY[@]}"; do
  cors "localhost:$puerto/api/metrics?panel=salud" "gateway :$puerto /api/metrics"
done
cors "localhost:8085/analytics?symbol=EUR/USD&minutes=3" "analitica :8085 /analytics"
cors "localhost:8080/actuator/health" "simulador :8080 /actuator/health"

# --- 5. La pagina vieja ---------------------------------------------------------

paso "La pagina de verificacion sigue sirviendose"
for puerto in "${PUERTOS_GATEWAY[@]}"; do
  if curl -s -m 8 "localhost:$puerto/" | grep -q "Aggora"; then
    ok ":$puerto / -> pagina HTML"
  else
    ko ":$puerto / no devuelve la pagina"
  fi
done

# --- Resultado ------------------------------------------------------------------

echo
if [ "$FALLOS" = "0" ]; then
  echo "TODO EN VERDE: el contrato del dashboard se cumple en los dos gateways."
  exit 0
fi
echo "FALLOS: $FALLOS. El contrato NO se cumple (mira las lineas [KO])."
exit 1
