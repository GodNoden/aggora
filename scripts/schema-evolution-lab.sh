#!/usr/bin/env bash
#
# Laboratorio de evolucion de esquemas (Fase 7).
#
#   bash scripts/schema-evolution-lab.sh
#
# Ejercita contra el Schema Registry de VERDAD los tres casos que importan cuando cambias
# un contrato Avro que ya esta en produccion:
#
#   1. un cambio compatible (un campo nuevo con valor por defecto) -> el registro lo acepta
#   2. un cambio que rompe (ese mismo campo sin valor por defecto, o un campo renombrado)
#      -> el registro lo RECHAZA con un 409 y dice exactamente por que
#   3. los arreglos que si funcionan: un alias, hacer el campo opcional, o un topic nuevo
#
# Ademas desmonta la trampa mas cara de este tema: BORRAR UN CAMPO pasa el filtro BACKWARD
# y sin embargo rompe a los consumidores que ya estaban desplegados.
#
# Todo lo que prueba en seco se prueba ANTES de registrar nada, para que los veredictos
# sean contra el contrato original y no contra el que acaba de registrar el propio script.
# Los esquemas que se registran de verdad se borran al final, y el script comprueba que el
# subject queda como estaba.
#
# Lo que NO hace es producir mensajes con el esquema nuevo: el topic guarda el ID del
# esquema de cada mensaje, asi que un mensaje escrito con un esquema que luego borras queda
# ilegible para siempre. Eso se explica en docs/schema-evolution-lab.md.
#
# El registro se busca solo: dentro del devcontainer es schema-registry:8081 y desde
# WSL/Windows, localhost:8081. Se puede forzar con SCHEMA_REGISTRY_URL.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SUBJECT="${SUBJECT:-market.ticks.canonical-value}"
V1="$ROOT/services/schemas/canonical.avsc"
PROPUESTA="$ROOT/services/ingestion-normalizer/src/test/resources/canonical-v2.avsc"
ANALITICA="${ANALITICA:-http://localhost:8085}"

TEMPORAL="$(mktemp -d)"
VERSION_NUEVA=""
NUEVO_SUBJECT=""

# Borra de verdad. El registro obliga a pasar por la papelera (borrado suave) antes del
# borrado definitivo, asi que van las dos llamadas.
borrar_subject() {
  curl -s -X DELETE "$REGISTRO/subjects/$1" >/dev/null || true
  curl -s -X DELETE "$REGISTRO/subjects/$1?permanent=true" >/dev/null || true
}
borrar_version() {
  curl -s -X DELETE "$REGISTRO/subjects/$1/versions/$2" >/dev/null || true
  curl -s -X DELETE "$REGISTRO/subjects/$1/versions/$2?permanent=true" >/dev/null || true
}

# Si el script se corta a medias, no se deja esquemas registrados por el camino.
limpiar() {
  local codigo=$?
  rm -rf "$TEMPORAL"
  if [ -n "$VERSION_NUEVA" ]; then
    borrar_version "$SUBJECT" "$VERSION_NUEVA"
  fi
  if [ -n "$NUEVO_SUBJECT" ]; then
    borrar_subject "$NUEVO_SUBJECT"
  fi
  exit "$codigo"
}
trap limpiar EXIT

# --- utilidades ------------------------------------------------------------

buscar_registro() {
  if [ -n "${SCHEMA_REGISTRY_URL:-}" ]; then
    echo "$SCHEMA_REGISTRY_URL"
    return
  fi
  for candidato in http://localhost:8081 http://schema-registry:8081; do
    if curl -sf -m 3 "$candidato/subjects" >/dev/null 2>&1; then
      echo "$candidato"
      return
    fi
  done
  echo "No encuentro el Schema Registry. Arranca la infraestructura con:" >&2
  echo "  docker compose -f infra/docker-compose.yml up -d" >&2
  exit 1
}

cuerpo() { # fichero .avsc -> {"schema": "<esquema escapado>"}
  jq -Rs '{schema: .}' "$1"
}

# Pregunta al registro si ese esquema se puede anadir al subject. No registra nada.
comprobar() { # $1 = fichero .avsc
  cuerpo "$1" | curl -s -X POST "$REGISTRO/compatibility/subjects/$SUBJECT/versions/latest?verbose=true" \
    -H 'Content-Type: application/vnd.schemaregistry.v1+json' --data @-
}

# Lo mismo, pero con el nivel de compatibilidad que se le diga (BACKWARD, FORWARD...).
comprobar_con_nivel() { # $1 = nivel, $2 = fichero .avsc
  curl -s -X PUT "$REGISTRO/config/$SUBJECT" -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
    --data "{\"compatibility\":\"$1\"}" >/dev/null
  comprobar "$2"
  # Se quita el nivel del subject para que vuelva a heredar el global.
  curl -s -X DELETE "$REGISTRO/config/$SUBJECT" >/dev/null
}

veredicto() { jq -r 'if .is_compatible then "COMPATIBLE" else "RECHAZADO" end'; }
motivo() { jq -r '.messages[]? | select(startswith("{errorType"))'; }

# Registra de verdad. Devuelve el codigo HTTP y deja la respuesta en $TEMPORAL/respuesta.
registrar() { # $1 = subject, $2 = fichero .avsc
  cuerpo "$2" | curl -s -o "$TEMPORAL/respuesta" -w '%{http_code}' -X POST "$REGISTRO/subjects/$1/versions" \
    -H 'Content-Type: application/vnd.schemaregistry.v1+json' --data @-
}

versiones() { curl -s "$REGISTRO/subjects/$SUBJECT/versions"; }
analitica_viva() { curl -sf -m 5 "$ANALITICA/analytics?symbol=EUR/USD&minutes=3" >/dev/null && echo si || echo NO; }

# El error del registro trae el esquema viejo entero detras; para leerlo basta el principio.
rechazo() { jq -c '{error_code, message: (.message | split(", details:")[0])}' "$1"; }

paso() { echo; echo "== $* =="; }
nota() { echo "   $*"; }

REGISTRO="$(buscar_registro)"

# --- preparar los cambios que se van a probar ------------------------------

# Se sacan del contrato real para que no se queden desfasados si canonical.avsc cambia.
jq '.fields += [{"name":"venueMic","type":"string","doc":"campo nuevo sin valor por defecto"}]' \
  "$V1" > "$TEMPORAL/sin-default.avsc"
jq '(.fields[] | select(.name=="symbol")) |= (.name="ticker")' \
  "$V1" > "$TEMPORAL/renombrado.avsc"
jq '(.fields[] | select(.name=="symbol")) |= (.name="ticker" | .aliases=["symbol"])' \
  "$V1" > "$TEMPORAL/renombrado-con-alias.avsc"
jq 'del(.fields[] | select(.name=="originOffset"))' \
  "$V1" > "$TEMPORAL/borrado.avsc"
jq '(.fields[] | select(.name=="originOffset")) |= (.type=["null","long"] | .default=null)' \
  "$V1" > "$TEMPORAL/opcional.avsc"

# --- 0. estado de partida --------------------------------------------------

paso "0. Estado de partida"
nota "registro: $REGISTRO"
nota "subject:  $SUBJECT"
nota "nivel de compatibilidad global: $(curl -s "$REGISTRO/config" | jq -r .compatibilityLevel)"
BASELINE="$(versiones)"
nota "versiones: $BASELINE"
if [ "$(echo "$BASELINE" | jq 'length')" != "1" ]; then
  echo "   Este laboratorio espera un subject con UNA sola version (el contrato original);"
  echo "   ahora tiene $BASELINE. Borra las de mas o usa otro subject con SUBJECT=..."
  exit 1
fi
nota "la analitica, compilada con ese contrato, responde: $(analitica_viva)"

# --- 1. lo que el registro acepta y lo que no (sin registrar nada) ---------

paso "1. Un cambio compatible: anadir venueMic CON valor por defecto"
nota "esquema propuesto: ${PROPUESTA#"$ROOT"/}"
nota "el registro dice: $(comprobar "$PROPUESTA" | veredicto)"

paso "2. Un cambio que NO es compatible: el mismo campo SIN valor por defecto"
nota "el registro dice: $(comprobar "$TEMPORAL/sin-default.avsc" | veredicto)"
nota "motivo: $(comprobar "$TEMPORAL/sin-default.avsc" | motivo)"

paso "2b. Otro que tampoco: renombrar symbol a ticker"
nota "el registro dice: $(comprobar "$TEMPORAL/renombrado.avsc" | veredicto)"
nota "el registro se niega a guardarlo si se intenta de verdad:"
CODIGO="$(registrar "$SUBJECT" "$TEMPORAL/renombrado.avsc")"
nota "HTTP $CODIGO, respuesta: $(rechazo "$TEMPORAL/respuesta")"
nota "versiones: $(versiones)  <- no ha crecido"

# --- 3. la trampa del borrado ---------------------------------------------

paso "3. La trampa: BORRAR un campo pasa el filtro BACKWARD y rompe a los antiguos"
nota "con el nivel BACKWARD (el de este proyecto): $(comprobar "$TEMPORAL/borrado.avsc" | veredicto)"
nota "  BACKWARD pregunta: ¿el esquema nuevo puede leer los datos viejos?"
nota "  Un campo que el lector no tiene se ignora, asi que responder que si es correcto..."
nota "con el nivel FORWARD: $(comprobar_con_nivel FORWARD "$TEMPORAL/borrado.avsc" | veredicto)"
nota "  ...pero FORWARD pregunta lo otro: ¿un lector viejo puede leer los datos nuevos?"
nota "  Ahi el campo que falta ya no se puede rellenar. El nivel se elige segun quien se"
nota "  despliegue primero, y por eso el mismo cambio da dos respuestas distintas."

# --- 4. los arreglos ------------------------------------------------------

paso "4. Los arreglos que si funcionan"
nota "a) renombrar + alias:                                    $(comprobar "$TEMPORAL/renombrado-con-alias.avsc" | veredicto)"
nota "b) campo opcional (union con null y default null):       $(comprobar "$TEMPORAL/opcional.avsc" | veredicto)"
nota "   (b) es el primer paso obligatorio para borrar un campo algun dia: primero se hace"
nota "   opcional, se espera a que ya nadie lo escriba y entonces se quita."

# --- 5. el mismo cambio, juzgado en las dos direcciones -------------------

paso "5. Resumen: el mismo cambio juzgado en las dos direcciones"
nota "BACKWARD = el esquema nuevo lee los datos viejos.  FORWARD = el viejo lee los nuevos."
nota "Todo esto esta medido contra la version 1, que sigue siendo la unica registrada."
printf '   %-42s %-11s %s\n' "cambio" "BACKWARD" "FORWARD"
for entrada in \
  "campo nuevo con valor por defecto:$PROPUESTA" \
  "campo nuevo sin valor por defecto:$TEMPORAL/sin-default.avsc" \
  "renombrar un campo sin alias:$TEMPORAL/renombrado.avsc" \
  "renombrar un campo con alias:$TEMPORAL/renombrado-con-alias.avsc" \
  "borrar un campo:$TEMPORAL/borrado.avsc" \
  "campo opcional (union con null):$TEMPORAL/opcional.avsc"; do
  nombre="${entrada%%:*}"
  fichero="${entrada#*:}"
  printf '   %-42s %-11s %s\n' "$nombre" \
    "$(comprobar "$fichero" | veredicto)" \
    "$(comprobar_con_nivel FORWARD "$fichero" | veredicto)"
done
nota "lo que dice la tabla, en una frase: lo unico que funciona en las dos direcciones es"
nota "ANADIR con valor por defecto. Todo lo demas obliga a elegir quien se despliega antes."
nota "nivel de compatibilidad global: $(curl -s "$REGISTRO/config" | jq -r .compatibilityLevel)"

# --- 6. ahora si: registrar de verdad -------------------------------------

paso "6. El cambio compatible se registra (version nueva del contrato)"
nota "propuesta de valor por defecto: $(jq -c '.fields[-1]' "$PROPUESTA")"
CODIGO="$(registrar "$SUBJECT" "$PROPUESTA")"
nota "HTTP $CODIGO, respuesta: $(jq -c '{id, version}' "$TEMPORAL/respuesta")"
VERSION_NUEVA="$(jq -r .version "$TEMPORAL/respuesta")"
nota "versiones ahora: $(versiones)"
nota "la analitica, que no sabe nada del campo nuevo y no se ha recompilado, sigue: $(analitica_viva)"
nota "el campo que sobra se ignora al leer; el que falta lo pone su valor por defecto."
nota "Eso es lo que hay detras de la palabra 'compatible' (ver SchemaEvolutionTest)."

paso "6b. La otra salida: un topic nuevo, que es un subject nuevo"
NUEVO_SUBJECT="${SUBJECT%-value}.v2-value"
nota "un cambio que rompe se acepta sin discutir en un subject nuevo, porque no tiene"
nota "historial con el que ser incompatible. Es lo que se hace cuando hay que cambiar el"
nota "contrato de verdad: topic nuevo (market.ticks.canonical.v2), consumidores nuevos"
nota "leyendo de los dos sitios durante la migracion y el viejo se apaga cuando nadie lo lee."
CODIGO="$(registrar "$NUEVO_SUBJECT" "$TEMPORAL/renombrado.avsc")"
nota "HTTP $CODIGO, respuesta: $(jq -c '{id, version}' "$TEMPORAL/respuesta")"
nota "el precio: dos contratos vivos a la vez y consumidores que hay que migrar a mano."

# --- 7. limpieza ----------------------------------------------------------

paso "7. Limpieza: el registro queda como estaba"
borrar_subject "$NUEVO_SUBJECT"
nota "borrado el subject temporal $NUEVO_SUBJECT"
NUEVO_SUBJECT=""
borrar_version "$SUBJECT" "$VERSION_NUEVA"
nota "borrada la version $VERSION_NUEVA de $SUBJECT"
VERSION_NUEVA=""
nota "versiones: $(versiones)  (baseline: $BASELINE)"
if [ "$(versiones)" != "$BASELINE" ]; then
  echo "   AVISO: el subject no ha quedado como estaba."
  exit 1
fi
nota "nivel de compatibilidad global: $(curl -s "$REGISTRO/config" | jq -r .compatibilityLevel)"
nota "la analitica sigue respondiendo: $(analitica_viva)"
echo
echo "Laboratorio terminado. La explicacion larga, en docs/schema-evolution-lab.md."
