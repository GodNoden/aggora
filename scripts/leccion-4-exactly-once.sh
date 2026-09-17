#!/usr/bin/env bash
#
# Leccion 4: exactly-once, visto en el log y no en la fe.
#
#   bash scripts/leccion-4-exactly-once.sh [repeticiones]
#
# Envuelve scripts/ExactlyOnceRaceCheck.java, que ya demuestra contra el cluster de verdad la
# semantica en la que se apoya el test ExactlyOnceKafkaIT del motor de matching: publicar la
# ejecucion y confirmar el offset son UNA sola operacion.
#
# Lo que hace el programa, en una frase: publica "confirmada" y "abortada" en un topic de prueba y
# comprueba que la abortada NO existe con read_committed (nadie la ve) y SI existe con
# read_uncommitted (estaba escrita). La secuencia "vieja" (send + abort inmediato) falla casi
# siempre; la "nueva" (send + esperar a verlo + abort) pasa siempre.
#
# Panel del dashboard que hay que mirar: transacciones. Y aqui hay que ser honesto: ese panel NO
# tiene series, porque Prometheus no publica confirmadas frente a abortadas (el exporter de Kafka
# solo da offsets, lag y grupos, y las metricas kafka_producer_txn_* del cliente son TIEMPOS, no
# un recuento). El panel lo dice con "nota" en vez de inventarse un numero. Lo que si se vigila es
# el lag de orders.executions: si el motor aborta, la orden se queda pendiente y se ve en el lag.
#
# Necesita compilar el classpath del motor de matching (Maven) y correr DENTRO del devcontainer.

set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPETICIONES="${1:-3}"
CLASSPATH=/tmp/aggora-eo-classpath.txt
BROKERS="${KAFKA_BROKERS:-kafka-1:9092,kafka-2:9092,kafka-3:9092}"

echo "== Leccion 4: exactly-once con transacciones =="
echo "Brokers: $BROKERS | repeticiones: $REPETICIONES"
echo
echo "El programa crea un topic de prueba por vuelta, publica una transaccion confirmada y otra"
echo "abortada, y las lee con los dos niveles de aislamiento."

if [ ! -f "$CLASSPATH" ] || [ "${RECOMPILAR:-no}" = "si" ]; then
  echo
  echo "== Compilando el classpath del motor de matching =="
  ( cd "$ROOT/services" && mvn -q -pl spring/order-matching-engine dependency:build-classpath \
      -Dmdep.outputFile="$CLASSPATH" ) || {
    echo "No he podido construir el classpath. Compila antes: cd services && mvn -q -DskipTests package"
    exit 1
  }
fi

echo
java -cp "$(cat "$CLASSPATH")" "$ROOT/scripts/ExactlyOnceRaceCheck.java" "$BROKERS" "$REPETICIONES"
CODIGO=$?

echo
echo "== Como se lee esto =="
echo "  VIEJA: la abortada NO llega al log (send + abort inmediato descarta el bufer). Por eso el"
echo "         test antiguo fallaba: no estaba probando la semantica, estaba probando una carrera."
echo "  NUEVA: la abortada SI esta en el log con read_uncommitted y NO se ve con read_committed."
echo "         Eso es exactamente lo que hace el motor: la ejecucion abortada no existe para nadie."
echo
if [ "$CODIGO" = "0" ]; then
  echo "Mira el panel 'transacciones' del dashboard: sale con series vacias y una nota que explica"
  echo "por que (Prometheus no publica confirmadas/abortadas). El dato que si importa en el dia a"
  echo "dia es el lag de orders.executions, en el panel 'lag' y en 'particiones'."
else
  echo "FALLO: la secuencia nueva no es determinista. Eso si seria una regresion."
fi
exit "$CODIGO"
