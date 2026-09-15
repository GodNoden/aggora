#!/usr/bin/env bash
# Para la implementacion de Quarkus del stack paralelo (la de Spring se para con stop-services.sh).
set -euo pipefail
SUFIJO=${AGGORA_SUFIJO:-q}
for servicio in ingestion-normalizer analytics-streams order-matching-engine portfolio-risk alerting-service audit-log; do
  if pgrep -f "[j]ava .*services/quarkus/$servicio/target/quarkus-app/quarkus-run.jar" >/dev/null; then
    pkill -TERM -f "[j]ava .*services/quarkus/$servicio/target/quarkus-app/quarkus-run.jar"
    echo "  $servicio-$SUFIJO: parado"
  else
    echo "  $servicio-$SUFIJO: no estaba en marcha"
  fi
done
