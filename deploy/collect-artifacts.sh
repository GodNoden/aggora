#!/usr/bin/env bash
#
# Compila los jars que van a la VM y los deja en deploy/artifacts/ con el nombre exacto
# que espera deploy/user-data.sh: <servicio>-spring-0.1.0-SNAPSHOT.jar.
#
#   bash deploy/collect-artifacts.sh
#
# Compila en el devcontainer si esta levantado (es el que tiene la toolchain fijada) y, si
# no, con el Maven del host. Se puede forzar con AGGORA_BUILD=local o AGGORA_BUILD=devcontainer.
#
# El jar de la Lambda NO se copia aqui: se queda en
# services/lambda/ingestion-normalizer-lambda/target/ y Terraform lo coge de alli. Como el
# build de services/ agrega los tres arboles (Spring, Quarkus y Lambda), sale hecho de paso.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ARTIFACTS_DIR="${REPO_ROOT}/deploy/artifacts"
CONTAINER_WORKSPACE="${AGGORA_CONTAINER_WORKSPACE:-/workspaces/aggora}"

# Los siete que corren en la VM. ingestion-normalizer no esta: es la Lambda.
SERVICES=(
  market-data-simulator
  analytics-streams
  order-matching-engine
  portfolio-risk
  alerting-service
  audit-log
  gateway-ws
)

# El devcontainer se reconoce por la imagen (vsc-aggora-...). El filtro del enunciado
# (docker ps | grep -v aggora) vale cuando no hay otros contenedores, pero en una maquina
# con mas proyectos coge el primero que pase.
find_devcontainer() {
  docker ps --format '{{.Names}}\t{{.Image}}' 2>/dev/null |
    awk -F'\t' '$2 ~ /aggora/ { print $1; exit }'
}

build_with() {
  local where="$1"
  echo "==> Compilando services/ (${where})"
  if [ "${where}" = "devcontainer" ]; then
    local container="$2"
    docker exec -u vscode "${container}" bash -lc \
      "cd '${CONTAINER_WORKSPACE}/services' && mvn -B -ntp -DskipTests package"
  else
    (cd "${REPO_ROOT}/services" && mvn -B -ntp -DskipTests package)
  fi
}

case "${AGGORA_BUILD:-auto}" in
  local)
    command -v mvn >/dev/null 2>&1 || {
      echo "ERROR: AGGORA_BUILD=local pero no hay mvn en el PATH." >&2
      exit 1
    }
    build_with local
    ;;
  devcontainer)
    container="$(find_devcontainer)"
    [ -n "${container}" ] || {
      echo "ERROR: AGGORA_BUILD=devcontainer pero no hay ningun contenedor de aggora levantado." >&2
      exit 1
    }
    build_with devcontainer "${container}"
    ;;
  auto)
    container="$(find_devcontainer)"
    if [ -n "${container}" ]; then
      echo "==> Devcontainer encontrado: ${container}"
      build_with devcontainer "${container}"
    elif command -v mvn >/dev/null 2>&1; then
      echo "==> Sin devcontainer; se usa el Maven del host"
      build_with local
    else
      echo "ERROR: no hay devcontainer de aggora levantado ni mvn en el PATH." >&2
      echo "       Arranca el devcontainer o instala Maven + Java 21." >&2
      exit 1
    fi
    ;;
  *)
    echo "ERROR: AGGORA_BUILD tiene que ser auto, local o devcontainer." >&2
    exit 1
    ;;
esac

# --- Copia de los jars --------------------------------------------------------------
JAR_VERSION="0.1.0-SNAPSHOT"
mkdir -p "${ARTIFACTS_DIR}"

missing=0
for service in "${SERVICES[@]}"; do
  source_jar="${REPO_ROOT}/services/spring/${service}/target/${service}-spring-${JAR_VERSION}.jar"
  if [ ! -f "${source_jar}" ]; then
    echo "FALTA el jar: ${source_jar}" >&2
    missing=1
    continue
  fi
  cp "${source_jar}" "${ARTIFACTS_DIR}/${service}-spring-${JAR_VERSION}.jar"
  echo "  ${service}-spring-${JAR_VERSION}.jar"
done

if [ "${missing}" -ne 0 ]; then
  echo >&2
  echo "ERROR: faltan jars. Compila con:" >&2
  echo "  cd services && mvn -B -ntp -DskipTests package" >&2
  exit 1
fi

# El jar de la Lambda es lo unico que Terraform necesita y no esta en deploy/artifacts/.
# Si no existe, el plan fallara al leerlo; mejor decirlo aqui.
LAMBDA_JAR="${REPO_ROOT}/services/lambda/ingestion-normalizer-lambda/target/ingestion-normalizer-lambda-${JAR_VERSION}.jar"
if [ ! -f "${LAMBDA_JAR}" ]; then
  echo
  echo "AVISO: no esta el jar de la Lambda (${LAMBDA_JAR})."
  echo "       Terraform lo necesita para aws_lambda_function: compila ese modulo antes del apply."
fi

echo
echo "==> ${#SERVICES[@]} jars en deploy/artifacts/"
ls -1 "${ARTIFACTS_DIR}"/*.jar
