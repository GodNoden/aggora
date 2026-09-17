#!/usr/bin/env bash
#
# preflight de la Fase 10: comprueba TODO lo comprobable sin gastar un euro.
#
#   bash deploy/preflight.sh
#
# Verde = listo. Rojo = falta algo, con el motivo y el comando que lo arregla.
# Sale con codigo != 0 si hay cualquier rojo.
#
# Lo unico que necesita AWS de verdad es `aws sts get-caller-identity`: si no hay
# credenciales, esa linea sale en rojo y el resto sigue comprobandose igual. Ese rojo no
# es un fallo del repo: es que aun no hay cuenta (ver deploy/SPRINT-AWS.md, paso 1).
#
# No lleva `set -e` a proposito: este script existe para acumular fallos y contarlos
# todos de una pasada, no para morir en el primero.
#
# Comentarios en espanol sin acentos, como el resto del shell del proyecto.

set -uo pipefail

# --- Presentacion -------------------------------------------------------------------

GREEN=$'\033[32m'
RED=$'\033[31m'
BOLD=$'\033[1m'
RESET=$'\033[0m'

FAILS=0

ok()   { printf '%s  OK  %s %s\n' "${GREEN}" "$1" "${RESET}"; }
fail() { printf '%sFALLO%s %s\n' "${RED}" "${RESET}" "$1"; FAILS=$((FAILS + 1)); }
note() { printf '      %s\n' "$1"; }
section() { printf '\n%s%s%s\n' "${BOLD}" "$1" "${RESET}"; }

# --- Rutas --------------------------------------------------------------------------

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TF_DIR="${REPO_ROOT}/deploy/terraform"
ARTIFACTS_DIR="${REPO_ROOT}/deploy/artifacts"

# El nombre exacto que esperan deploy/user-data.sh y deploy/collect-artifacts.sh.
JAR_VERSION="0.1.0-SNAPSHOT"
SERVICES=(
  market-data-simulator
  analytics-streams
  order-matching-engine
  portfolio-risk
  alerting-service
  audit-log
  gateway-ws
)

# El jar gordo de la Lambda, tal cual lo declara el default de `lambda_jar_path` en
# deploy/terraform/variables.tf (relativo a deploy/terraform/, aqui lo montamos a mano).
LAMBDA_JAR="${REPO_ROOT}/services/lambda/ingestion-normalizer-lambda/target/ingestion-normalizer-lambda-${JAR_VERSION}-shaded.jar"

TMP_OUT="$(mktemp)"
trap 'rm -f "${TMP_OUT}"' EXIT

section "preflight Fase 10 - $(date '+%Y-%m-%d %H:%M:%S')"
echo "repo: ${REPO_ROOT}"

# --- 1. Terraform (binario del host o el contenedor oficial) ------------------------

# TF_MODE vacio = no hay forma de ejecutar terraform y `terraform validate` se salta.
TF_MODE=""

section "1. Herramientas"

if command -v terraform >/dev/null 2>&1; then
  TF_MODE="host"
  ok "terraform: $(terraform version 2>/dev/null | head -1)"
elif command -v docker >/dev/null 2>&1 && docker image inspect hashicorp/terraform:1.9 >/dev/null 2>&1; then
  TF_MODE="docker"
  ok "terraform: no esta en el PATH; se usa el contenedor hashicorp/terraform:1.9 (ya descargado)"
else
  fail "no hay terraform ni el contenedor hashicorp/terraform:1.9"
  note "instala Terraform >= 1.6, o: docker pull hashicorp/terraform:1.9"
fi

if command -v aws >/dev/null 2>&1; then
  ok "aws: $(aws --version 2>&1 | head -1)"
else
  fail "no hay aws CLI en el PATH"
  note "instala el AWS CLI v2 antes de seguir con deploy/SPRINT-AWS.md"
fi

# El plugin de Session Manager no es del CLI: es un binario aparte y sin el no se puede
# entrar en la VM (no hay SSH, el security group no tiene ingress). Paso 6 de la guia.
if command -v session-manager-plugin >/dev/null 2>&1; then
  ok "session-manager-plugin: $(session-manager-plugin --version 2>&1 | head -1)"
else
  fail "falta el plugin de AWS Session Manager (sin el, 'aws ssm start-session' no funciona)"
  note "instalalo antes del paso 6 de deploy/SPRINT-AWS.md (instrucciones en la guia)"
fi

# --- 2. Credenciales de AWS ---------------------------------------------------------

HAVE_AWS=0

section "2. Credenciales de AWS (lo unico que no se puede comprobar sin cuenta)"

if command -v aws >/dev/null 2>&1; then
  if CALLER="$(aws sts get-caller-identity --query Account --output text 2>/dev/null)" && [ -n "${CALLER}" ]; then
    ok "aws sts get-caller-identity responde: cuenta ${CALLER}"
    HAVE_AWS=1
  else
    fail "aws sts get-caller-identity falla: no hay credenciales configuradas"
    note "arreglo: aws configure   (region eu-west-1, ver deploy/SPRINT-AWS.md paso 1)"
    note "sin credenciales no se pueden comprobar los pasos 2, 5, 6, 7 y 8 de la guia"
  fi
fi

# --- 3. Artefactos ------------------------------------------------------------------

section "3. Artefactos (deploy/collect-artifacts.sh)"

MISSING_JARS=()
for service in "${SERVICES[@]}"; do
  jar="${ARTIFACTS_DIR}/${service}-spring-${JAR_VERSION}.jar"
  [ -f "${jar}" ] || MISSING_JARS+=("${service}-spring-${JAR_VERSION}.jar")
done

if [ "${#MISSING_JARS[@]}" -eq 0 ]; then
  ok "los 7 jars Spring estan en deploy/artifacts/"
else
  fail "faltan ${#MISSING_JARS[@]} de los 7 jars Spring en deploy/artifacts/"
  for jar in "${MISSING_JARS[@]}"; do
    note "falta: deploy/artifacts/${jar}"
  done
  note "compilalos: bash deploy/collect-artifacts.sh"
fi

if [ -f "${LAMBDA_JAR}" ]; then
  ok "jar gordo de la Lambda: $(basename "${LAMBDA_JAR}") ($(du -h "${LAMBDA_JAR}" | cut -f1))"
else
  fail "no esta el jar GORDO de la Lambda (el que sube Terraform a aws_lambda_function)"
  note "esperado: ${LAMBDA_JAR}"
  note "el jar sin '-shaded' es el fino y Lambda fallaria con ClassNotFoundException en la"
  note "primera invocacion: compila el modulo con 'mvn -pl services/lambda/ingestion-normalizer-lambda package'"
fi

# --- 4. TF_VAR obligatorias ---------------------------------------------------------
# "Obligatoria" = la variable de variables.tf que no tiene `default`. Si manana se anade
# una sin valor por defecto, esta comprobacion la caza sola.

section "4. Variables TF_VAR obligatorias"

REQUIRED_VARS=()
while IFS= read -r var; do
  [ -n "${var}" ] && REQUIRED_VARS+=("${var}")
done < <(awk '
  /^variable "/ { name=$2; gsub(/"/, "", name); has_default=0 }
  /^[[:space:]]*default[[:space:]]*=/ { has_default=1 }
  /^}/ { if (name != "" && has_default == 0) print name; name=""; has_default=0 }
' "${TF_DIR}/variables.tf")

if [ "${#REQUIRED_VARS[@]}" -eq 0 ]; then
  fail "no se ha podido leer ningun variable sin default en deploy/terraform/variables.tf"
  note "revisa que el fichero sigue existiendo y con el mismo formato"
else
  MISSING_VARS=()
  for var in "${REQUIRED_VARS[@]}"; do
    # Expansion indirecta: lee el valor de la variable cuyo nombre esta en $var.
    [ -n "${!var:-}" ] || MISSING_VARS+=("${var}")
  done

  if [ "${#MISSING_VARS[@]}" -eq 0 ]; then
    ok "las ${#REQUIRED_VARS[@]} TF_VAR obligatorias estan puestas (${REQUIRED_VARS[*]})"
  else
    fail "faltan ${#MISSING_VARS[@]} de las ${#REQUIRED_VARS[@]} TF_VAR obligatorias"
    for var in "${MISSING_VARS[@]}"; do
      note "export ${var}=...   (secreto: por entorno, nunca en terraform.tfvars)"
    done
    note "el bloque exacto de exports esta en deploy/SPRINT-AWS.md, paso 4"
  fi
fi

# --- 5. terraform validate ----------------------------------------------------------

section "5. terraform validate"

if [ -z "${TF_MODE}" ]; then
  fail "terraform validate: saltado, no hay terraform ni contenedor"
else
  tf() {
    if [ "${TF_MODE}" = "host" ]; then
      (cd "${TF_DIR}" && terraform "$@")
    else
      # --user + HOME=/tmp: el contenedor no deja ficheros root en el repo.
      # El repo entero montado en /w: el modulo lee ../user-data.sh, ../artifacts/*.jar
      # y ../../infra/**, asi que montar solo deploy/terraform/ falla con un falso error.
      docker run --rm --user "$(id -u):$(id -g)" -e HOME=/tmp \
        -v "${REPO_ROOT}:/w" -w /w/deploy/terraform \
        hashicorp/terraform:1.9 "$@"
    fi
  }

  if tf validate >"${TMP_OUT}" 2>&1; then
    ok "terraform validate: $(sed -n 's/\x1b\[[0-9;]*m//gp' "${TMP_OUT}" | tr -d '\n' | sed 's/  */ /g')"
  else
    fail "terraform validate ha fallado:"
    sed 's/^/      /' "${TMP_OUT}"
    note "si dice que faltan providers: cd deploy/terraform && terraform init -backend=false"
  fi
fi

# --- Resumen ------------------------------------------------------------------------

echo
if [ "${FAILS}" -eq 0 ]; then
  printf '%s  OK  preflight completo: todo lo comprobable esta listo.%s\n' "${GREEN}" "${RESET}"
  [ "${HAVE_AWS}" -eq 1 ] || note "sin credenciales de AWS: revisa el punto 2 antes de lanzar el sprint"
  exit 0
fi

printf '%sFALLO %s comprobacion(es) en rojo. Arreglalas antes de gastar dinero.%s\n' "${RED}" "${FAILS}" "${RESET}"
exit 1
