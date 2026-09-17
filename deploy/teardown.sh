#!/usr/bin/env bash
#
# teardown de la Fase 10: destruye lo que creo Terraform, en orden, y avisa de lo que
# Terraform NO puede destruir (el broker gestionado, sus topics y sus credenciales).
#
#   bash deploy/teardown.sh
#
# SIN -auto-approve a proposito: esto borra la VM, la Lambda, el bucket, la cola de
# fallos, los parametros SSM, las alarmas y los roles. Terraform pide su confirmacion por
# teclado y este script pide la suya antes, para que no se destruya nada por un dedo
# torcido en el historial del shell.
#
# Lo que este script NO hace: borrar el cluster del proveedor de Kafka (no lo creo
# Terraform) ni las API keys. Eso son pasos de consola, y el script los recuerda al final.
#
# Comentarios en espanol sin acentos, como el resto del shell del proyecto.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TF_DIR="${REPO_ROOT}/deploy/terraform"

# Errores de terraform a un fichero temporal: hay que distinguir "no hay state file"
# (nada que destruir) de "el state no se puede leer" (un fallo de verdad).
TMP_ERR="$(mktemp)"
trap 'rm -f "${TMP_ERR}"' EXIT

GREEN=$'\033[32m'
RED=$'\033[31m'
BOLD=$'\033[1m'
RESET=$'\033[0m'

ok()   { printf '%s  OK  %s %s\n' "${GREEN}" "$1" "${RESET}"; }
bad()  { printf '%sFALLO%s %s\n' "${RED}" "${RESET}" "$1"; }
note() { printf '      %s\n' "$1"; }
titi() { printf '\n%s%s%s\n' "${BOLD}" "$1" "${RESET}"; }

# --- Terraform (binario del host o el contenedor oficial) ---------------------------

if command -v terraform >/dev/null 2>&1; then
  TF_MODE="host"
else
  if command -v docker >/dev/null 2>&1 && docker image inspect hashicorp/terraform:1.9 >/dev/null 2>&1; then
    TF_MODE="docker"
  else
    bad "no hay terraform ni el contenedor hashicorp/terraform:1.9: no se puede destruir"
    note "instala Terraform >= 1.6, o: docker pull hashicorp/terraform:1.9"
    exit 1
  fi
fi

tf() {
  if [ "${TF_MODE}" = "host" ]; then
    (cd "${TF_DIR}" && terraform "$@")
  else
    # --user + HOME=/tmp: el destroy escribe el state y no queremos ficheros root en el
    # repo. El repo entero montado en /w porque el modulo lee ../artifacts/*.jar y
    # ../../infra/** al evaluar la configuracion.
    docker run --rm --user "$(id -u):$(id -g)" -e HOME=/tmp \
      -v "${REPO_ROOT}:/w" -w /w/deploy/terraform \
      hashicorp/terraform:1.9 "$@"
  fi
}

# --- Credenciales: sin ellas no hay destroy ni comprobacion final -------------------

if command -v aws >/dev/null 2>&1 && aws sts get-caller-identity >/dev/null 2>&1; then
  ACCOUNT="$(aws sts get-caller-identity --query Account --output text)"
  ok "credenciales AWS validas (cuenta ${ACCOUNT})"
  HAVE_AWS=1
else
  bad "sin credenciales de AWS: terraform destroy no puede borrar nada"
  note "arreglo: aws configure   (region eu-west-1, ver deploy/SPRINT-AWS.md paso 1)"
  exit 1
fi

# --- 1. Que hay en el estado --------------------------------------------------------

titi "1. Recursos en el estado de Terraform (lo que se va a destruir)"

if STATE_LIST="$(tf state list 2>"${TMP_ERR}")"; then
  if [ -z "${STATE_LIST}" ]; then
    echo "El estado esta vacio: Terraform no tiene nada que destruir."
    echo "Ojo: eso NO significa que no quede nada. El broker gestionado y sus credenciales"
    echo "viven fuera del estado; mira el punto 3 igualmente."
    NOTHING_TO_DESTROY=1
  else
    printf '%s\n' "${STATE_LIST}" | sed 's/^/      /'
    echo
    echo "Total: $(printf '%s\n' "${STATE_LIST}" | wc -l) recursos."
    NOTHING_TO_DESTROY=0
  fi
elif grep -q "No state file was found" "${TMP_ERR}"; then
  # Sin state file no hay nada que Terraform pueda destruir (ya se destruyo, o el state
  # vivia en otro sitio). No es un error: se sigue con los avisos manuales.
  echo "No hay state file en deploy/terraform/: Terraform no tiene nada que destruir."
  echo "Ojo: eso NO significa que no quede nada. El broker gestionado y sus credenciales"
  echo "viven fuera del estado; mira el punto 3 igualmente."
  NOTHING_TO_DESTROY=1
else
  bad "no se ha podido leer el estado (terraform state list ha fallado)"
  sed 's/^/      /' "${TMP_ERR}"
  note "si el state esta en un backend remoto, configura el backend antes de destruir"
  exit 1
fi

# --- 2. Confirmacion y destroy ------------------------------------------------------

if [ "${NOTHING_TO_DESTROY}" -eq 0 ]; then
  titi "2. Destruir"
  echo "Esto borra la VM, su disco, la Lambda, el bucket S3, la cola SQS, los parametros"
  echo "SSM, las alarmas SNS/CloudWatch, el secreto de Secrets Manager y los roles IAM."
  echo "Es irreversible. El broker gestionado NO se toca (punto 3)."
  echo
  # Prompt con printf y no con `read -p`: bash se calla el de `read -p` cuando la entrada
  # no es un terminal (por ejemplo, si el script se lanza desde otro script).
  printf "Escribe exactamente 'destruir' para continuar: "
  read -r RESPUESTA
  if [ "${RESPUESTA}" != "destruir" ]; then
    echo "Cancelado: no se ha destruido nada."
    exit 1
  fi

  echo
  if ! tf destroy; then
    bad "terraform destroy ha fallado"
    echo
    echo "El fallo mas probable es que el bucket S3 no este vacio (Terraform borra los"
    echo "objetos que el mismo creo, pero no los que aparecieron por otro lado). El error"
    echo "exacto es 'BucketNotEmpty'. Arreglo:"
    echo
    echo "  cd deploy/terraform"
    echo "  aws s3 rm \"s3://\$(terraform output -raw artifacts_bucket)\" --recursive"
    echo "  terraform destroy"
    echo
    echo "El segundo mas probable: un recurso creado a mano por fuera (una regla, una"
    echo "politica). El mensaje de Terraform dice cual; quitalo a mano y reintenta."
    exit 1
  fi
  ok "terraform destroy ha terminado"
else
  titi "2. Destruir"
  echo "Saltado: no hay nada en el estado."
fi

# --- 3. Lo que queda fuera de Terraform ---------------------------------------------

titi "3. Lo que Terraform NO destruye (hazlo tu, a mano)"

echo "a) El cluster gestionado de Kafka + Schema Registry. Es lo unico que factura por"
echo "   hora aunque no haya nada corriendo, y no lo creo Terraform. En la consola del"
echo "   proveedor:"
echo "     - Confluent Cloud: borra el cluster (los topics se van con el). Despues borra"
echo "       las API keys: la del cluster y la del Schema Registry (es otra distinta), y"
echo "       el service account si lo creaste. Si el environment era solo para el sprint,"
echo "       borralo tambien. (verificar los nombres exactos del menu en tu consola)"
echo "     - Redpanda Cloud: borra el cluster; los topics y los usuarios del cluster se"
echo "       van con el. Si el proveedor guarda las credenciales aparte, borralas tambien."
echo "   Si prefieres conservar el cluster, como minimo borra las API keys para que nada"
echo "   pueda autenticarse contra el."
echo
echo "b) Los topics, si conservas el cluster (no cuestan casi nada, pero guardan datos):"
echo "   usa el helper topics() de deploy/README.md, seccion 2:"
echo "     topics --delete --topic market.ticks.raw    (y el resto de la tabla)"
echo
echo "c) Las credenciales en tu maquina:"
echo "     - El state de Terraform lleva los secretos de Kafka en claro. Si no vas a"
echo "       reutilizarlo, borralo despues de comprobar que destroy termino:"
echo "         rm -f deploy/terraform/terraform.tfstate deploy/terraform/terraform.tfstate.backup"
echo "         rm -f deploy/terraform/phase10.tfplan deploy/terraform/terraform.tfvars"
echo "     - El client.properties que usaste para crear los topics lleva la clave SASL:"
echo "         rm -f /tmp/client.properties"
echo "     - Cierra la terminal donde hiciste los 'export TF_VAR_...' (o 'unset' de cada uno)."
echo
echo "d) Los jars de deploy/artifacts/ y el codigo no cuestan nada: se quedan."

# --- 4. Comprobacion final con aws --------------------------------------------------

titi "4. Comprobacion final: que no quede nada cobrando"

LEFTOVERS=0

# `None` es lo que imprime el CLI cuando la lista esta vacia; se normaliza a vacio.
limpiar() { printf '%s' "$1" | tr -d '\r' | sed 's/^None$//'; }

# EC2: instancias del proyecto en cualquier estado que no sea 'terminated'.
if INSTANCIAS_RAW="$(aws ec2 describe-instances \
  --filters "Name=tag:Project,Values=aggora" \
            "Name=instance-state-name,Values=pending,running,shutting-down,stopping,stopped" \
  --query 'Reservations[].Instances[].InstanceId' --output text 2>/dev/null)"; then
  INSTANCIAS="$(limpiar "${INSTANCIAS_RAW}")"
  if [ -z "${INSTANCIAS}" ]; then
    ok "0 instancias EC2 del proyecto"
  else
    bad "quedan instancias EC2: ${INSTANCIAS}"
    note "mira si el destroy fallo a medias: aws ec2 describe-instances --instance-ids ${INSTANCIAS}"
    LEFTOVERS=$((LEFTOVERS + 1))
  fi
else
  bad "no se ha podido comprobar EC2 (permisos, red o credenciales)"
  LEFTOVERS=$((LEFTOVERS + 1))
fi

# Lambda.
if FUNCIONES_RAW="$(aws lambda list-functions \
  --query "Functions[?starts_with(FunctionName, 'aggora')].FunctionName" --output text 2>/dev/null)"; then
  FUNCIONES="$(limpiar "${FUNCIONES_RAW}")"
  if [ -z "${FUNCIONES}" ]; then
    ok "0 funciones Lambda del proyecto"
  else
    bad "quedan funciones Lambda: ${FUNCIONES}"
    note "si destroy fallo, borralas a mano: aws lambda delete-function --function-name <nombre>"
    LEFTOVERS=$((LEFTOVERS + 1))
  fi
else
  bad "no se ha podido comprobar Lambda (permisos, red o credenciales)"
  LEFTOVERS=$((LEFTOVERS + 1))
fi

# SQS.
if COLAS_RAW="$(aws sqs list-queues --queue-name-prefix aggora --query 'QueueUrls' --output text 2>/dev/null)"; then
  COLAS="$(limpiar "${COLAS_RAW}")"
  if [ -z "${COLAS}" ]; then
    ok "0 colas SQS del proyecto"
  else
    bad "quedan colas SQS: ${COLAS}"
    note "si destroy fallo, borralas a mano: aws sqs delete-queue --queue-url <url>"
    LEFTOVERS=$((LEFTOVERS + 1))
  fi
else
  bad "no se ha podido comprobar SQS (permisos, red o credenciales)"
  LEFTOVERS=$((LEFTOVERS + 1))
fi

echo
if [ "${LEFTOVERS}" -eq 0 ]; then
  ok "no queda nada del proyecto en AWS (segun estas tres comprobaciones)"
else
  bad "${LEFTOVERS} comprobacion(es) con restos: no des el sprint por cerrado"
fi

echo
echo "Ultima parada, que ninguna API te puede decir por ti: la pagina de facturacion de"
echo "la consola. Comprueba que no queda el cluster del proveedor y que la linea de EC2"
echo "deja de crecer. (El desglose tarda unas horas en reflejar el destroy; el cluster"
echo "borrado se nota antes.)"

[ "${LEFTOVERS}" -eq 0 ] || exit 1
exit 0
