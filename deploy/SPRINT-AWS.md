# Sprint de AWS: la Fase 10 de verdad, de principio a fin y hasta destruirla

Esta es la guía operativa del sprint de 1-2 semanas: desplegar la Fase 10 en AWS, aprender
mirando la plataforma funcionando, y **destruirlo todo al final**. Está en español porque
la va a seguir una persona; `deploy/README.md` (en inglés) sigue siendo el runbook de
referencia del repositorio y esta guía no lo contradice: lo ordena.

**Regla de oro:** el sprint **no ha terminado** hasta que se completa el **paso 8**
(teardown). Mientras el broker y la VM existan, hay una factura corriendo.

Dos avisos antes de empezar:

- **Nada de esto se ha ejecutado nunca contra AWS.** En este repo no hay cuenta ni
  credenciales y no se ha gastado un euro. Lo que está verificado (Terraform, el compose,
  la unidad de systemd, los tests) está listado al final de `deploy/README.md`. Lo que solo
  se puede comprobar desplegando está marcado en esta guía.
- **`(verificar)`** marca los datos y comandos que no salen de un fichero de `deploy/` ni de
  `docs/`: son correctos según la API de AWS o la consola del proveedor, pero no están
  copiados de este repositorio, así que se confirman al ejecutarlos.

**Convención de rutas:** todos los comandos se ejecutan desde la raíz del repo, salvo los
que dicen `cd deploy/terraform`. Las variables (`BOOTSTRAP`, los `TF_VAR_`) viven en la
terminal en la que las exportes: si abres otra, vuelve a exportarlas.

---

## Paso 0 — Antes de empezar (5 minutos que ahorran dinero)

- [ ] **Crear una alarma de presupuesto en AWS.** No es un límite de gasto (AWS Budgets
      avisa, no apaga nada): es el aviso que evita descubrir el susto a fin de mes.
- [ ] **Leer la tabla de costes** de abajo y decidir un techo realista para 1-2 semanas.
- [ ] **Aceptar el compromiso:** el sprint termina en el **paso 8**, destruyendo todo.

### 0.1 Alarma de presupuesto

Por CLI (necesita permisos de `budgets`; `(verificar)`, no sale del repo):

```bash
export TOPE_USD=30                  # tu techo: elige el numero, no sale del repo
export NOTIFICAR_A="tu@example.com"

cat > /tmp/aggora-budget.json <<EOF
{
  "BudgetName": "aggora-sprint",
  "BudgetLimit": { "Amount": "${TOPE_USD}", "Unit": "USD" },
  "TimeUnit": "MONTHLY",
  "BudgetType": "COST"
}
EOF

cat > /tmp/aggora-budget-alertas.json <<EOF
[
  {
    "Notification": { "NotificationType": "ACTUAL",     "ComparisonOperator": "GREATER_THAN", "Threshold": 50 },
    "Subscribers":  [ { "SubscriptionType": "EMAIL", "Address": "${NOTIFICAR_A}" } ]
  },
  {
    "Notification": { "NotificationType": "FORECASTED", "ComparisonOperator": "GREATER_THAN", "Threshold": 100 },
    "Subscribers":  [ { "SubscriptionType": "EMAIL", "Address": "${NOTIFICAR_A}" } ]
  }
]
EOF

aws budgets create-budget \
  --account-id "$(aws sts get-caller-identity --query Account --output text)" \
  --budget file:///tmp/aggora-budget.json \
  --notifications-with-subscribers file:///tmp/aggora-budget-alertas.json
```

Por consola (`(verificar)`): **Billing and Cost Management → Budgets → Create budget →
Monthly cost budget**, pon el techo y dos alertas (50 % real, 100 % previsto). El correo de
la alerta hay que confirmarlo (como el de SNS).

### 0.2 Qué cuesta cada pieza y cuándo factura

Estimaciones (no es un presupuesto), leídas el **2026-09-16** en las fuentes de la última
columna. Los números son los de `deploy/README.md` §8; la columna de "cuándo factura" es la
que importa para un sprint corto.

| Pieza | Estimado / mes | Cuándo factura | Fuente (2026-09-16) |
|---|---|---|---|
| EC2 `t4g.small` on-demand | ~$13.43 (`$0.0184/h`) | **Por hora encendida**: mientras la instancia exista y no esté `stopped`; parada, deja de contar el cómputo | [Holori, t4g.small eu-west-1](https://calculator.holori.com/aws/ec2/t4g.small?region=eu-west-1) |
| EBS gp3, 20 GB | ~$1.76 (`$0.088/GB-mo`) | Por GB-mes, mientras el volumen exista (se va con la instancia al destruir) | [AWS EC2 Price List API, eu-west-1](https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonEC2/current/eu-west-1/index.json) |
| Lambda a ~60 msg/s | ~$52 a ~$130 | **Por uso**: por invocación (`$0.20/M`) y por GB-s (`$0.0000166667`) | [AWS Lambda Price List API, eu-west-1](https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AWSLambda/current/eu-west-1/index.json) |
| CloudWatch Logs (Lambda) | ~$5 a ~$15 | **Por uso**: ~15 GB ingeridos a `$0.57/GB` con el feed a 60 msg/s | [AWS CloudWatch Price List API, eu-west-1](https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonCloudWatch/current/eu-west-1/index.json) |
| Alarmas CloudWatch (3) | ~$0.30 | Por alarma-mes (`$0.10`), mientras existan | misma lista de precios |
| Secrets Manager (1 secreto) | ~$0.40 | Por secreto-mes, mientras exista | [AWS Secrets Manager pricing](https://aws.amazon.com/secrets-manager/pricing/) |
| SQS (cola de fallos) | < $0.01 | **Por uso**: `$0.40`/millón de peticiones; solo entran fallos | [AWS SQS Price List API, eu-west-1](https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AWSQueueService/current/eu-west-1/index.json) |
| S3 (jars + config, ~600 MB) | < $0.01 | **Por uso**: `$0.023/GB-mo`; la transferencia en la misma región es gratis | [AWS S3 Price List API, eu-west-1](https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonS3/current/eu-west-1/index.json) |
| SSM Parameter Store | **$0.00** | Los parámetros estándar (incluido `SecureString`) son gratis | [AWS Systems Manager pricing](https://aws.amazon.com/systems-manager/pricing/) |
| **Broker gestionado + Schema Registry** | **la gran incógnita** | **Por hora de clúster + GB**, procese o no. Es la línea que no descansa | [Confluent Cloud pricing](https://www.confluent.io/confluent-cloud/pricing/), [Redpanda pricing](https://www.redpanda.com/pricing) |

**La cuenta de 2 semanas** (aritmética a partir de la tabla anterior, estimación): EC2
~$6.2 (336 h × `$0.0184`); EBS ~$0.9; Lambda y logs, si el feed de 60 msg/s está encendido
**las dos semanas seguidas**, ~$26-72; alarmas + secreto + SQS + S3, < $1. Total AWS del
sprint: **~$35-80 con el feed encendido a todo trapo, y ~$10 si apenas lo enciendes**. Más
el broker, que depende del proveedor y del tier y no baja por apagar el feed.

Consecuencias prácticas:

- La **VM** y el **broker** son las dos líneas por hora. La VM se puede **parar** entre
  sesiones de trabajo (deja de contar el cómputo; el disco sigue); el broker **no**.
- **Lambda y S3 facturan por uso**: si el simulador no produce, casi no cuestan. El feed a
  60 msg/s es lo que convierte a Lambda en la línea cara (~$52-130/mes), que es exactamente
  la lección del capítulo 19 de `docs/kafka-101.md`.
- **`t4g.small` va justo** (2 GB para 7 JVMs + Postgres + Prometheus + Grafana). Si hay OOM
  kills, `instance_type = "t4g.medium"` y a correr.

---

## Paso 1 — Cuenta y CLI

- [ ] `bash deploy/preflight.sh` en verde (salvo lo que necesita credenciales: eso es este paso).
- [ ] `aws configure` con la región `eu-west-1`.
- [ ] `aws sts get-caller-identity` responde.
- [ ] Decidir permisos (cómodo vs. mínimo) y asumir el riesgo.
- [ ] Instalar el plugin de Session Manager (sin él no se entra en la VM: paso 6).

`deploy/preflight.sh` comprueba todo lo comprobable sin gastar dinero: terraform (o su
contenedor), el CLI de AWS, las credenciales, los 7 jars, el jar gordo de la Lambda, las
`TF_VAR_` obligatorias y `terraform validate`. Los rojos, con su motivo.

### 1.1 Configurar el CLI

```bash
aws configure
# AWS Access Key ID [None]: AKIA...
# AWS Secret Access Key [None]: ...
# Default region name [None]: eu-west-1
# Default output format [None]: json

# Comprobar que hay credenciales y a que cuenta apuntan:
aws sts get-caller-identity
```

**Región: `eu-west-1`** (la del `default` de `variables.tf` y la que el runbook supone junto
al broker). Si usas otra, exporta `TF_VAR_aws_region` **y** pásale `--region` a los comandos
`aws` que no la lean del perfil.

### 1.2 Permisos

- **Lo cómodo para aprender: `AdministratorAccess`.** Dilo y asúmelo: es una cuenta de
  práctica, de un solo dueño y de 1-2 semanas, y depurar un `AccessDenied` a mitad de sprint
  cuesta más que el riesgo. **El riesgo es real**: esa credencial puede crear, borrar y
  facturar cualquier cosa de la cuenta. Al terminar el paso 8, **borra el usuario/clave** que
  hayas creado para el sprint.
- **Lo mínimo**, si prefieres apretar: los servicios que toca `deploy/terraform/main.tf` son
  `ec2` (instancia, SG, VPC por defecto), `s3`, `iam` (roles, políticas, instance profile),
  `lambda`, `sqs`, `sns`, `cloudwatch` (alarmas y log groups), `logs`, `ssm` (parámetros y
  Session Manager), `secretsmanager`, `kms` (solo para descifrar lo que pasa por SSM) y
  `budgets` para el paso 0. `(verificar)` la lista exacta de acciones de cada uno en la
  consola de IAM.
- Las credenciales del **broker** (usuario/clave SASL, API keys) son **otra cosa**: no son
  credenciales de AWS y se crean en la consola del proveedor (paso 2).

### 1.3 Plugin de Session Manager

La VM **no abre el puerto 22**. Se entra con Session Manager, y eso necesita el **plugin**
en tu máquina (no viene con el CLI). `(verificar)` el enlace de instalación en
<https://docs.aws.amazon.com/systems-manager/latest/userguide/session-manager-working-with-install-plugin.html>.

```bash
# Comprobacion (si no dice una version, falta):
session-manager-plugin --version
```

`deploy/preflight.sh` también lo comprueba en rojo si falta.

---

## Paso 2 — El broker gestionado, a mano en la consola

- [ ] Elegir proveedor (Confluent Cloud o Redpanda Cloud) en **`eu-west-1`**, junto a la VM.
- [ ] Confirmar las **tres comprobaciones obligatorias** antes de comprometerse.
- [ ] Apuntar los cuatro datos + la API key propia del Schema Registry (Confluent).
- [ ] Crear los **12 topics** con el nombre, las particiones y la política de limpieza exactos.

Esto **no lo crea Terraform** a propósito: un clúster factura por hora y guarda estado, y
Terraform es para lo que se puede destruir y recrear sin perder nada (`deploy/README.md`,
`docs/kafka-101.md` capítulo 19).

### 2.1 Las tres comprobaciones obligatorias

Antes de meter credenciales en ningún sitio, confirma con el proveedor:

1. **Transacciones.** Sin ellas no hay exactly-once y el motor de matching de la Fase 4 no
   funciona. `(verificar)`: si el broker no las soporta, el servicio falla al abrir el
   productor transaccional (`UnsupportedVersionException` / `InvalidProducerEpoch`).
2. **Topic admin.** Tienes que poder crear topics y cambiar particiones desde un cliente
   (es lo que hace el bloque de comandos de 2.3).
3. **Kafka Streams.** Tres de los siete servicios son topologías de Streams
   (`analytics-streams`, `portfolio-risk`, `alerting-service`); un broker que solo haga
   produce/consume no vale. Los brokers que hablan **HTTP** en vez del protocolo de Kafka
   quedan descartados por muy "serverless" que se anuncien.

### 2.2 Qué apuntar

| Valor | Confluent Cloud | Redpanda Cloud |
|---|---|---|
| Bootstrap servers | Cluster → Endpoint (`pkc-xxxxx.eu-west-1.aws.confluent.cloud:9092`) | Cluster → Bootstrap server |
| Schema Registry URL | Stream Governance endpoint (`https://psrc-xxxxx...`) | Schema Registry URL |
| SASL username | Cluster API key | Cluster user |
| SASL password | Cluster API secret | Cluster password |

Y dos trampas que conviene saber **antes** de crear nada (`deploy/README.md` §1):

- **Mecanismo SASL.** Confluent Cloud usa `PLAIN` (el usuario es la API key). Redpanda Cloud
  usa `SCRAM-SHA-256`. De esto dependen `TF_VAR_kafka_sasl_mechanism` **y** el tipo
  `source_access_configuration` del *event source mapping* de la Lambda.
- **En Confluent Cloud el Schema Registry tiene su propia API key**, distinta de la del
  clúster. Va en `TF_VAR_kafka_sr_username` / `TF_VAR_kafka_sr_password`; si las dejas
  vacías, `user-data.sh` no escribe autenticación básica en `common.env` y el Schema
  Registry responderá `401` a los servicios.

### 2.3 Crear los topics (la tabla es el contrato)

Mismos nombres, mismas particiones y misma política que en local. **No fijes factor de
réplica**: deja el que aplique el broker gestionado (normalmente 3). Kafka Streams crea sus
propios topics internos de changelog y reparto; deja la creación automática como venga.

| Topic | Particiones | `cleanup.policy` | Lo escribe |
|---|---|---|---|
| `market.ticks.raw` | 6 | delete | `market-data-simulator` |
| `market.ticks.canonical` | 6 | delete | `ingestion-normalizer` (Lambda) |
| `market.ticks.raw.DLT` | 1 | delete | `ingestion-normalizer` (Lambda) |
| `market.fx.reference` | 1 | **compact** | `ingestion-normalizer` (Lambda) |
| `orders.incoming` | 6 | delete | `market-data-simulator` |
| `orders.incoming.DLT` | 1 | delete | `order-matching-engine` |
| `orders.executions` | 6 | delete | `order-matching-engine` |
| `portfolio.updates` | 6 | delete | `portfolio-risk` |
| `market.analytics` | 6 | delete | `analytics-streams` |
| `market.arbitrage` | 6 | delete | `analytics-streams` |
| `alerts.raised` | 3 | delete | `alerting-service` |
| `audit.events` | 3 | **compact** | `audit-log` |

El helper y los comandos, tal cual están en `deploy/README.md` §2:

```bash
export BOOTSTRAP="pkc-xxxxx.eu-west-1.aws.confluent.cloud:9092"

cat > /tmp/client.properties <<'EOF'
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="KEY" password="SECRET";
EOF
chmod 600 /tmp/client.properties

topics() {
  docker run --rm -i -v /tmp/client.properties:/client.properties apache/kafka:3.9.0 \
    /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" \
    --command-config /client.properties "$@"
}

topics --create --topic market.ticks.raw           --partitions 6
topics --create --topic market.ticks.canonical     --partitions 6
topics --create --topic market.ticks.raw.DLT       --partitions 1
topics --create --topic market.fx.reference        --partitions 1 --config cleanup.policy=compact
topics --create --topic orders.incoming            --partitions 6
topics --create --topic orders.incoming.DLT        --partitions 1
topics --create --topic orders.executions          --partitions 6
topics --create --topic portfolio.updates          --partitions 6
topics --create --topic market.analytics           --partitions 6
topics --create --topic market.arbitrage           --partitions 6
topics --create --topic alerts.raised              --partitions 3
topics --create --topic audit.events               --partitions 3 --config cleanup.policy=compact

topics --list
```

Para **Redpanda Cloud**, cambia la línea del mecanismo a `SCRAM-SHA-256` y el módulo de
login a `org.apache.kafka.common.security.scram.ScramLoginModule required username="..."
password="...";`.

El helper `topics()` vive solo en la terminal donde lo defines: si abres otra, vuelve a
definirlo.

**Comprobación de que quedaron bien** (antes de seguir, no después):

```bash
topics --describe --topic market.ticks.raw
topics --describe --topic market.fx.reference    # debe decir cleanup.policy=compact
```

- [ ] `topics --list` muestra los 12.
- [ ] Las particiones coinciden con la tabla (si `market.ticks.raw` tiene 3, el paralelismo
      cambia y el simulador se reparte de otra forma: capítulo 21 de `docs/kafka-101.md`).

---

## Paso 3 — Compilar y juntar los artefactos

- [ ] `bash deploy/collect-artifacts.sh` termina sin errores.
- [ ] Hay **7 jars** en `deploy/artifacts/`.
- [ ] Existe el jar **gordo** de la Lambda (`...-shaded.jar`), porque Terraform lo sube.

```bash
bash deploy/collect-artifacts.sh

# Los 7 de la VM (nombre exacto que espera user-data.sh):
ls -1 deploy/artifacts/*.jar
ls -1 deploy/artifacts/*.jar | wc -l        # -> 7

# El gordo de la Lambda (Terraform lo lee de aqui, NO esta en deploy/artifacts/):
ls -lh services/lambda/ingestion-normalizer-lambda/target/*-shaded.jar
```

| Qué | Dónde | Por qué |
|---|---|---|
| 7 jars Spring (`<servicio>-spring-0.1.0-SNAPSHOT.jar`) | `deploy/artifacts/` | Es el nombre exacto que resuelve la unidad `aggora@.service` |
| `ingestion-normalizer-lambda-0.1.0-SNAPSHOT-shaded.jar` (~34 MB) | `services/lambda/ingestion-normalizer-lambda/target/` | Es el **gordo**, con todas las dependencias dentro; Terraform lo sube a la Lambda |

El jar **sin** `-shaded` (unos 120 KB) es el fino: Lambda fallaría en la primera invocación
con `ClassNotFoundException` porque no lleva dependencias. `lambda_jar_path` en
`variables.tf` permite cambiarlo, pero no hace falta.

El plan de Terraform **falla a propósito** si `deploy/artifacts/` está vacío (la
precondición de `terraform_data.artifacts_present`): mejor un error claro que una VM que
arranca sin jars.

---

## Paso 4 — Las variables (`TF_VAR_*`)

- [ ] Exportar los 5 obligatorios (los que no tienen `default` en `variables.tf`).
- [ ] Exportar el mecanismo SASL y, en Confluent, la pareja del Schema Registry.
- [ ] **Nunca** meter secretos en `terraform.tfvars` ni en un `.env` versionado.

`variables.tf` es la fuente de verdad de los nombres. Las que **no** tienen `default` (y por
tanto hay que pasar sí o sí) son cinco: `kafka_bootstrap_servers`,
`kafka_schema_registry_url`, `kafka_sasl_username`, `kafka_sasl_password` y
`postgres_password`. `deploy/preflight.sh` lo comprueba leyendo ese fichero.

```bash
cd deploy/terraform

export TF_VAR_kafka_bootstrap_servers="pkc-xxxxx.eu-west-1.aws.confluent.cloud:9092"
export TF_VAR_kafka_schema_registry_url="https://psrc-xxxxx.eu-west-1.aws.confluent.cloud"
export TF_VAR_kafka_sasl_username="..."
export TF_VAR_kafka_sasl_password="..."
export TF_VAR_postgres_password="$(openssl rand -hex 16)"
# Solo Confluent Cloud: el Schema Registry tiene su propia API key.
# export TF_VAR_kafka_sr_username="..."
# export TF_VAR_kafka_sr_password="..."

# No secretos: variables.tf ya trae buenos valores por defecto.
export TF_VAR_kafka_sasl_mechanism="PLAIN"
export TF_VAR_alarm_email="tu@example.com"

cd ../..
```

Detalles que ahorran tiempo:

- **`TF_VAR_` y no `terraform.tfvars`**: los secretos no acaban ni en el historial ni en el
  repositorio (`terraform.tfvars` está en `deploy/terraform/.gitignore`). Mira
  `terraform.tfvars.example` para la lista completa: `aws_region`, `project`,
  `instance_type`, `subnet_id`, `artifacts_bucket_name`, `kafka_sasl_mechanism`,
  `lambda_source_topic`, `lambda_target_topic`, `alarm_email`, `log_retention_days` y las
  claves opcionales de proveedores de datos. Todas esas tienen `default`, así que solo hace
  falta exportarlas si quieres cambiarlas.
- **`alarm_email`**: si la dejas vacía (default `""`), no se crea la suscripción SNS. Si la
  pones, el correo de confirmación de SNS hay que **pincharlo** o no llega ninguna alarma.
- **Claves de datos (`twelvedata`/`alphavantage`)**: opcionales. Sin ellas el simulador
  arranca igual con precios semilla y ticks sintéticos, que es lo más barato para empezar.
- **El state de Terraform contiene los secretos** (los valores de los parámetros SSM y las
  variables de entorno de la Lambda). Está fuera de git, pero es un secreto: no lo subas a
  ningún sitio ni lo pegues en un chat.

---

## Paso 5 — `terraform init`, `plan` y `apply`

- [ ] `terraform init` descarga el provider de AWS.
- [ ] `terraform plan -out=phase10.tfplan` enseña el plan y se guarda.
- [ ] Revisar el plan: **~44-49 recursos nuevos**, todos `to add`, ninguno `to destroy`.
- [ ] `terraform apply phase10.tfplan`.
- [ ] Leer los `outputs` y apuntar el comando de sesión SSM.

```bash
cd deploy/terraform

terraform init

terraform plan -out=phase10.tfplan

# Lee el plan antes de aplicarlo. Cuando estes convencido:
terraform apply phase10.tfplan
```

### 5.1 Qué dice el plan

Primero lee los **data sources** (necesitan credenciales: si `init` va bien y el plan falla
con `NoCredentialProviders`, es el paso 1): la identidad de la cuenta (`aws_caller_identity`),
la **VPC por defecto**, sus subredes, y el parámetro público de AWS con la AMI
**Amazon Linux 2023 ARM64** más reciente.

Después anuncia los recursos **nuevos**. Ninguno existe antes, así que es
`Plan: N to add, 0 to change, 0 to destroy`. `N` está entre **44 y 49** según las variables
opcionales:

| Bloque | Piezas | Cuántos |
|---|---|---|
| Bucket S3 + bloqueo de acceso público + cifrado | `aws_s3_bucket`, `..._public_access_block`, `..._server_side_encryption_configuration` | 3 |
| Objetos del bucket: 7 jars + 6 de configuración (4 de `infra/`, el compose y la unidad de systemd) | `aws_s3_object.jars`, `aws_s3_object.config` | 13 |
| Precondición de "hay jars" | `terraform_data.artifacts_present` | 1 |
| Parámetros SSM: 8 fijos + 2 del Schema Registry + 2 de proveedores de datos (estos dos, opcionales) | `aws_ssm_parameter.*` | 8-12 |
| IAM de la VM: rol, instance profile, política inline y adjunto de `AmazonSSMManagedInstanceCore` | `aws_iam_role.instance`, `..._instance_profile`, `..._role_policy`, `..._role_policy_attachment` | 4 |
| Security group **sin reglas de entrada** | `aws_security_group.vm` | 1 |
| VM ARM `t4g.small` con disco gp3 de 20 GB cifrado y `user_data` | `aws_instance.vm` | 1 |
| IAM de la Lambda: rol, política inline y adjunto de ejecución básica | `aws_iam_role.lambda` y compañía | 3 |
| Secreto de Secrets Manager con el SASL del event source mapping | `aws_secretsmanager_secret`, `..._version` | 2 |
| Log group de la Lambda (retención 14 días) | `aws_cloudwatch_log_group.lambda` | 1 |
| Cola SQS de fallos | `aws_sqs_queue.lambda_failures` | 1 |
| Función Lambda (`java21`, 1024 MB, timeout 60 s) | `aws_lambda_function.normalizer` | 1 |
| Event source mapping (lote 10, `starting_position = LATEST`, destino SQS) | `aws_lambda_event_source_mapping.normalizer` | 1 |
| Topic SNS + suscripción de correo (esta, solo si pones `alarm_email`) | `aws_sns_topic.alarms`, `..._subscription` | 1-2 |
| Alarmas: errores de Lambda, estado de la VM, backlog de la cola de fallos | `aws_cloudwatch_metric_alarm.*` | 3 |

Si el plan dice que va a **destruir** algo en la primera pasada, para: no hay nada que
destruir en una cuenta limpia, así que estás aplicando sobre un estado que no es el que
crees.

### 5.2 Qué hace el `apply`, paso a paso

1. Crea el bucket S3 y lo deja privado y cifrado (AES256).
2. **Sube los 13 objetos**: los 7 jars (casi 600 MB entre todos) y la configuración. Esta es la
   parte lenta si tu conexión de subida es doméstica; Terraform sube los jars desde tu
   máquina.
3. Escribe los parámetros SSM con las credenciales de Kafka.
4. Crea roles, políticas e instance profile de la VM.
5. Crea el security group (solo egreso) y **lanza la instancia**. Aquí se ejecuta
   `deploy/user-data.sh` como cloud-init.
6. En paralelo: el secreto de Secrets Manager, el log group, la cola SQS, la **Lambda**
   (sube el jar gordo, ~34 MB), el **event source mapping**, el topic SNS y las 3 alarmas.
7. El `apply` **termina cuando la instancia está `running`**, no cuando el pipeline está
   listo: el `user_data` sigue trabajando unos minutos más.
8. Mientras tanto, dentro de la VM: instala Java 21, Docker y el plugin de compose; crea el
   usuario `aggora`; **baja los casi 600 MB de jars** de S3; lee los secretos de SSM; escribe
   `/etc/aggora/*.env`; levanta Postgres, Prometheus, Grafana y kafka-exporter con compose;
   y arranca los 7 servicios con `systemd`.

**Cuánto tarda** (estimación, no medido): `init` 30-60 s; `plan` 10-30 s; `apply` 3-8 min
dominado por la subida de jars y el arranque de la EC2; y **5-10 min más** hasta que los 7
servicios están de pie. El primer arranque es el más lento (instala paquetes y baja medio
giga). `user_data` es idempotente: volver a ejecutarlo no rompe nada.

### 5.3 Cómo leer los `outputs`

```bash
terraform output                       # todo, en legible
terraform output -raw instance_id      # solo el valor, sin comillas
```

| Output | Qué es |
|---|---|
| `instance_id` | ID de la VM (lo pide `aws ssm start-session`) |
| `instance_public_ip` | IP pública; no hay puertos abiertos, se usa desde dentro del túnel SSM |
| `ssm_start_session` | El comando exacto para entrar por Session Manager |
| `ssm_port_forward_gateway` | El túnel para ver la página del gateway en `http://localhost:8089` |
| `lambda_function_name` | `aggora-ingestion-normalizer` |
| `sqs_queue_url` | URL de la cola de fallos |
| `artifacts_bucket` | Bucket con los jars y la configuración |
| `ssm_parameter_prefix` | `/aggora` |

Y el atajo que más vas a usar:

```bash
$(terraform output -raw ssm_start_session)
```

---

## Paso 6 — Verificar la VM

- [ ] La instancia aparece como *managed node* en SSM.
- [ ] Entrar con `$(terraform output -raw ssm_start_session)`.
- [ ] `systemctl status 'aggora@*'` sin servicios en `failed` (tras un ciclo de reintento).
- [ ] Los topics avanzan y el **lag** se ve en Grafana / kafka-exporter.
- [ ] El **state store** responde por la sonda de salud y por la consulta interactiva.
- [ ] La página del gateway se ve **por el túnel SSM**, no abriendo un puerto.

### 6.1 Por qué SSM y no SSH (esto es una decisión, no una limitación)

El security group de la VM tiene **cero reglas de entrada**: solo egreso a internet (para el
broker, S3, SSM y los proveedores de datos). No hay puerto 22, no hay key pair que perder, y
por tanto no hay forma de llegar a la VM desde fuera salvo **Session Manager**, que va por
el agente hacia fuera. La razón es la de `deploy/README.md` §7: en esa máquina hay
credenciales de un broker; un puerto abierto a internet es la vía más rápida de perderlas.
Todo lo que hay dentro (Grafana, el gateway, `/actuator/health`) se mira por un **túnel de
SSM**, que es cifrado, autenticado con tu identidad de AWS y no expone nada.

Primero comprueba que el agente está registrado (el `user_data` tarda un poco en instalarlo):

```bash
aws ssm describe-instance-information \
  --filters "Key=InstanceIds,Values=$(terraform output -raw instance_id)" \
  --query 'InstanceInformationList[].{Id:InstanceId,Ping:PingStatus,Agent:AgentVersion,Plataforma:PlatformName}' \
  --output table
```

Cuando `PingStatus` sea `Online`:

```bash
$(terraform output -raw ssm_start_session)
```

### 6.2 Los servicios

Dentro de la VM:

```bash
systemctl status 'aggora@*' --no-pager

journalctl -u aggora@analytics-streams -n 50 --no-pager
journalctl -u aggora@gateway-ws -n 50 --no-pager
journalctl -u aggora@analytics-streams -f          # seguir en vivo (Ctrl-C para salir)
```

Un servicio en `activating`/`failed` que vuelve cada 15 s **no es un bug**: es la carrera de
topics reintentándose (`Restart=always` + `RestartSec=15`). Kafka Streams falla si su topic
de origen aún no existe (`MissingSourceTopicException`), y los topics los crea el servicio
que escribe en ellos. Espera un ciclo completo y mira otra vez. Si a los 5 minutos sigue
igual, entonces sí: mira el log.

Dos apuntes de memoria, porque `t4g.small` va justo:

```bash
free -m
journalctl -k --no-pager | grep -i 'oom\|killed process' | tail
```

Si aparecen OOM kills, sube a `t4g.medium` (paso 9, fallo 6).

### 6.3 Que los topics avanzan y el lag

```bash
# El helper topics() del paso 2, en tu maquina (necesita Docker y /tmp/client.properties).
topics --describe --topic market.ticks.canonical
topics --describe --topic market.analytics
```

El **lag por grupo de consumo** es la señal de que el pipeline no se está atascando, y es lo
único que delata a los servicios **sin** servidor web (`portfolio-risk`, `alerting-service`),
que no tienen `/actuator/health`:

- En **Grafana** (`http://localhost:3000` dentro de la VM, acceso anónimo), dashboard de
  Kafka, alimentado por **kafka-exporter**.
- O el exporter en crudo, dentro de la VM:

```bash
curl -s localhost:9308/metrics | grep '^kafka_consumergroup_lag' | head   # (verificar) el nombre exacto de la metrica
```

El exporter, Prometheus, Grafana y Postgres los levanta `docker compose` con
`deploy/docker-compose.vm.yml`; sus puertos están **solo en `127.0.0.1`**, así que desde
fuera de la VM también se llega a ellos por túnel (6.5).

### 6.4 El state store (que Streams no solo esté vivo, sino trabajando)

`analytics-streams` es el que expone la sonda de salud y la consulta interactiva:

```bash
curl -s localhost:8085/actuator/health

curl -s 'localhost:8085/analytics?symbol=EUR/USD&minutes=3'
curl -s localhost:8085/arbitrage | head -c 300
```

La segunda lee el **state store consultable**: si devuelve datos, el motor de Streams no solo
está vivo, está *funcionando*. Es la diferencia que una alarma de "el proceso está vivo" no
puede ver (el porqué, al final de `deploy/terraform/main.tf` y en el capítulo 18 de
`docs/kafka-101.md`). Los state stores están en disco, en
`/var/lib/aggora/streams/<servicio>`:

```bash
sudo ls -la /var/lib/aggora/streams/
```

### 6.5 Ver la página del gateway desde el navegador: túnel de puerto por SSM

**Sal de la sesión SSM** (`exit`) y, en tu máquina, donde tienes los outputs:

```bash
aws ssm start-session --target "$(terraform output -raw instance_id)" --region eu-west-1 \
  --document-name AWS-StartPortForwardingSession \
  --parameters '{"portNumber":["8089"],"localPortNumber":["8089"]}'
```

Deja esa sesión abierta y, en otra terminal:

```bash
curl -s localhost:8089 | head -c 200
# y en el navegador: http://localhost:8089
```

Por qué el túnel es mejor que abrir el puerto:

- **No hay que tocar el security group**: la VM sigue con cero reglas de entrada, y no queda
  un puerto abierto cuando cierras el sprint.
- La sesión está **autenticada con tu identidad de IAM** y autorizada por
  `AmazonSSMManagedInstanceCore`; no hay `0.0.0.0/0` ni contraseña compartida.
- El túnel se cierra con la sesión: nada sobrevive por olvido.

Cada puerto necesita su propia sesión (un `--parameters` por túnel). Para Grafana:

```bash
aws ssm start-session --target "$(terraform output -raw instance_id)" --region eu-west-1 \
  --document-name AWS-StartPortForwardingSession \
  --parameters '{"portNumber":["3000"],"localPortNumber":["3000"]}'
```

Y `9090` para Prometheus, `9308` para kafka-exporter, si los quieres en crudo. `terraform
output ssm_port_forward_gateway` imprime el del gateway para copiar.

---

## Paso 7 — Verificar la Lambda

- [ ] El *event source mapping* está `Enabled` y entrega lotes (log group con líneas).
- [ ] El topic canónico avanza.
- [ ] La cola SQS de fallos está a 0 (está vacía cuando todo va bien).
- [ ] Provocar un fallo **a propósito** y verlo en el `.DLT` con su cabecera.

### 7.1 Que está entregando lotes

```bash
aws logs tail /aws/lambda/aggora-ingestion-normalizer --since 15m --follow
```

El log group lo crea Terraform con **14 días** de retención (`log_retention_days`). Si no
ves nada, o el ESM no está entregando, o la Lambda está bien y callada: el handler solo
escribe cuando normaliza o cuando manda algo al DLT.

```bash
FUNCION="$(terraform output -raw lambda_function_name)"

aws lambda list-event-source-mappings --function-name "$FUNC" \
  --query 'EventSourceMappings[].{Estado:State,Ultimo:LastProcessingResult,Topic:Topics,Batch:BatchSize}' \
  --output table

UUID="$(aws lambda list-event-source-mappings --function-name "$FUNC" \
  --query 'EventSourceMappings[0].UUID' --output text)"

aws lambda get-event-source-mapping --uuid "$UUID" \
  --query '{Estado:State,Ultimo:LastProcessingResult,Topic:Topics,Batch:BatchSize,Inicio:StartingPosition}'
```

`LastProcessingResult` en `OK` (o `NO_RECORDS_PROCESSED` si el feed está parado) es lo que
quieres ver. `State` tiene que ser `Enabled`. El mapping lo deja Terraform con
`starting_position = LATEST` y `batch_size = 10`.

### 7.2 Que los canónicos avanzan

```bash
topics --describe --topic market.ticks.raw
topics --describe --topic market.ticks.canonical
```

Si `market.ticks.raw` sube y `market.ticks.canonical` no se mueve, el problema está entre la
Lambda y el broker (credenciales del ESM, o el broker inalcanzable): mira el log group y el
`LastProcessingResult`.

### 7.3 La cola SQS de fallos

Es donde acaba un **lote que revienta por infraestructura** (el broker no responde), no un
mensaje venenoso: ese va al `.DLT` desde el handler sin lanzar excepción (`main.tf`, y
capítulo 21 de `docs/kafka-101.md`).

```bash
aws sqs get-queue-attributes --queue-url "$(terraform output -raw sqs_queue_url)" \
  --attribute-names ApproximateNumberOfMessagesVisible,ApproximateNumberOfMessagesNotVisible \
  --output table
```

Si hay algo, se puede leer **sin borrar nada**:

```bash
aws sqs receive-message --queue-url "$(terraform output -raw sqs_queue_url)" \
  --max-number-of-messages 10 --attribute-names All
```

No lleva `--delete`: no se pierde nada, pero ojo, los mensajes que devuelve quedan
**invisibles 300 s** (`visibility_timeout_seconds = 300` en `main.tf`). El cuerpo trae el
lote de Kafka con el error. Y hay una alarma (`aggora-lambda-failure-backlog`) que salta con
solo 1 mensaje: una cola de fallos sin nadie mirándola no sirve de nada.

### 7.4 Provocar un fallo a propósito (y verlo en el `.DLT`)

Manda un mensaje **inválido** al topic crudo. El handler no lanza: lo manda al `.DLT` con el
motivo en una cabecera. Necesitas el helper `topics()` y el `BOOTSTRAP` del paso 2, y un
terminal con `client.properties`:

```bash
echo 'esto no es un tick' | docker run --rm -i -v /tmp/client.properties:/client.properties \
  apache/kafka:3.9.0 /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server "$BOOTSTRAP" --producer.config /client.properties \
  --topic market.ticks.raw
```

Espera unos segundos (el ESM hace *polling*) y mira el `.DLT`:

```bash
docker run --rm -v /tmp/client.properties:/client.properties apache/kafka:3.9.0 \
  /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server "$BOOTSTRAP" \
  --consumer.config /client.properties --topic market.ticks.raw.DLT \
  --from-beginning --max-messages 1 --property print.headers=true
```

Qué deberías ver: el mensaje original y, junto a él, la cabecera **`x-dlt-reason`** con el
motivo (`(verificar)` que tu versión de `kafka-console-consumer.sh` imprime las cabeceras con
`--property print.headers=true`; en el log de la Lambda también sale una línea `[DLT]` con
topic, partición, offset y motivo). Y lo importante: **la cola SQS sigue vacía**, porque un
mensaje venenoso no es un fallo de infraestructura. Si ahí no aparece nada en el `.DLT`:

- mira el log group: si el lote no se procesó, `LastProcessingResult` y `State` del mapping;
- `market.ticks.raw.DLT` tiene **1 partición**: si el topic no existía, la Lambda no puede
  escribir y lo verás en el log.

---

## Paso 8 — Teardown, en orden, y comprobar que no queda nada cobrando

Este es el paso que cierra el sprint. El script hace la parte de Terraform y la comprobación
final; los puntos 8.3 y 8.4 son de consola y de tu máquina.

- [ ] `bash deploy/teardown.sh` (enseña el estado, pide confirmación y destruye).
- [ ] Borrar el **clúster gestionado a mano** (lo único que Terraform no creó).
- [ ] Borrar las **credenciales/API keys** del broker y el `client.properties` local.
- [ ] Comprobar en la consola: EC2, Lambda, SQS, S3, SSM **y la página de facturación**.
- [ ] **Qué deberías ver: 0 instancias, 0 funciones, 0 colas, y el broker borrado.**

### 8.1 `terraform destroy` (guiado)

```bash
bash deploy/teardown.sh
```

Hace, en orden: comprueba credenciales; enseña `terraform state list` (lo que va a caer);
pide que escribas `destruir`; ejecuta `terraform destroy` **sin `-auto-approve`** (Terraform
vuelve a pedir su propio `yes`); y al final comprueba con `aws` que no quedan EC2, Lambda ni
colas SQS del proyecto, avisando de lo que queda fuera.

A mano sería exactamente esto:

```bash
cd deploy/terraform
terraform state list
terraform destroy
```

**Si `terraform destroy` falla porque el bucket S3 no está vacío** (`BucketNotEmpty`):
Terraform borra los objetos que él mismo creó, pero no los que aparecieron por otro lado.
Vacíalo y reintenta:

```bash
cd deploy/terraform
aws s3 rm "s3://$(terraform output -raw artifacts_bucket)" --recursive
terraform destroy
```

### 8.2 Lo que Terraform sí destruye

VM y su disco gp3, Lambda y su log group, cola SQS, bucket S3, parámetros SSM, secreto de
Secrets Manager, alarmas, topic SNS, roles y políticas, security group. Todo eso está en el
estado y `destroy` lo borra.

### 8.3 Lo que Terraform **no** destruye: el broker (a mano)

El clúster gestionado, sus topics y sus credenciales los creaste tú en la consola. Es la
línea que más factura y la única que sigue viva después del destroy.

- **Confluent Cloud** `(verificar)` los nombres del menú: borra el **clúster** (los topics se
  van con él); después borra las **API keys** —la del clúster y la del **Schema Registry**,
  que es otra— y el *service account* si lo creaste; si el *environment* era solo para el
  sprint, bórralo también.
- **Redpanda Cloud** `(verificar)`: borra el clúster; topics y usuarios del clúster se van
  con él. Si el proveedor guarda las credenciales aparte, bórralas.
- Si prefieres **conservar** el clúster, como mínimo borra las API keys para que nada pueda
  autenticarse, y borra los topics con el helper del paso 2
  (`topics --delete --topic market.ticks.raw`, y el resto de la tabla).

Y en tu máquina, que también guarda secretos:

```bash
# El state lleva las credenciales de Kafka en claro. Si no lo vas a reutilizar:
rm -f deploy/terraform/terraform.tfstate deploy/terraform/terraform.tfstate.backup
rm -f deploy/terraform/phase10.tfplan deploy/terraform/terraform.tfvars

# El client.properties de los topics:
rm -f /tmp/client.properties

# Cierra la terminal con los 'export TF_VAR_...' (o unset de cada uno).
```

### 8.4 La comprobación final, en la consola

`teardown.sh` ya comprueba EC2, Lambda y SQS por CLI. En la consola, y en esta línea:

| Servicio | Qué debe estar a 0 |
|---|---|
| **EC2 → Instances** | 0 instancias con `Project = aggora` (y 0 volúmenes gp3 huérfanos) |
| **Lambda → Functions** | 0 funciones `aggora-*` |
| **SQS → Queues** | 0 colas `aggora-*` |
| **S3 → Buckets** | 0 buckets `aggora-artifacts-<cuenta>` |
| **SSM → Parameter Store** | 0 parámetros bajo `/aggora/` |
| **CloudWatch → Alarms / Logs** | 0 alarmas `aggora-*`; el log group de la Lambda, borrado |
| **Secrets Manager** | 0 secretos `aggora/lambda-kafka-sasl` |
| **Proveedor de Kafka** | **clúster borrado** y 0 API keys |
| **Billing → Cost Explorer** | la línea de EC2 deja de crecer; el clúster borrado se nota antes que el desglose |

El desglose de facturación **tarda unas horas** en reflejar el destroy; que la EC2 y el
clúster ya no existan se ve antes. Mientras el clúster siga ahí, sigue facturando aunque no
haya nada conectado.

---

## Paso 9 — Qué mirar/aprender en cada paso, y los fallos probables

### 9.1 El porqué, paso a paso

| Paso | Qué aprender (el porqué en dos líneas) |
|---|---|
| 0 | El gasto no lo para un `destroy` bien hecho, lo para saber **qué factura por hora** (VM y broker) y qué factura por uso (Lambda, S3). Un presupuesto avisa; no protege. |
| 1 | AWS reparte permisos por identidad, no por proyecto: la comodidad de `AdministratorAccess` es un atajo consciente con un riesgo real, y el clúster gestionado tiene credenciales **aparte** de AWS. |
| 2 | Kafka es **estado**: el broker se crea a mano porque no se puede recrear sin perder nada. Y sin transacciones, topic admin ni Streams no hay Fase 10: son tres requisitos que se comprueban **antes** de pagar. |
| 3 | Lo que se despliega son **artefactos**, no fuente: el jar gordo de la Lambda es el ejemplo de que "compila" y "funciona en Lambda" son dos cosas distintas (el fino compila y revienta en la primera invocación). |
| 4 | Los secretos van por entorno y **nunca** al repo; el precio es que el *state* de Terraform también es un secreto que hay que cuidar. |
| 5 | Terraform describe **lo recreable**: el plan es un contrato que se lee antes de aplicar, y `apply` devuelve el control antes de que el `user_data` termine (arrancar no es estar listo). |
| 6 | La VM no se toca desde fuera: **cero reglas de entrada** y Session Manager como única puerta. El estado de un servicio de Streams en ERROR se ve en la sonda y el lag, no en el proceso. |
| 7 | En Lambda no eres tú quien consume: **AWS hace el bucle y confirma offsets**, el lote llega en base64 y por eso el veneno va al `.DLT` sin lanzar y solo la infraestructura cae en SQS. |
| 8 | Lo que crea una persona, lo borra una persona: el `destroy` cubre lo recreable, y el clúster (lo que factura por hora) es un paso de consola deliberado. |
| 9 | Desplegar de verdad duele por orden: **credenciales, topics con las mismas particiones y el reloj** (capítulo 21 de `docs/kafka-101.md`). |

### 9.2 Fallos probables: síntoma, causa y arreglo

**1. El broker no resuelve o no autentica (SASL/DNS)**

- **Síntoma:** en `journalctl -u aggora@<servicio>`:
  `Connection to node -1 (...) could not be established. Broker may not be available.`,
  `Authentication failed` o `UnknownHostException`. `LastProcessingResult` del ESM con error
  y la cola SQS empezando a llenarse.
- **Causa:** mecanismo SASL equivocado (`PLAIN` vs `SCRAM-SHA-256`), API key/clave mal
  copiada, la pareja del Schema Registry sin poner (y el SR responde `401`), o la VM sin
  salida a internet / DNS.
- **Arreglo:** comprueba desde la VM que el broker es alcanzable
  (`timeout 5 bash -c '</dev/tcp/HOST/9092' && echo abierto`) y que resuelve
  (`getent hosts HOST`); revisa `kafka_sasl_mechanism` en SSM contra la config del clúster;
  si has rotado una credencial, reescribe el parámetro y vuelve a ejecutar el `user_data`
  (paso 9, fallo 5). Un `Access denied` de SASL no dice qué falta: se depura mirando el
  broker, no la aplicación.

**2. Topics creados con otro número de particiones**

- **Síntoma:** `topics --describe` enseña 3 donde la tabla dice 6; el paralelismo cambia
  (Kafka Streams crea otro número de tareas), el simulador se reparte de otra forma, o
  aparece `MissingSourceTopicException` si además falta el topic.
- **Causa:** se crearon a mano antes de copiar la tabla, o los creó la autocreación del
  broker con 1 partición por defecto.
- **Arreglo:** las particiones **solo se pueden aumentar**, así que para dejarlo igual que
  local hay que **borrar y recrear** el topic (con nada escribiendo en él) y dejar que
  `Restart=always` levante los servicios. Es más rápido que pelearse con un pipeline
  desbalanceado durante días.

**3. La Lambda fuera de la VPC (y por qué se eligió así)**

- **Síntoma (si la metes en la VPC):** timeouts de la Lambda hacia Kafka y hacia Secrets
  Manager, lotes que fallan y cola SQS llenándose.
- **Causa:** una Lambda dentro de una VPC **pierde la salida a internet** salvo que montes
  **NAT Gateway** o endpoints de VPC. El broker gestionado es público, así que dentro de la
  VPC no ganas nada y pierdes la salida.
- **Arreglo:** déjala **fuera de la VPC**, como está en `main.tf` (el *event source mapping*
  solo lleva autenticación: `source_access_configuration`, sin `VPC_SUBNET` ni
  `VPC_SECURITY_GROUP`). Si algún día el broker fuera privado, entonces sí: subred privada +
  NAT o endpoints, y a pagar el NAT por horas.

**4. `min.insync.replicas` del broker gestionado**

- **Síntoma:** el motor de matching (exactly-once, Fase 4) falla al producir:
  `NotEnoughReplicasException` (`(verificar)` el nombre según broker) o un error de factor de
  réplica al crear topics, mientras el resto del pipeline funciona.
- **Causa:** el productor transaccional escribe con `acks=all` y necesita que el número de
  réplicas sincronizadas sea ≥ `min.insync.replicas` (típicamente 2). En un clúster de un
  solo broker, o si creaste los topics con factor de réplica 1, no se cumple.
- **Arreglo:** no fijes factor de réplica al crear los topics (deja el que ponga el proveedor,
  normalmente 3) y usa un tier con al menos 3 brokers. Comprueba el valor por defecto del
  clúster antes de culpar al código: `(verificar)` dónde lo publica tu proveedor.

**5. La VM arranca antes de que existan los parámetros de SSM**

- **Síntoma:** `/etc/aggora/*.env` vacíos o ausentes y servicios en bucle con
  `No resolvable bootstrap urls given in bootstrap.servers`, o el `user_data` cortado en el
  log de cloud-init.
- **Causa:** el `user_data` lee SSM **en el primer minuto de vida**. En el primer `apply`
  no pasa, porque `aws_instance.vm` tiene un `depends_on` que espera a los parámetros, pero
  sí puede pasar si recreas **solo la instancia** (`terraform apply -replace=aws_instance.vm`)
  o si rotas parámetros mientras la VM arranca.
- **Arreglo:** el `user_data` es idempotente; vuelve a ejecutarlo desde la sesión SSM y
  reinicia:

  ```bash
  sudo bash /var/lib/cloud/instance/user-data.txt
  sudo systemctl restart 'aggora@*'
  ```

**6. `t4g.small` se queda sin memoria**

- **Síntoma:** servicios que mueren y vuelven, `journalctl -k` con `oom-kill`, la máquina
  que responde a tirones.
- **Causa:** 7 JVMs + Postgres + Prometheus + Grafana en 2 GB. `JAVA_OPTS` está capado
  (`-Xmx320m` por servicio) precisamente por esto, pero es el suelo, no un ajuste cómodo.
- **Arreglo:** `instance_type = "t4g.medium"` (4 GB, ~el doble de la línea de cómputo) y
  vuelve a aplicar. Cuesta menos que perseguir bugs fantasma.

**7. `starting_position = LATEST` y el canónico no arranca**

- **Síntoma:** `market.ticks.raw` tiene mensajes, pero `market.ticks.canonical` está vacío
  justo después del `apply`.
- **Causa:** el mapping se crea con `LATEST` (está fijado en `main.tf`): solo procesa lo que
  llega **después** de que el mapping existe.
- **Arreglo:** no es un fallo; espera a que el simulador produzca. Si quisieras reprocesar lo
  antiguo habría que recrear el mapping con otra posición inicial (`(verificar)` el
  procedimiento en la consola de Lambda).

**8. Las alarmas no llegan por correo**

- **Síntoma:** saltan alarmas y no llega nada.
- **Causa:** la suscripción de SNS está *Pending confirmation* hasta que se pincha el enlace
  del correo.
- **Arreglo:** `(verificar)` con
  `aws sns list-subscriptions-by-topic --topic-arn <arn>` (el ARN sale de `terraform output`
  o de la consola) y confirma la suscripción.

---

## Paso 10 — Chuleta final (los comandos que de verdad vas a usar)

Desde la raíz del repo, y `cd deploy/terraform` donde diga.

| Para qué | Comando |
|---|---|
| Comprobar sin gastar | `bash deploy/preflight.sh` |
| Credenciales AWS | `aws sts get-caller-identity` |
| Compilar artefactos | `bash deploy/collect-artifacts.sh` |
| Ver los jars de la VM | `ls -1 deploy/artifacts/*.jar \| wc -l` (7) |
| Ver el jar gordo de la Lambda | `ls -lh services/lambda/ingestion-normalizer-lambda/target/*-shaded.jar` |
| Variables (los 5 obligatorios + extras) | `export TF_VAR_kafka_bootstrap_servers=...` … (paso 4) |
| Inicializar | `cd deploy/terraform && terraform init` |
| Planificar | `terraform plan -out=phase10.tfplan` |
| Aplicar | `terraform apply phase10.tfplan` |
| Ver salidas | `terraform output` |
| Datos de la VM | `terraform output -raw instance_id` / `-raw instance_public_ip` |
| Entrar en la VM | `$(terraform output -raw ssm_start_session)` |
| ¿Está el agente? | `aws ssm describe-instance-information --filters "Key=InstanceIds,Values=$(terraform output -raw instance_id)"` |
| Servicios | `systemctl status 'aggora@*' --no-pager` |
| Logs de un servicio | `journalctl -u aggora@analytics-streams -f` |
| Sonda de salud | `curl -s localhost:8085/actuator/health` |
| State store (analítica) | `curl -s 'localhost:8085/analytics?symbol=EUR/USD&minutes=3'` |
| Túnel al gateway | `aws ssm start-session --target "$(terraform output -raw instance_id)" --region eu-west-1 --document-name AWS-StartPortForwardingSession --parameters '{"portNumber":["8089"],"localPortNumber":["8089"]}'` |
| Logs de la Lambda | `aws logs tail /aws/lambda/aggora-ingestion-normalizer --since 15m --follow` |
| Estado del ESM | `aws lambda list-event-source-mappings --function-name "$(terraform output -raw lambda_function_name)"` |
| Cola de fallos | `aws sqs get-queue-attributes --queue-url "$(terraform output -raw sqs_queue_url)" --attribute-names ApproximateNumberOfMessagesVisible` |
| Topics (helper del paso 2) | `topics --list` / `topics --describe --topic market.ticks.canonical` |
| Meter veneno al crudo | `echo 'esto no es un tick' \| docker run --rm -i -v /tmp/client.properties:/client.properties apache/kafka:3.9.0 /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server "$BOOTSTRAP" --producer.config /client.properties --topic market.ticks.raw` |
| Ver el `.DLT` con cabeceras | `docker run --rm -v /tmp/client.properties:/client.properties apache/kafka:3.9.0 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server "$BOOTSTRAP" --consumer.config /client.properties --topic market.ticks.raw.DLT --from-beginning --max-messages 1 --property print.headers=true` |
| Ver el estado de Terraform | `cd deploy/terraform && terraform state list` |
| **Destruir** | `bash deploy/teardown.sh` |
| Vaciar el bucket si el destroy falla | `aws s3 rm "s3://$(terraform output -raw artifacts_bucket)" --recursive` |

---

## Al terminar

- `bash deploy/preflight.sh` no es solo para empezar: si el sprint se alarga, vuelve a
  pasarlo para confirmar que sigues con los pies en el mismo sitio.
- Lo que hayas aprendido (y lo que se rompa) merece acabar en `docs/kafka-101.md` y en
  `deploy/README.md`: este repo trata el despliegue como material del curso, no como un
  trámite. Eso sí, **después** del paso 8.
