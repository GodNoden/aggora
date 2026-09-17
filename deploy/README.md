# Deploying Aggora to AWS (Phase 10)

> **Doing this as a learning sprint?** Start with [`SPRINT-AWS.md`](SPRINT-AWS.md) (Spanish): an
> operational guide, step 0 to 10, with the budget alarm, the exact commands, the verification of
> every piece and the teardown that leaves nothing billing. Run `bash deploy/preflight.sh` before
> spending anything: it checks the local side (terraform, jars, TF_VARs, credentials) and tells you
> what is missing.

This is the runbook for the Phase 10 deployment: the same pipeline that runs on a laptop
(three KRaft brokers in Docker, seven JVMs in a devcontainer) running against a **managed
Kafka broker** on real infrastructure.

The design and the reasoning live in the repo, not here. Read
[`docs/kafka-101.md`](../docs/kafka-101.md) chapters 18, 19 and 21 before changing
anything in this directory: this file is the *how*, those chapters are the *why*.

## The shape of it

| Piece | Where it runs | Why there |
|---|---|---|
| Kafka broker + Schema Registry | A managed provider (Confluent Cloud, Redpanda Cloud), **created by hand in the console** | It bills by the hour and holds state. Terraform is for resources you can destroy and recreate; a broker is not one of them |
| The three Kafka Streams services (`analytics-streams`, `portfolio-risk`, `alerting-service`) | One small always-on VM, under `systemd` | State stores live on local disk, rebalancing needs a live process, and exactly-once holds a transaction open. Serverless is not an option for these |
| The rest of the pipeline (`market-data-simulator`, `order-matching-engine`, `audit-log`, `gateway-ws`) | The same VM, under `systemd` | Same tool, one template unit, and the whole pipeline stays in one place |
| `ingestion-normalizer` | **AWS Lambda** with a Kafka event source mapping | The only truly stateless service. Lambda owns the consumer loop, invokes with a pre-read batch, and commits the offsets itself |
| Postgres, Prometheus, Grafana, kafka-exporter | The same VM, in Docker (`deploy/docker-compose.vm.yml`) | State that is not Kafka, plus observability. No Kafka and no Schema Registry in there: those are managed |

**No EKS.** The control plane is billed hourly even with zero nodes, and Kubernetes would
only add IAM, pod networking, ingress and storage to learn for a case that is "always-on
processes plus one Lambda". The one thing it would buy — a consumer that scales to zero on
lag — is KEDA on a cheap k3s if that ever becomes the goal.

## What is infrastructure-as-code, and what is not

| Thing | Created by | Why |
|---|---|---|
| Managed broker + Schema Registry | **Hand**, provider console | Terraform must not be able to create something that starts billing by the hour and cannot be safely destroyed |
| Topics (names, partitions, cleanup policy) | **Hand**, `kafka-topics.sh` | They must match local exactly, and creating them up front removes the Streams "source topic does not exist yet" race instead of relying on it |
| Kafka/Schema Registry credentials | Terraform writes them to **SSM Parameter Store** (`SecureString`) | The app reads them at boot; they never live in the repository |
| S3 bucket, jars/config objects, IAM roles, VM, Lambda, SQS queue, alarms, SNS topic | **Terraform** | Destroyable and recreatable with no data loss |
| The 20 GB gp3 root disk | **Terraform** (`root_block_device`, encrypted) | Same |
| Jars | `deploy/collect-artifacts.sh`, uploaded by Terraform | Build artifacts, not infrastructure |
| App config on the VM (`/etc/aggora/*.env`, unit file) | `deploy/user-data.sh` from the repo files shipped in S3 | The VM has no checkout of the repo, and the repo must stay the single source of truth |

## Prerequisites

- An AWS account and credentials with permission to create the resources above.
- Terraform >= 1.6, or Docker (the verification below uses the official
  `hashicorp/terraform:1.9` image).
- The devcontainer running (`docker ps`), or Maven + JDK 21 on the host, to build the jars.
- An account at a Kafka provider that supports **transactions**, **topic admin** and
  **Kafka Streams**. Brokers that speak HTTP instead of the Kafka protocol do not count.

## 1. Create the managed broker by hand

In the provider console, in **eu-west-1** (keep it next to the VM), create the cluster and
the Schema Registry. Before you commit to a provider, confirm all three:

1. **Transactions** — without them the exactly-once matching engine (Phase 4) does not work.
2. **Topic admin** — you need to create topics and set partitions from a client.
3. **Kafka Streams** — three of the seven services are Streams topologies; a broker that
   only does produce/consume is useless here.

Write down four values; the next steps need them:

| Value | Confluent Cloud | Redpanda Cloud |
|---|---|---|
| Bootstrap servers | Cluster → Endpoint (`pkc-xxxxx.eu-west-1.aws.confluent.cloud:9092`) | Cluster → Bootstrap server |
| Schema Registry URL | Stream Governance endpoint (`https://psrc-xxxxx...`) | Schema Registry URL |
| SASL username | Cluster API key | Cluster user |
| SASL password | Cluster API secret | Cluster password |

Two gotchas worth knowing before you start:

- **SASL mechanism.** Confluent Cloud uses `PLAIN` (the API key is the username). Redpanda
  Cloud uses `SCRAM-SHA-256`. The Terraform variable `kafka_sasl_mechanism` and the
  `source_access_configuration` type of the Lambda event source mapping both depend on it.
- **The Schema Registry has its own credentials on Confluent Cloud.** It is a separate API
  key from the cluster key. Pass it as `kafka_sr_username` / `kafka_sr_password`; if you
  leave them empty, no basic auth is written to the VM config.

## 2. Create the topics

Same names, same partitions and same cleanup policy as local. This table is the contract:

| Topic | Partitions | `cleanup.policy` | Written by |
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

Do **not** set a replication factor: let the managed broker apply its default (normally 3).
Kafka Streams creates its own internal changelog and repartition topics itself, so leave
auto topic creation as the provider ships it.

A provider-agnostic way to create them, using the same Kafka distribution as local:

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

For Redpanda Cloud, swap the `sasl.mechanism` line to `SCRAM-SHA-256` and use the SCRAM
login module in `sasl.jaas.config`:
`org.apache.kafka.common.security.scram.ScramLoginModule required username="..." password="...";`

The `topics()` helper lives only in the shell that defines it; redefine it in the
verification step below (or do everything in one terminal).

## 3. Build the artifacts

```bash
bash deploy/collect-artifacts.sh
```

This builds `services/` (in the devcontainer when one is running, otherwise with local
Maven) and copies the seven Spring jars into `deploy/artifacts/` with the exact name the
VM expects: `<service>-spring-0.1.0-SNAPSHOT.jar`. `deploy/artifacts/` is git-ignored.

It also reminds you about the Lambda fat jar, which Terraform needs at
`services/lambda/ingestion-normalizer-lambda/target/ingestion-normalizer-lambda-0.1.0-SNAPSHOT-shaded.jar`

That is the **shaded** jar (the fat one, ~34 MB, with every dependency inside). The file
without `-shaded` is the thin jar and Lambda would fail on the first invocation with a
`ClassNotFoundException`: it has no dependencies in it. `lambda_jar_path` overrides the path.

## 4. Terraform

Secrets go in through the environment, not through `terraform.tfvars`, so they do not end
up in your shell history or in the repository:

```bash
cd deploy/terraform

export TF_VAR_kafka_bootstrap_servers="pkc-xxxxx.eu-west-1.aws.confluent.cloud:9092"
export TF_VAR_kafka_schema_registry_url="https://psrc-xxxxx.eu-west-1.aws.confluent.cloud"
export TF_VAR_kafka_sasl_username="..."
export TF_VAR_kafka_sasl_password="..."
export TF_VAR_postgres_password="$(openssl rand -hex 16)"
# Confluent Cloud only: the Schema Registry has its own API key.
# export TF_VAR_kafka_sr_username="..."
# export TF_VAR_kafka_sr_password="..."

# Non-secret settings: variables.tf already has good defaults for these.
export TF_VAR_kafka_sasl_mechanism="PLAIN"
export TF_VAR_alarm_email="you@example.com"

terraform init
terraform plan -out=phase10.tfplan
terraform apply phase10.tfplan
```

The plan **fails on purpose** if `deploy/artifacts/` is empty: better a clear error than a
VM that boots with no jars.

> **The Terraform state contains secrets.** The SSM parameter values and the Lambda
> environment variables include the Kafka credentials. `deploy/terraform/.gitignore` keeps
> `*.tfstate`, `terraform.tfvars` and `.terraform/` out of git; for anything long-lived move
> the state to an encrypted remote backend.

## 5. Credentials in SSM Parameter Store

Terraform already created the parameters from the `TF_VAR_...` values in step 4, under
`/aggora/`:

| Parameter | Type | Read by |
|---|---|---|
| `/aggora/kafka/bootstrap-servers` | SecureString | `common.env` |
| `/aggora/kafka/schema-registry-url` | SecureString | `common.env` |
| `/aggora/kafka/sasl-username` | SecureString | `common.env` |
| `/aggora/kafka/sasl-password` | SecureString | `common.env` |
| `/aggora/kafka/sasl-mechanism` | String | `common.env` |
| `/aggora/kafka/schema-registry-username` | SecureString (optional) | `common.env` |
| `/aggora/kafka/schema-registry-password` | SecureString (optional) | `common.env` |
| `/aggora/postgres/password` | SecureString | `audit-log.env`, compose `.env` |
| `/aggora/topics/lambda-source` | SecureString | runbook / Lambda variable |
| `/aggora/deploy/artifacts-bucket` | String | bucket name for `user_data` |

Verify:

```bash
aws ssm get-parameter --name /aggora/kafka/bootstrap-servers --with-decryption \
  --query Parameter.Value --output text
```

The VM reads these once, at boot, from `deploy/user-data.sh`. **Rotating a credential is
therefore two steps**: overwrite the parameter and restart the readers.

```bash
aws ssm put-parameter --name /aggora/kafka/sasl-password --type SecureString \
  --value 'NEW_SECRET' --overwrite

# On the VM (via SSM session): re-run user-data's env-file step and restart.
sudo bash /var/lib/cloud/instance/user-data.txt   # idempotent: rewrites the env files
sudo systemctl restart 'aggora@*'
```

If you would rather keep secrets **out of the Terraform state entirely**, delete the
`aws_ssm_parameter` resources for the credentials and create those parameters with
`aws ssm put-parameter` before the first `apply`. The tradeoff is real: you trade
state exposure for a manual step that can drift. Phase 10 chose Terraform-managed
parameters because the whole point of the exercise is that `user_data` reads them at boot
without a human in the loop.

## 6. Verify

```bash
cd deploy/terraform
terraform output
```

Then open a shell on the VM (there is **no inbound port open**; SSM Session Manager is the
only door):

```bash
$(terraform output -raw ssm_start_session)

# On the VM:
systemctl status 'aggora@*' --no-pager
journalctl -u aggora@analytics-streams -n 50 --no-pager
journalctl -u aggora@gateway-ws -n 50 --no-pager
```

A service showing `activating`/`failed` and coming back every 15 s is the topic race being
retried, not a bug: wait for one full `RestartSec` cycle and look again.

**The pipeline is alive** when the canonical topic is moving:

```bash
topics --describe --topic market.ticks.canonical
# or watch the lag per group in Grafana -> Kafka dashboard
```

**The Streams state store answers** (analytics-streams is the one with a query endpoint and
an HTTP health probe):

```bash
curl -s localhost:8085/actuator/health
curl -s 'localhost:8085/analytics?symbol=EUR/USD&minutes=3'
curl -s localhost:8085/arbitrage | head -c 300
```

The second one reads the queryable state store; if it returns data, the Streams engine is
not just alive, it is *working* — which is exactly what a process-alive alarm cannot tell
you.

**The gateway serves the live feed** (WebSocket + a plain page). Nothing is open to the
internet, so tunnel it:

```bash
# Leave the SSM session first (`exit`), then run this on your own machine, where the
# terraform outputs are:
aws ssm start-session --target "$(terraform output -raw instance_id)" --region eu-west-1 \
  --document-name AWS-StartPortForwardingSession \
  --parameters '{"portNumber":["8089"],"localPortNumber":["8089"]}'

curl -s localhost:8089 | head -c 200
```

**The Lambda is consuming** (CloudWatch Logs, and the event source mapping state):

```bash
aws logs tail /aws/lambda/aggora-ingestion-normalizer --since 15m --follow
aws lambda list-event-source-mappings \
  --function-name "$(terraform output -raw lambda_function_name)" \
  --query 'EventSourceMappings[].{State:State,Last:LastProcessingResult}' --output table
```

**Nothing is silently piling up** in the failure queue:

```bash
aws sqs get-queue-attributes --queue-url "$(terraform output -raw sqs_queue_url)" \
  --attribute-names ApproximateNumberOfMessagesVisible
```

## 7. What NOT to do

- **Never run the Spring and Quarkus implementations against the same output topics at the
  same time.** If they share a `group.id`, they split the partitions and each message is
  processed by only one of them; if they share output topics, they interleave incompatible
  data. To compare the two stacks: give Quarkus its own `group.id` and its own output topics
  (`market.analytics.quarkus`), or run one at a time.
- **Never share a `group.id` between two services that are supposed to both see everything.**
  A consumer group divides the work; it does not copy it. `gateway-ws` has its own group for
  exactly this reason.
- **Never let Terraform manage the broker or the topics.** The broker bills by the hour and
  holds state; deleting it should be a deliberate console action, not a `terraform destroy`
  side effect.
- **Never open an inbound port** to reach Grafana, the gateway or `/actuator/health`. Use SSM
  port forwarding, as above. The security group has zero ingress rules on purpose.
- **Never commit `terraform.tfstate`, `terraform.tfvars` or a `.env`.** They contain the
  Kafka credentials.
- **Do not scale a Streams service by copying it to a second VM with the same state
  directory.** Kafka Streams scales by instance, not by volume: a second instance needs its
  own `state.dir` (this deployment already uses `/var/lib/aggora/streams/<service>`), and to
  truly scale out each one needs its own disk.
- **Do not alarm on "the process is alive" for a Streams service.** See the note at the
  bottom of `deploy/terraform/main.tf`: a Streams engine stuck in `ERROR` keeps the process
  running and CPU normal, so EC2 status checks stay green while it processes nothing.

## 8. Costs

These are **estimates**, not a quote, and prices change. Every number was read on
**2026-09-16** from the source in the last column (the AWS figures come from the official
Price List API for **eu-west-1**, which is the same data the pricing pages render). Check
the AWS Pricing Calculator and your provider's calculator before you commit.

| Line | Estimate / month | Basis | Source (checked 2026-09-16) |
|---|---|---|---|
| EC2 `t4g.small`, on-demand, 24/7 | **~$13.43** (`$0.0184/h`) | 2 vCPU / 2 GB, eu-west-1. A 1-year no-upfront Savings Plan is `$0.0151/h` (~$11.02) | [Holori, t4g.small eu-west-1](https://calculator.holori.com/aws/ec2/t4g.small?region=eu-west-1) |
| EBS gp3 root, 20 GB | **~$1.76** (`$0.088/GB-mo`) | Includes 3,000 IOPS and 125 MiBps; a bigger disk or more IOPS costs more (`$0.006`/IOPS-mo, `$0.044`/MiBps-mo) | [AWS EC2 Price List API, eu-west-1](https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonEC2/current/eu-west-1/index.json) |
| Lambda, at ~60 msg/s | **~$52 to ~$130** | 15.6M invocations/month (batch 10), 1 GB memory, 200–500 ms each. Requests alone are only ~$3.12; the GB-seconds dominate | [AWS Lambda Price List API, eu-west-1](https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AWSLambda/current/eu-west-1/index.json) (`$0.0000166667/GB-s`, `$0.20/M requests`) |
| CloudWatch Logs (Lambda) | **~$5 to ~$15** | ~1 KB per invocation over 15.6M invocations ≈ 15 GB ingested at `$0.57/GB` | [AWS CloudWatch Price List API, eu-west-1](https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonCloudWatch/current/eu-west-1/index.json) |
| CloudWatch alarms (3) | **~$0.30** | `$0.10` per alarm-month | same CloudWatch price list |
| Secrets Manager (1 secret) | **~$0.40** + negligible API calls | `$0.40` per secret-month | [AWS Secrets Manager pricing](https://aws.amazon.com/secrets-manager/pricing/) |
| SQS (failure queue) | **< $0.01** | `$0.40`/million requests; only failures land here | [AWS SQS Price List API, eu-west-1](https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AWSQueueService/current/eu-west-1/index.json) |
| S3 (jars + config, a few hundred MB) | **< $0.01** | `$0.023/GB-mo` standard storage; same-region transfer is free | [AWS S3 Price List API, eu-west-1](https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonS3/current/eu-west-1/index.json) |
| SSM Parameter Store | **$0.00** | Standard parameters (including `SecureString`) are free | [AWS Systems Manager pricing](https://aws.amazon.com/systems-manager/pricing/) |
| **Managed broker + Schema Registry** | **the big unknown** | Billed hourly whether the pipeline processes or not. Both providers have free/basic entry tiers, and above that it is the single largest line on this bill | [Confluent Cloud pricing](https://www.confluent.io/confluent-cloud/pricing/), [Redpanda pricing](https://www.redpanda.com/pricing) |

The honest summary of the AWS side, excluding the broker: **roughly $16/month if the Lambda
is idle, and roughly $73–160/month once the 60 msg/s feed is running**, almost all of it
Lambda compute and its logs. That is the phase-19 point made with a number: a serverless
consumer is a poor fit for a constant stream. If the feed ever matters more than the
exercise, move `ingestion-normalizer` onto the VM with the other seven — it is stateless, so
it is a `systemd` unit away, and it would cost a few hundred MB of RAM instead of $50–130.

Two more cost notes:

- **`t4g.small` is the floor, not a comfortable fit.** Seven JVMs plus Postgres, Prometheus
  and Grafana in 2 GB will swap. If `systemctl status` shows OOM kills or the machine
  thrashes, set `instance_type = "t4g.medium"` (4 GB, roughly double the compute line) before
  you start chasing phantom bugs in the pipeline. `JAVA_OPTS` is capped per unit precisely
  because of this.
- **The broker bill does not take holidays.** It is the one line that keeps running when
  everything else is quiet, which is why it is manual and visible in the console.

## What has been verified, and what has not

Verified in this repository, with the commands and output recorded in the phase report:

- `terraform fmt -check -recursive`, `terraform init -backend=false` and `terraform validate`
  over `deploy/terraform/`;
- `docker compose config -q` over `deploy/docker-compose.vm.yml`, both bare and with a sample
  `--env-file` (to prove the credentials interpolate);
- `deploy/systemd/aggora@.service` passes `systemd-analyze verify` and the health probe in
  `ExecStartPost` was exercised with `HEALTH_URL` unset (exit 0) and pointing at a dead port
  (exit 1);
- the jar names `deploy/user-data.sh` expects follow `<artifactId>-<version>` of each Spring
  module (`market-data-simulator` → `market-data-simulator-spring-0.1.0-SNAPSHOT.jar`).

Run the Terraform checks with the **repository root** mounted, not just `deploy/terraform/`:
this module reads `../user-data.sh`, `../artifacts/*.jar` and `../../infra/**` on purpose, so a
container that only sees `deploy/terraform/` cannot resolve them and `validate` fails with
`no file exists at "./../user-data.sh"`. That is a mount-scope error, not a config error.

```bash
cd deploy/terraform
docker run --rm -v /path/to/aggora:/w -w /w/deploy/terraform hashicorp/terraform:1.9 \
  fmt -check -recursive
docker run --rm -v /path/to/aggora:/w -w /w/deploy/terraform hashicorp/terraform:1.9 \
  init -backend=false -input=false
docker run --rm -v /path/to/aggora:/w -w /w/deploy/terraform hashicorp/terraform:1.9 validate

cd ..   # the compose mounts ../infra, so run it from deploy/ with the repo checkout present
docker compose -f docker-compose.vm.yml config -q
```

**Not** verified, because there is no AWS account or credentials here and deploying costs
money nobody approved: that the network and the IAM permissions actually let the VM talk to
the broker, that the Lambda receives batches from the event source mapping, and that the
alarms fire. That is what the "Verify" section above is for.
