# Aggora — Global Markets Event Platform

**A project for actually learning Kafka: not with slides, but by building a market-data
platform live and measuring what happens.**

Aggora ingests real equity and FX prices, normalises them, computes windowed analytics,
matches orders in an order book, tracks every account's portfolio, raises alerts and audits
the whole thing. Every piece exists to exercise **one specific Kafka concept**, and every
claim in this repository is verified against the running cluster.

The market data is **real** (two free providers). So are the failures: `docs/decisions.md`
and `docs/kafka-101.md` record the problems that showed up, why they showed up and how they
were fixed.

---

## What is demonstrated here?

| Kafka concept | Where it lives in the project |
| --- | --- |
| Topics, partitions, keys and per-partition ordering | `market.ticks.raw` with 6 partitions and `key = symbol` |
| Manual offset commit and at-least-once | `ingestion-normalizer` (`AckMode.MANUAL`) |
| Consumer groups and rebalancing | Two normalizer instances split the 3/3 partitions |
| Avro + Schema Registry and compatibility | 12 schemas in `services/schemas/`, a real compatible change (`v2` of `Order`/`Execution`) |
| Schema evolution without breaking consumers | `scripts/schema-evolution-lab.sh`: measured BACKWARD and FORWARD verdicts, a real `409` rejection and the three fixes |
| Kafka Streams with the plain API | Tumbling and hopping windows, state stores, stream-stream and stream-GlobalKTable joins |
| Interactive queries into state | `GET /analytics?symbol=...` reads the live state store |
| End-to-end exactly-once | Transactional producer + `isolation.level=read_committed` |
| Dead-letter topics and retries | `.DLT` on two consumers + `@RetryableTopic` with `.retry-*` topics |
| Transactional outbox | Postgres and the compacted `audit.events` topic |
| Replicas, ISR and `min.insync.replicas` | 3-broker KRaft cluster with 3 replicas per partition |
| Operations and observability | kafka-exporter + Prometheus + Grafana, provisioned from this repository; app metrics from actuator/micrometer and a health probe that goes DOWN when the engine is dead |
| Fan-out: the same topic, one copy per consumer group | `gateway-ws` keeps its own group on three topics and pushes every record to every open browser |
| Measuring the two implementations instead of arguing | `scripts/throughput-test.sh` pushes both pipelines at once (up to 47x the simulator) and reports throughput, peak lag per consumer group and **CPU per message** read from `/proc`: [docs/throughput-lab.md](docs/throughput-lab.md) |
| Deploying it: what can be serverless and what cannot | `deploy/README.md`: managed broker, one always-on VM for the stateful services, `ingestion-normalizer` on Lambda with an event source mapping |

---

## Architecture

```
   Twelve Data (US, FX, gold, ETFs)       ┐
   Alpha Vantage (Euronext, Shanghai)     ┘
                    │  REST polling + a random walk to add volume
                    ▼
        ┌───────────────────────┐        ┌──────────────────────┐
        │ market-data-simulator │───────►│   market.ticks.raw   │
        │      (HTTP 8080)      │        │  6 partitions        │
        └───────────┬───────────┘        └──────────┬───────────┘
                    │ simulated orders              │  Avro
                    ▼                               ▼
        ┌───────────────────────┐        ┌──────────────────────────┐
        │   orders.incoming     │        │  ingestion-normalizer    │
        └───────────┬───────────┘        │  manual commit, validates│
                    │                    │  republishes canonical   │
                    │                    └────┬───────────────┬─────┘
                    │                        │               │ invalid → .DLT
                    │              market.ticks.canonical    │
                    │                        │               │
                    │                        ▼               ▼
                    │            ┌────────────────────┐  market.fx.reference
                    │            │  analytics-streams │  (compacted)
                    │            │  windows · joins   │
                    │            │    HTTP 8085       │
                    │            └───┬────────────┬───┘
                    │                ▼            ▼
                    │        market.analytics  market.arbitrage
                    │                              (ASML NASDAQ vs AMS)
                    ▼
        ┌───────────────────────┐
        │ order-matching-engine │  per-instrument book, price-time priority
        │  exactly-once · .DLT  │
        └───────────┬───────────┘
                    ▼
            orders.executions
                    │
      ┌─────────────┼──────────────────┬────────────────────┐
      ▼             ▼                  ▼                    ▼
┌───────────────┐ ┌────────────────┐ ┌──────────────┐ ┌──────────────┐
│ portfolio-risk│ │alerting-service│ │  audit-log   │ │  (grafana /  │
│ KTable of     │ │ 3 rules +      │ │ outbox in    │ │ prometheus)  │
│ positions     │ │ punctuator     │ │ Postgres     │ │              │
└───────┬───────┘ └───────┬────────┘ └──────┬───────┘ └──────────────┘
        ▼                 ▼                 ▼
 portfolio.updates   alerts.raised     audit.events
                                      (compacted)
                    │            │
                    └────────────┴──► gateway-ws · fan-out · ws://localhost:8089/ws
                                      (its own consumer group: it gets its own
                                       copy of every record instead of stealing
                                       partitions from the services above)
```

Eight Spring Boot services (the same eight exist again in Quarkus), two compacted
topics, a three-broker cluster and an infrastructure that comes up with a single
command.

---

## Stack

| Piece | Version | Why |
| --- | --- | --- |
| Kafka (KRaft mode, no ZooKeeper) | `apache/kafka:3.9.0` | Plain Apache broker, no Confluent dependencies |
| Schema Registry | `confluentinc/cp-schema-registry:8.3.1` | Aligned with the Avro library and the serializer |
| Java | 21 (LTS) | Same language for the Spring version and the future Quarkus one |
| Spring Boot | 4.1.1 | Brings the Jackson 3 migration and `spring-boot-starter-kafka` |
| Kafka Streams | Plain API (`Topology`) | The point is to learn the API, not to hide it |
| Postgres | `postgres:16-alpine` | Transactional outbox pattern |
| Observability | kafka-exporter + Prometheus + Grafana | Dashboard provisioned from the repo, no clicking |
| Build | Maven multi-module | An aggregator in `services/` with one parent per implementation (`spring/` today, `quarkus/` in phase 8) |

---

## How to run it

Requirements: Docker (or Docker Desktop with WSL2) and the repository devcontainer, which is
where Java and Maven live. **The infrastructure runs outside the devcontainer**, on the
shared `aggora-net` network.

```bash
# 0) Infrastructure: 3 KRaft brokers, Schema Registry, Postgres, Prometheus, Grafana
docker network create aggora-net 2>/dev/null || true
docker compose -f infra/docker-compose.yml up -d

# 1) Build (inside the devcontainer)
cd /workspaces/aggora/services
mvn -q -DskipTests package

# 2) Start the seven services in order, waiting for each one to be ready
export TWELVEDATA_API_KEY=...      # US, FX, gold and ETFs
export ALPHAVANTAGE_API_KEY=...    # Euronext and Shanghai (free tier: 25 requests/day)
cd /workspaces/aggora
bash scripts/start-services.sh     # logs in /tmp/<service>.log
bash scripts/stop-services.sh      # stop them all

# 3) Quick check
curl -s localhost:8081/subjects                              # the registered contracts
curl -s 'localhost:8085/analytics?symbol=EUR/USD&minutes=3'   # query the state store
java scripts/GatewayLiveCheck.java localhost 8089 12          # live feed, end to end
open http://localhost:3000/d/aggora-kafka                     # the Kafka dashboard in Grafana
open http://localhost:8089/                                   # the live feed demo page
open http://localhost:8189/                                   # the same page, Quarkus version
```

`gateway-ws` is the only service that exists **twice at once on purpose**: the Spring
one (port `8089`) and the Quarkus one (port `8189`) read the same three topics with
different consumer groups, so both pages show the same positions and alerts side by
side.

The API keys are optional: without them the simulator still runs, just with synthetic
prices. They are **never written to the repository**; they are read from environment
variables.

- Kafka (from outside Docker): `localhost:29092`, `29093`, `29094`
- Schema Registry: `localhost:8081` · Postgres: `localhost:5432` · Prometheus: `localhost:9090`
- Grafana: `localhost:3000` (dashboard **Aggora — Kafka**, anonymous access in dev) · raw metrics: `localhost:9308`

---

## Project status

| Phase | Content | Status |
| --- | --- | --- |
| 0 | Local infrastructure in Docker (KRaft + `aggora-net`) | ✅ |
| 1 | Simulator and normalizer; manual commit, rebalancing and at-least-once | ✅ |
| 2 | Avro and Schema Registry end to end; explicit topics | ✅ |
| 3 | Kafka Streams: windows, state, joins and interactive queries | ✅ |
| 4 | Order matching engine with transactional exactly-once | ✅ |
| 5 | Portfolio, alerts and audit with a transactional outbox | ✅ |
| 6 | Resilience and operations: DLT, retries, 3 brokers, Grafana | ✅ |
| 7 | Schema evolution lab: what breaks, how it is caught and how it is fixed | ✅ |
| 8 | Port of the services to Quarkus + GraalVM native image | ✅ (all 7 services ported and measured; native image pending, recipe in `scripts/build-native.sh`) |
| 9 | [Spring vs Quarkus comparison report](SPRING_VS_QUARKUS.md), with numbers | ✅ |
| 9 | `gateway-ws` in both implementations: the live WebSocket feed | ✅ |
| 10 | Deployment: managed broker, one always-on VM, and the stateless service on Lambda | ✅ artifacts (validated, not deployed: no AWS account in this repo) |

**Verified live, not in theory:** 41 unit tests green, 3 brokers with a KRaft quorum and 3
replicas per partition (with two brokers down, writes stop with `NOT_ENOUGH_REPLICAS`
instead of losing data), exactly-once measured on the same topic (`read_committed` 121
messages versus `read_uncommitted` 168), all six schema compatibility verdicts measured
against the registry in both directions, and 131,932 audited events that survived a complete
rebuild of the environment.

---

## Repository layout

```
README.md                  project front page
SPEC.md                    the original spec (immutable)
CONTRIBUTING.md            working rules, conventions and the phase-by-phase log
docs/kafka-101.md          the Kafka concepts in plain language (21 chapters)
docs/dashboard.md          the dashboard contract: panels, metrics and how to publish it
docs/CONTRACT.md           short copy-paste contract for the dashboard repository
docs/START-ANGULAR-CHAT.md the kickoff prompt for the Angular dashboard chat
docs/curso-aggora.md       the guided course: domain, Kafka, phase by phase, file by file, labs and interview questions (Spanish)
docs/decisions.md          decision log and the deliberate deviations from the spec
docs/dashboard.md          the dashboard contract: WebSocket snapshot, metrics catalog, CORS, the five labs and how to publish it behind TLS
docs/CONTRACT.md           the same contract, short and copy-pasteable for the dashboard repository
docs/schema-evolution-lab.md  the schema evolution lab manual
docs/throughput-lab.md     the throughput stress test: method, numbers and what it uncovered
infra/                     docker-compose, Prometheus, Grafana dashboard
scripts/                   start/stop, the schema evolution lab and the measurement of startup/RSS
services/                  Maven aggregator: the platform in both implementations
  schemas/                 the 12 Avro contracts (.avsc) the classes are generated from
  spring/                  Spring Boot implementation (phase 1)
    market-data-simulator/ real prices + simulated orders
    ingestion-normalizer/  validation and the canonical event
    analytics-streams/     windows, joins and interactive queries
    order-matching-engine/ order book and exactly-once
    portfolio-risk/        positions and P&L per account
    alerting-service/      anomaly rules
    audit-log/             transactional outbox
    gateway-ws/            live WebSocket feed for browsers (the fan-out)
  quarkus/                 the same eight services, ported in phase 8
    .../                   same names as above, plus the native-image recipe
  lambda/                  the stateless service as a Lambda (phase 10)
deploy/                    runbook, AWS sprint guide, preflight/teardown, Terraform, systemd units and the VM compose
```

The eight services exist **twice** (`services/spring/*` and `services/quarkus/*`) on
purpose: same topics, same contracts, two frameworks, so the comparison is measured
instead of argued. See [SPRING_VS_QUARKUS.md](SPRING_VS_QUARKUS.md).

---

## Documentation

The deep-dive docs are written in **Spanish** — they are the author's learning material, and
the reason the project exists. The README and the whole commit history are in English.

- **[`docs/curso-aggora.md`](docs/curso-aggora.md)** — the course: the market domain and Kafka
  from zero, then the project phase by phase and file by file, with the problems, the labs and
  the interview questions. Start here if you are learning; use the rest as reference.
- **[`docs/kafka-101.md`](docs/kafka-101.md)** — the concepts: what a topic, a partition, an
  offset, a rebalance or a transaction is, explained with examples from this project and
  assuming no prior knowledge.
- **[`docs/decisions.md`](docs/decisions.md)** — why each decision was made, including the
  deliberate deviations from the spec and the mistakes that cost time.
- **[`docs/dashboard.md`](docs/dashboard.md)** — the read-only contract for the visual dashboard:
  the aggregated WebSocket snapshot, the closed metrics catalog, CORS, the five lab scripts and
  how to publish it behind TLS. The short, copy-pasteable version for the dashboard repository is
  **[`docs/CONTRACT.md`](docs/CONTRACT.md)**.
- **[`docs/schema-evolution-lab.md`](docs/schema-evolution-lab.md)** — what can be changed in
  a contract that is already running without breaking anyone, with the verdicts the registry
  returned and the playbook for doing it in production.
- **[`docs/interview-notes.md`](docs/interview-notes.md)** — the phases read as interview
  questions, each answer with the measured number that backs it.
- **[`SPRING_VS_QUARKUS.md`](SPRING_VS_QUARKUS.md)** — the comparison report: startup and memory
  per service, imperative vs reactive, the gotchas and an honest recommendation.
- **[`CONTRIBUTING.md`](CONTRIBUTING.md)** — how work happens in this repo, the network and
  naming conventions, and the verification detail for every phase.

---

## What this project is not

It is not a production-ready trading platform: there is no authentication, no real order
execution and no connection to a real market, and the credentials you will find are for
development. It is a **laboratory** whose goal is to understand Kafka by measuring its
behaviour, failure modes included.
