# Project Spec: Global Markets Event Platform (Kafka Deep-Dive)

## 1. Purpose of This Document

This is a build spec for an AI coding agent (or a human developer) to implement a
**non-trivial, production-shaped backend system** whose real purpose is to force
hands-on mastery of Apache Kafka's core and advanced concepts. The domain is
**global financial markets and macroeconomics** (equities, FX, commodities, rates,
inflation prints) — explicitly **not** a personal finance / budgeting app.

The project must be implemented **twice**, in two phases, against the *same*
functional spec:

* **Phase A — Spring Boot** (Spring Kafka + Spring Cloud Stream / Kafka Streams binder)
* **Phase B — Quarkus** (SmallRye Reactive Messaging + Quarkus Kafka Streams extension,
  including a native-image build)

The end goal is a working system **and** a written comparison of the two frameworks.

***

## 2. Kafka Concepts This Project Must Exercise

Do not consider the project "done" until every item below has a corresponding,
working piece of code — not just a mention in a README:

* \[ ] Topics, partitions, and keys — deliberate partitioning strategy (not just default)
* \[ ] Producers: sync vs async sends, delivery guarantees, idempotent producer
* \[ ] Consumers: consumer groups, partition assignment, rebalancing behavior
* \[ ] Offset management: auto-commit vs manual commit, at-least-once vs exactly-once
* \[ ] Exactly-once semantics (transactional producer + `read_committed` consumers)
* \[ ] Kafka Streams: `KStream`, `KTable`, stateful aggregations, windowing (tumbling,
  hopping, session windows), stream-stream and stream-table joins
* \[ ] State stores + interactive queries (query a running Streams app's local state)
* \[ ] Schema Registry with Avro or Protobuf, including a **breaking + non-breaking**
  schema evolution exercise (compatibility modes: BACKWARD, FORWARD, FULL)
* \[ ] Dead-letter topics and structured error handling / retry topics
* \[ ] Compacted topics (used for the audit/portfolio-state topic)
* \[ ] Transactional outbox pattern (service writes to its DB and Kafka atomically)
* \[ ] Multi-broker cluster behavior: replication factor, min.insync.replicas,
  simulating a broker failure and observing consumer behavior
* \[ ] Observability: consumer lag, Kafka UI (e.g. Redpanda Console or AKHQ),
  Prometheus/Grafana dashboards for topic throughput and lag
* \[ ] (Stretch) Kafka Connect: a sink connector to Elasticsearch or Postgres
* \[ ] (Stretch) ksqlDB for ad-hoc stream queries
* \[ ] (Stretch) MirrorMaker 2 to simulate a multi-region exchange setup

***

## 3. Domain Concept

**"Aggora"** — a simulated global markets data & trading platform. It ingests
market activity across multiple real exchanges and asset classes, processes it
through streaming analytics, and simulates order execution and portfolio risk in
real time. Real reference data is used for realism; high-frequency tick volume is
synthesized on top of it (see Section 3.3) since real-time raw exchange feeds are
commercial, licensed products that are not viable for a personal project.

### 3.1 Data sources

* **Equities**: real tickers from three real exchanges, chosen specifically because
  they run on different timezones and market hours — this is a deliberate design
  choice, not cosmetic:
  * **NYSE** (New York) — USD, 9:30–16:00 ET
  * **Euronext Paris** (or another Euronext market) — EUR, 9:00–17:30 CET
  * **SSE** (Shanghai Stock Exchange) — CNY, 9:30–15:00 CST (with the midday break)
* **FX rates**: real currency pairs matching the above (USD/EUR, USD/CNY)
* **Commodities**: oil, gold — real reference prices
* **Macro/economic indicators**: simulated central bank rate decisions, inflation
  prints, jobs reports — released on a schedule, each causing a modeled shock to
  correlated instruments (these stay synthetic; real macro calendars are harder to
  license and add little Kafka-relevant complexity)
* **News/sentiment events**: random sentiment shocks tagged to a sector or symbol
  (synthetic)

### 3.2 Sourcing real reference data — practical options

Pick **one** of these for the `market-data-simulator` service's real-data feed;
document which you used and why in the README:

* **Aggregator APIs with a free tier** (Twelve Data, Alpha Vantage, Finnhub,
  Marketstack): give real (sometimes 15-min-delayed) quotes across many global
  exchanges, including Euronext and several Asian markets. Free tiers are rate
  limited (roughly 100–800 calls/day depending on provider) — plenty to poll a
  handful of symbols every few seconds to a minute, not enough to simulate raw
  tick-by-tick flow directly.
* Check each provider's terms of service before building on it: most free tiers
  explicitly allow personal/educational use but prohibit redistribution or
  commercial resale of the data — fine for this project, but don't skip reading it.
* Unofficial scrapers for exchange data (e.g. for SSE) exist but sit in a legal/ToS
  gray area — acceptable to experiment with for a private learning project, but
  don't build the "official" data path on them, and don't publish/redistribute
  what they return.

### 3.3 From real snapshots to Kafka-scale tick volume

Because free-tier real data arrives too slowly to fill Kafka topics at a
useful rate for windowing/Streams work, `market-data-simulator` should:

1. Poll the chosen real-data API on an allowed interval per symbol (respect rate
   limits — build a scheduler, not a busy loop)
2. Treat each real quote as a "true" reference point
3. Synthesize higher-frequency ticks between real reference points using a simple
   stochastic model (e.g. small random walk / jitter bounded so it reverts toward
   the next real reference point) — this is what actually feeds Kafka at volume
4. Tag every event with a field indicating whether it's a real reference tick or a
   synthetic interpolated one — this matters later for the schema-evolution lab and
   for being honest about what's real vs. simulated in the audit trail
5. Respect each exchange's real trading hours (Section 3.1) — no ticks (real or
   synthetic) for NYSE outside 9:30–16:00 ET, etc. This is a genuinely useful
   exercise in itself (scheduling, timezone handling, "market closed" states)

### 3.4 What the platform actually does with this data

1. Ingests raw ticks/events into Kafka
2. Normalizes them into a canonical schema (currency-adjusted, timestamp-aligned)
3. Computes real-time analytics (moving averages, VWAP, volatility, cross-exchange
   arbitrage spread for dual-listed instruments)
4. Accepts simulated buy/sell orders from "traders," matches them against a simple
   order book per instrument, and emits trade executions
5. Maintains live portfolio positions and P\&L per simulated trader/account
6. Detects and alerts on anomalies (price spikes, margin breaches, stale data feeds)
7. Persists an immutable audit trail of every order, execution, and portfolio change

***

## 4. Suggested Microservices / Modules

Each of these should be its own deployable service (or Quarkus/Spring module) so
that consumer groups, scaling, and independent failure are all real and observable.

| # | Service | Responsibility | Key Kafka role |
|---|---|---|---|
| 1 | `market-data-simulator` | Generates synthetic ticks for equities/FX/commodities on realistic intervals with randomized volatility | Producer (high throughput, keyed by symbol) |
| 2 | `macro-events-simulator` | Emits scheduled macro/economic events, occasionally correlated shocks to instrument prices | Producer (low volume, high impact) |
| 3 | `ingestion-normalizer` | Consumes raw ticks, validates + normalizes currency/timestamp, republishes canonical events | Consumer + Producer, schema validation |
| 4 | `analytics-streams` | Kafka Streams topology: moving averages, VWAP, volatility bands, cross-exchange arbitrage detection | Kafka Streams (KStream/KTable, windowing) |
| 5 | `order-matching-engine` | Accepts simulated orders, maintains an order book per instrument, emits executions | Consumer + Producer, local state store |
| 6 | `portfolio-risk` | Builds live positions/P\&L per account from executions, computes exposure, flags margin issues | KTable, stateful aggregation |
| 7 | `alerting-service` | Windowed anomaly detection (price spike, stale feed, margin breach) → alert topic | Streams windowing → Producer |
| 8 | `audit-log` | Transactional outbox: persists every domain event to Postgres and a compacted Kafka topic atomically | Transactional producer |
| 9 | `gateway-ws` | WebSocket gateway pushing live market data / portfolio updates to a frontend/dashboard | Consumer (fan-out) |
| 10 | `schema-evolution-lab` | A deliberately isolated exercise: evolve one event schema in a breaking and non-breaking way and prove compatibility handling | Schema Registry only |

A minimal dashboard (even a simple React or plain HTML/WebSocket page) is useful to
*see* the system working, but the backend/Kafka mechanics are the point — don't over
invest in frontend polish.

***

## 5. Suggested Kafka Topics

| Topic | Key | Partitions (dev) | Notes |
|---|---|---|---|
| `market.ticks.raw` | symbol | 6 | Raw simulated ticks, all asset classes |
| `market.ticks.canonical` | symbol | 6 | Normalized, currency-tagged |
| `market.analytics` | symbol | 6 | Derived metrics (VWAP, MA, volatility) |
| `macro.events` | indicator | 3 | Scheduled macro releases |
| `orders.incoming` | accountId | 6 | Simulated trader orders |
| `orders.executions` | symbol | 6 | Matched trades |
| `portfolio.updates` | accountId | 6 | KTable changelog-style updates |
| `alerts.raised` | symbol/accountId | 3 | Anomaly/margin alerts |
| `audit.events` (compacted) | entityId | 3 | Immutable audit trail |
| `*.DLT` | — | 1 each | Dead-letter topics per consumer group that needs one |

Partition counts above are for local/dev; document what you'd change for a
production-shaped cluster and why (throughput target, consumer parallelism).

***

## 6. Build Phases

### Phase 0 — Infrastructure

* `docker-compose.yml` with: Kafka (KRaft mode, no Zookeeper needed), Schema Registry,
  Postgres, a Kafka UI (Redpanda Console or AKHQ), Prometheus + Grafana
* Confirm you can produce/consume manually via CLI before writing any app code

### Phase 1 — Spring Boot: Foundations

* `market-data-simulator` producing to `market.ticks.raw`
* A minimal `ingestion-normalizer` consumer, plain JSON first (no schema registry yet)
* Manual offset commit, log what happens on rebalance (kill/restart an instance)

### Phase 2 — Spring Boot: Schema Registry

* Introduce Avro schemas for tick and canonical events
* Wire up Schema Registry; validate serialization/deserialization end-to-end

### Phase 3 — Spring Boot: Kafka Streams

* `analytics-streams`: moving average + VWAP via windowed aggregation
* Add a stream-stream join for cross-exchange arbitrage spread on dual-listed symbols

### Phase 4 — Spring Boot: Order Matching + Stateful Service

* `order-matching-engine` with an in-memory (or state-store-backed) order book
* Exactly-once producer config for execution events

### Phase 5 — Spring Boot: Portfolio, Alerts, Audit

* `portfolio-risk` KTable aggregation from executions
* `alerting-service` with windowed anomaly rules
* `audit-log` with the transactional outbox pattern

### Phase 6 — Spring Boot: Resilience & Ops

* Dead-letter topics + retry topic pattern for at least two consumers
* Simulate a broker restart / partition leader change; document consumer behavior
* Grafana dashboard: consumer lag, throughput per topic

### Phase 7 — Schema Evolution Lab

* Make a backward-compatible schema change (add optional field) — verify old
  consumers keep working
* Make a breaking change on a branch — observe/document the failure, then fix it
  properly (new topic version or compatible transformation)

### Phase 8 — Quarkus Port

Re-implement every service above in Quarkus:

* Use SmallRye Reactive Messaging for producers/consumers
* Use the Quarkus Kafka Streams extension for `analytics-streams`
* Keep functional behavior identical — this is a controlled comparison, not a rewrite
  with different features
* Build a GraalVM native image for at least two services and measure:
  * Cold start time (JVM mode vs native)
  * Memory footprint (RSS) at idle and under load
  * Throughput under the same load test as the Spring version
  * Lines of code / config verbosity for equivalent functionality
  * Live-reload / inner-loop developer experience

### Phase 9 — Comparison Report

Produce a short written comparison (`SPRING_VS_QUARKUS.md`) covering at minimum:

* Startup time and memory (with numbers, not vibes)
* Reactive (Quarkus/Mutiny) vs imperative (Spring Kafka) programming model —
  where reactive helped, where it added complexity
* Kafka Streams DX differences between the two frameworks
* Native image gotchas encountered (reflection config, serialization issues, etc.)
* Your honest recommendation: which would you reach for, and when

***

## 7. Testing Strategy

* Use **Testcontainers** (Kafka + Schema Registry + Postgres) for integration tests
  in both the Spring and Quarkus versions
* At least one test per service that proves *correct behavior under rebalance* or
  *at-least-once delivery with a manual failure injected mid-stream*
* Contract tests against the Schema Registry for the schema-evolution lab

***

## 8. Deliverables Checklist

* \[ ] `docker-compose.yml` for full local infra
* \[ ] Spring Boot implementation of all 10 services (Phase 1–7 complete)
* \[ ] Quarkus implementation of all 10 services (Phase 8 complete)
* \[ ] At least 2 native-image builds with measured benchmarks
* \[ ] Grafana dashboard screenshots or exported JSON
* \[ ] `SPRING_VS_QUARKUS.md` comparison report
* \[ ] `README.md` explaining how to run everything locally, including a "tour" of
  which Kafka concept lives in which service (map back to Section 2's checklist)

***

## 9. Explicit Non-Goals

* No licensed real-time exchange feeds and no real brokerage/order execution — real
  data is limited to free-tier reference quotes (Section 3.2); actual trading,
  money movement, and brokerage integration are out of scope
* No personal finance / budgeting features
* No need for production-grade auth/security — this is a learning project; note
  where you're cutting corners rather than silently skipping them
* Frontend can be minimal; this project is graded on Kafka mechanics, not UI polish

# Internal Docker Use

Como usas Docker Desktop con integración WSL2, todo lo que hicimos funciona igual, pero hay dos cosas que quiero que sepas porque te van a ahorrar dolores de cabeza:

La red aggora-net la creaste dentro del motor de Docker de Desktop. Si algún día haces "Docker Desktop → Troubleshoot → Clean / Purge data", o reinstalas Docker Desktop, la red desaparece y tu docker compose up va a fallar con network aggora-net not found. Cuando pase, basta con volver a crear la red:

```
bash
docker network create aggora-net
```

Docker Desktop arranca su propio motor en su propia VM. Eso significa que la red aggora-net que creaste en tu WSL es la misma que ven tus contenedores Docker Desktop — no hay conflicto. Pero ten claro que si algún día ves algo raro con IPs (172.x.x.x), es normal: Docker Desktop usa la red 172.x para sus contenedores y tu WSL tiene su propia IP en otra subred. No deben chocar.

Nada de esto requiere acción ahora. Solo lo apunto para que si mañana ves un error raro de red, no entres en pánico.
