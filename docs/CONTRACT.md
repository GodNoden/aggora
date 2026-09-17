# CONTRACT — Aggora dashboard API (v1)

Interface between the Aggora backend (this repository) and the dashboard app (another repository).
Everything here is **read-only**: GET and WebSocket only. There is no write endpoint.

Short, copy-pasteable version. The long version with the reasoning is in
[`docs/dashboard.md`](dashboard.md).

## Ports and endpoints

| What | Spring | Quarkus | Path |
|---|---|---|---|
| Live events | `ws://<host>:8089/ws` | `ws://<host>:8189/ws` | WebSocket, one JSON per frame |
| Metrics catalog | `http://<host>:8089/api/metrics` | `http://<host>:8189/api/metrics` | `?panel=<name>` |
| Interactive query | `http://<host>:8085/analytics` | `http://<host>:8185/analytics` | `?symbol=EUR/USD&minutes=3` |
| Simulator status | `http://<host>:8080/actuator/health` | — | actuator |

Behind TLS use `wss://` for the WebSocket (an `https://` page cannot open a `ws://` connection).
Both gateways honour `X-Forwarded-*` headers.

## WebSocket messages

Every message has the envelope `v` (contract version, currently `1`), `kind`, `stack`
(`"spring"` / `"quarkus"`) and `ts` (ISO-8601 UTC).

### `snapshot` — once per second, last tick per symbol

```json
{"v":1,"kind":"snapshot","stack":"spring","ts":"2026-09-17T08:00:00.123Z",
 "ticksIn":84,"ticksOut":12,
 "symbols":{"EUR/USD":{"price":"1.0875","currency":"USD","size":100,"source":"REFERENCE","at":"2026-09-17T08:00:00.100Z"}}}
```

- `ticksIn`: ticks consumed from Kafka during the last second.
- `ticksOut`: symbols present in this snapshot (one entry per symbol).
- `symbols`: object keyed by symbol; **only the last value per symbol**, never a list.
- `price`, `averageCost`, `realizedPnl`, `exposure`, `value` are **strings** (Avro decimals).
- A symbol that stops ticking keeps its last known price.

### `position` — immediately

```json
{"v":1,"kind":"position","stack":"spring","ts":"...","account":"ACC-1","symbol":"AAPL",
 "quantity":100,"averageCost":"333.47","realizedPnl":"0","exposure":"33347","currency":"USD","marginBreach":false}
```

### `alert` — immediately

```json
{"v":1,"kind":"alert","stack":"spring","ts":"...","severity":"CRITICAL","type":"VOLATILITY_SPIKE",
 "subject":"ASML","detail":"...","value":"0.0123","raisedAt":"..."}
```

`severity` is `CRITICAL`, `WARNING` or `INFO`.

The WebSocket is a fan-out: no replay, no redelivery. A slow client misses messages and recovers
with the next snapshot. Do not treat it as a log.

## `GET /api/metrics?panel=<name>`

Closed catalog: the panel name is the only input. Free PromQL is not accepted.

```json
{"panel":"lag","ts":"2026-09-17T08:00:00.123Z","stack":"spring",
 "series":[{"label":"ingestion-normalizer","points":[[1758096000,0]]}]}
```

- `points` is a list of `[epoch seconds, value]`; currently one point per series (instant query).
- `nota` appears **only** when `series` is empty, explaining why. Never invent a series.
- `transacciones` always returns `"series":[]` plus a `nota`: Prometheus in this environment has no
  committed/aborted transaction counts.

### Panels

| Panel | Series (`label`) | Meaning |
|---|---|---|
| `pulso` | `entrada`, `salida-spring`, `salida-quarkus` | ticks/s in `market.ticks.raw` and out of `market.ticks.canonical(.q)` |
| `lag` | consumer group name | lag of `ingestion-normalizer`, `ingestion-normalizer-q`, `analytics-streams`, `analytics-streams-q` |
| `particiones` | `topic/partition` | current offset of `market.ticks.raw`, `market.ticks.canonical(.q)`, `market.analytics(.q)` |
| `transacciones` | — | **empty + nota** (no committed/aborted series exists) |
| `descartes` | `topic/partition` | offsets of `market.ticks.raw.DLT(.q)` and `*.retry-*` |
| `salud` | target `job/instance`, `stack/service`, `under-replicated/<topic>` | Prometheus `up`, `aggora_kafka_streams_running`, under-replicated partitions |
| `comparativa` | `spring...`, `quarkus...` | needs `&de=<panel>`: returns the Spring and Quarkus series of that panel side by side |

`comparativa` accepts `de` = `pulso`, `lag`, `particiones`, `descartes`, `salud`, `transacciones`.
Default `de` is `pulso`. Example: `GET /api/metrics?panel=comparativa&de=lag`.

### Errors

| HTTP | Body |
|---|---|
| 400 | `{"error":"panel desconocido: X","detalle":["pulso","lag","particiones","transacciones","descartes","salud","comparativa"]}` |
| 400 | `{"error":"comparativa no soporta de=X","detalle":[...]}` |
| 502 | `{"error":"prometheus no responde","detalle":"..."}` |

## CORS

Allowed origins are configurable per service; the default is
`http://localhost:4200,http://localhost:8080,http://localhost:8089,http://localhost:8189`.
Methods: `GET`. Credentials: not enabled. When publishing, add the public origin:

- Spring: `aggora.ui.allowed-origins` (comma-separated).
- Quarkus: `quarkus.http.cors.enabled=true` + `quarkus.http.cors.origins`, or
  `QUARKUS_HTTP_CORS_ORIGINS`. In Quarkus 3.39.3 the older `quarkus.http.cors=true` is not
  recognised and CORS is silently not applied.

CORS is enforced by the browser, not the server: it is not authentication.

## Not available (do not rely on it)

- No committed/aborted transaction counts (see `transacciones`).
- No historical data in the WebSocket: only the last value per symbol.
- No per-partition ISR in the catalog beyond the under-replicated count.
- No write endpoint of any kind.
