# Aggora — Global Markets Event Platform

**Un proyecto para aprender Kafka de verdad: no con diapositivas, sino construyendo en
vivo una plataforma de datos de mercado y midiendo lo que pasa.**

> *A hands-on Kafka deep-dive: a market-data platform built service by service, with the
> numbers, the failures and the fixes documented as they happened.*

Aggora ingiere precios reales de bolsa y divisas, los normaliza, calcula analítica en
ventanas, cruza órdenes en un libro de órdenes, lleva la cartera de cada cuenta, levanta
alertas y lo audita todo. Cada pieza existe para ejercitar **un concepto de Kafka** concreto,
y cada afirmación del proyecto está verificada contra el clúster en marcha.

Los datos de mercado son **reales** (dos proveedores gratuitos). Los fallos también son
reales: en `docs/decisions.md` y en `docs/kafka-101.md` están los problemas que aparecieron,
por qué aparecieron y cómo se arreglaron.

---

## ¿Qué se demuestra aquí?

| Concepto de Kafka | Dónde está en el proyecto |
| --- | --- |
| Topics, particiones, clave y orden por partición | `market.ticks.raw` con 6 particiones y `key = símbolo` |
| Commit manual de offsets y at-least-once | `ingestion-normalizer` (`AckMode.MANUAL`) |
| Grupos de consumo y rebalanceo | Dos instancias del normalizer reparten 3/3 particiones |
| Avro + Schema Registry y compatibilidad | 12 esquemas en `services/schemas/`, cambio compatible real (`v2` de `Order`/`Execution`) |
| Evolución de esquemas sin romper consumidores | `scripts/schema-evolution-lab.sh`: veredictos BACKWARD y FORWARD medidos, rechazo real con `409` y los tres arreglos |
| Kafka Streams con la API a pelo | Ventanas fijas y móviles, state stores, join stream-stream y stream-GlobalKTable |
| Consultas interactivas al estado | `GET /analytics?symbol=...` lee el state store en caliente |
| Exactly-once de punta a punta | Productor transaccional + `isolation.level=read_committed` |
| Dead-letter topics y reintentos | `.DLT` en dos consumidores + `@RetryableTopic` con topics `.retry-*` |
| Transactional outbox | Postgres y el topic compactado `audit.events` |
| Réplicas, ISR y `min.insync.replicas` | Clúster KRaft de 3 brokers con 3 réplicas por partición |
| Operación y observabilidad | kafka-exporter + Prometheus + Grafana provisionados desde el repo |

---

## Arquitectura

```
   Twelve Data (EEUU, forex, oro, ETFs)   ┐
   Alpha Vantage (Euronext, Shanghai)     ┘
                    │  sondeo REST + paseo aleatorio para dar volumen
                    ▼
        ┌───────────────────────┐        ┌──────────────────────┐
        │ market-data-simulator │───────►│   market.ticks.raw   │
        │      (HTTP 8080)      │        │  6 particiones       │
        └───────────┬───────────┘        └──────────┬───────────┘
                    │ órdenes simuladas             │  Avro
                    ▼                               ▼
        ┌───────────────────────┐        ┌──────────────────────────┐
        │   orders.incoming     │        │  ingestion-normalizer    │
        └───────────┬───────────┘        │  commit manual · valida  │
                    │                    │  y republica canónico    │
                    │                    └────┬───────────────┬─────┘
                    │                        │               │ inválidos → .DLT
                    │              market.ticks.canonical    │
                    │                        │               │
                    │                        ▼               ▼
                    │            ┌────────────────────┐  market.fx.reference
                    │            │  analytics-streams │  (compactado)
                    │            │  ventanas · joins  │
                    │            │    HTTP 8085       │
                    │            └───┬────────────┬───┘
                    │                ▼            ▼
                    │        market.analytics  market.arbitrage
                    │                              (ASML NASDAQ vs AMS)
                    ▼
        ┌───────────────────────┐
        │ order-matching-engine │  libro por instrumento, precio-tiempo
        │  exactly-once · .DLT  │
        └───────────┬───────────┘
                    ▼
            orders.executions
                    │
      ┌─────────────┼──────────────────┬────────────────────┐
      ▼             ▼                  ▼                    ▼
┌───────────────┐ ┌────────────────┐ ┌──────────────┐ ┌──────────────┐
│ portfolio-risk│ │alerting-service│ │  audit-log   │ │  (grafana /  │
│ KTable de     │ │ 3 reglas +     │ │ outbox en    │ │ prometheus)  │
│ posiciones    │ │ punctuator     │ │ Postgres     │ │              │
└───────┬───────┘ └───────┬────────┘ └──────┬───────┘ └──────────────┘
        ▼                 ▼                 ▼
 portfolio.updates   alerts.raised     audit.events
                                      (compactado)
```

Siete servicios Spring Boot, dos topics compactados, un clúster de tres brokers y una
infraestructura que se levanta con un solo comando.

---

## Stack

| Pieza | Versión | Por qué |
| --- | --- | --- |
| Kafka (KRaft, sin ZooKeeper) | `apache/kafka:3.9.0` | Broker Apache puro, sin dependencias de Confluent |
| Schema Registry | `confluentinc/cp-schema-registry:8.3.1` | Alineado con la librería Avro y el serializador |
| Java | 21 (LTS) | Mismo lenguaje para la versión Spring y la futura Quarkus |
| Spring Boot | 4.1.1 | Incluye la migración a Jackson 3 y `spring-boot-starter-kafka` |
| Kafka Streams | API a pelo (`Topology`) | El objetivo es aprender la API, no esconderla |
| Postgres | `postgres:16-alpine` | Patrón transactional outbox |
| Observabilidad | kafka-exporter + Prometheus + Grafana | Panel provisionado desde el repo, sin clics |
| Build | Maven multi-módulo | Un `pom.xml` padre y siete módulos en `services/` |

---

## Cómo arrancarlo

Requisitos: Docker (o Docker Desktop con WSL2) y el devcontainer del repo, que es donde
viven Java y Maven. **La infraestructura corre fuera del devcontainer**, en la red
compartida `aggora-net`.

```bash
# 0) Infraestructura: 3 brokers KRaft, Schema Registry, Postgres, Prometheus, Grafana
docker network create aggora-net 2>/dev/null || true
docker compose -f infra/docker-compose.yml up -d

# 1) Compilar (dentro del devcontainer)
cd /workspaces/aggora/services
mvn -q -DskipTests package

# 2) Arrancar los siete servicios en orden, esperando a que cada uno esté listo
export TWELVEDATA_API_KEY=...      # EEUU, forex, oro y ETFs
export ALPHAVANTAGE_API_KEY=...    # Euronext y Shanghai (plan gratuito: 25 peticiones/día)
cd /workspaces/aggora
bash scripts/start-services.sh     # logs en /tmp/<servicio>.log
bash scripts/stop-services.sh      # para pararlos todos

# 3) Comprobación rápida
curl -s localhost:8081/subjects                              # los contratos registrados
curl -s 'localhost:8085/analytics?symbol=EUR/USD&minutes=3'   # consulta al state store
open http://localhost:3000/d/aggora-kafka                     # panel de Kafka en Grafana
```

Las API keys son opcionales: sin ellas el simulador funciona igual, solo que con precios
sintéticos. **Nunca se escriben en el repositorio**; se leen de variables de entorno.

- Kafka (desde fuera de Docker): `localhost:29092`, `29093`, `29094`
- Schema Registry: `localhost:8081` · Postgres: `localhost:5432` · Prometheus: `localhost:9090`
- Grafana: `localhost:3000` (panel **Aggora — Kafka**, entrada anónima en dev) · métricas en crudo: `localhost:9308`

---

## Estado del proyecto

| Fase | Contenido | Estado |
| --- | --- | --- |
| 0 | Infraestructura local en Docker (KRaft + red `aggora-net`) | ✅ |
| 1 | Simulador y normalizador; commit manual, rebalanceo y at-least-once | ✅ |
| 2 | Avro y Schema Registry de punta a punta; topics explícitos | ✅ |
| 3 | Kafka Streams: ventanas, estado, joins y consultas interactivas | ✅ |
| 4 | Motor de cruce de órdenes con exactly-once transaccional | ✅ |
| 5 | Cartera, alertas y auditoría con transactional outbox | ✅ |
| 6 | Resiliencia y operación: DLT, reintentos, 3 brokers, Grafana | ✅ |
| 7 | Laboratorio de evolución de esquemas: qué rompe, cómo se detecta y cómo se arregla | ✅ |
| 8 | Port de los servicios a Quarkus + imagen nativa de GraalVM | 🔄 siguiente |
| 9 | Informe comparativo Spring vs Quarkus con números | ⏳ |

**Verificado en vivo, no en teoría:** 41 tests unitarios en verde, 3 brokers con quórum
KRaft y 3 réplicas por partición (con dos brokers caídos la escritura se detiene con
`NOT_ENOUGH_REPLICAS` en vez de perder datos), exactly-once medido sobre el mismo topic
(`read_committed` 121 mensajes frente a `read_uncommitted` 168), los seis veredictos de
compatibilidad de esquemas medidos contra el registro en las dos direcciones, y 131.932
eventos auditados que sobrevivieron a la reconstrucción completa del entorno.

---

## Estructura del repositorio

```
README.md                  portada del proyecto
SPEC.md                    especificación original (inmutable)
CONTRIBUTING.md            directrices de trabajo, convenciones y estado fase a fase
docs/kafka-101.md          los conceptos de Kafka en lenguaje llano (17 capítulos)
docs/decisions.md          registro de decisiones y de las desviaciones del spec
docs/schema-evolution-lab.md  manual del laboratorio de evolución de esquemas
infra/                     docker-compose, Prometheus, panel de Grafana
scripts/                   arranque y parada de los servicios, y el laboratorio de esquemas
services/                  Maven multi-módulo
  schemas/                 los 12 contratos Avro (.avsc) de los que se generan las clases
  market-data-simulator/   precios reales + órdenes simuladas
  ingestion-normalizer/    validación y evento canónico
  analytics-streams/       ventanas, joins y consultas interactivas
  order-matching-engine/   libro de órdenes y exactly-once
  portfolio-risk/          posiciones y P&L por cuenta
  alerting-service/        reglas de anomalía
  audit-log/               transactional outbox
```

---

## Documentación

- **[`docs/kafka-101.md`](docs/kafka-101.md)** — la guía de conceptos: qué es un topic, una
  partición, un offset, un rebalanceo, una transacción… explicado con ejemplos de este
  proyecto, sin dar por sabido nada.
- **[`docs/decisions.md`](docs/decisions.md)** — por qué cada decisión se tomó así, incluidas
  las desviaciones conscientes del spec y los errores que costaron tiempo.
- **[`docs/schema-evolution-lab.md`](docs/schema-evolution-lab.md)** — qué se puede cambiar en
  un contrato en marcha sin romper a nadie, con los veredictos que dio el registro y el
  playbook para hacerlo en producción.
- **[`CONTRIBUTING.md`](CONTRIBUTING.md)** — cómo se trabaja en el repo, convenciones de red
  y de nombres, y el detalle de verificación de cada fase.

---

## Lo que este proyecto no es

No es una plataforma de trading lista para producción: no hay autenticación, no hay
ejecución real de órdenes ni conexión a un mercado de verdad, y las credenciales que
aparecen son de desarrollo. Es un **laboratorio** cuyo objetivo es entender Kafka midiendo
su comportamiento, incluidos sus modos de fallo.
