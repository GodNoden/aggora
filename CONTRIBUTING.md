# Guía de trabajo — directrices del proyecto

> Este documento es el **cuaderno de trabajo** del proyecto: las reglas que seguimos,
> las convenciones de red y de nombres, el estado de cada fase con sus verificaciones
> y las decisiones tomadas sobre la marcha. Nació siendo el `README.md` y se movió aquí
> cuando el README pasó a ser la portada del repositorio.
>
> Si buscas qué es Aggora y cómo arrancarlo, empieza por el [README](README.md).

## Idioma y estilo de trabajo
- Responde SIEMPRE en español.
- **El usuario está aprendiendo Kafka. Explicar es parte del entregable, no un extra.**
  - Antes de usar un término técnico (offset, partición, idempotencia, lag,
    rebalanceo…), defínelo en la misma frase y con un ejemplo de ESTE proyecto.
  - Nada de tablas de métricas sin decir qué significan.
  - Usa analogías cotidianas antes de la definición formal.
  - Al cerrar cada fase, aparte del resumen técnico, escribe un apartado corto
    "qué has aprendido" en lenguaje llano, y **pregunta explícitamente qué parte
    no se ha entendido** antes de dar la fase por cerrada.
- Trabaja POR FASES. No adelantes trabajo de una fase futura.
- Al terminar cada fase, PÁRATE y pide confirmación antes de seguir.
- Entrega código completo y compilable, no fragmentos ni pseudocódigo.
- Si algo requiere una decisión, PREGUNTA antes de asumir.

## Entorno del desarrollador
- Host: Windows con Docker Desktop (integración WSL2).
- El código corre dentro de un **devcontainer** (VSCode).
- La infraestructura (Kafka, Postgres, Schema Registry, etc.) corre como
  contenedores Docker **fuera** del devcontainer.
- La red `aggora-net` es compartida entre el devcontainer y la infra.
  Se crea UNA vez con `docker network create aggora-net` y NO se destruye
  con `docker compose down` (está declarada como `external: true`).
- El devcontainer se une a esa red via `runArgs: ["--network=aggora-net"]`.

## Convenciones de red
- Dentro de `aggora-net`, los servicios se hablan POR NOMBRE y puerto interno:
  - `kafka-1:9092,kafka-2:9092,kafka-3:9092` (listener INTERNAL de los 3 brokers;
    se listan los tres para que si uno está caído el cliente se entere por los otros)
  - `schema-registry:8081`
  - `postgres:5432`
  - `prometheus:9090`
- Desde WSL/Windows (fuera de Docker), los servicios se acceden por `localhost`
  y el puerto publicado:
  - `localhost:29092`, `29093`, `29094` (listener EXTERNAL de cada broker)
  - `localhost:8081` (Schema Registry)
  - `localhost:5432` (Postgres)
  - `localhost:8090` (Redpanda Console, pendiente)
  - `localhost:9090` (Prometheus)
  - `localhost:9308` (kafka-exporter, metricas en crudo)
  - `localhost:3000` (Grafana → panel "Aggora — Kafka")
  - `localhost:3000` (Grafana)
- El código Java NUNCA usa `localhost:9092` ni `localhost:8081`. Siempre
  usa los nombres internos (`kafka:9092`, `schema-registry:8081`).

## Convenciones de nombres y versiones
- Contenedores: prefijo `aggora-` (ej. `aggora-kafka`).
- Hostnames internos: sin prefijo (`kafka`, `postgres`, `schema-registry`).
- Imágenes (fijas, no cambiar sin justificar):
  - `apache/kafka:3.9.0` (KRaft, sin Zookeeper)
  - `confluentinc/cp-schema-registry:7.6.1`
  - `postgres:16-alpine`
  - `redpandadata/console:v2.7.2`
  - `danielqsj/kafka-exporter:v1.8.0`
  - `prom/prometheus:v2.54.1`
  - `grafana/grafana:11.2.0`
- Credenciales dev (NO son de producción):
  - Postgres: `aggora / aggora / aggora`
  - Grafana: `admin / admin`

## Estructura del repo
- `SPEC.md` — spec original, inmutable.
- `AGENTS.md` — ponytail service
- `README.md` — portada del repositorio (qué es y cómo arrancarlo).
- `CONTRIBUTING.md` — este archivo. Actualizar al cerrar cada fase.
- `docs/` — decisiones e infra detallada (`decisions.md`), guía de conceptos Kafka en
  lenguaje llano (`kafka-101.md`) y manual del laboratorio de evolución de esquemas
  (`schema-evolution-lab.md`).
- `infra/` — docker-compose, configs de Prometheus/Grafana.
- `scripts/` — arranque y parada de los servicios y el laboratorio de esquemas.
- `services/` — agregador Maven de las DOS implementaciones (no hereda de nadie):
  - `schemas/` — los contratos Avro (`.avsc`) de los que se generan las clases Java,
    compartidos por las dos implementaciones.
  - `spring/` — implementación Spring Boot (Fase 1): `market-data-simulator` (produce
    ticks a `market.ticks.raw`), `ingestion-normalizer` (los consume con commit
    manual) y los otros cinco.
  - `quarkus/` — implementación Quarkus (Fase 8).

## Estado actual (actualizar al cerrar cada fase)
- ✅ **Fase 0a** — Kafka solo, red `aggora-net` creada y verificada
  desde el devcontainer (`nc -zv kafka 9092` → succeeded).
- ⚠️ **Fase 0b** — el compose perdió los servicios auxiliares respecto a lo que
  decía este README. Se reconstruyen en la fase que los use: **Schema Registry ya
  está (Fase 2)**; Postgres en Fase 5; Redpanda Console, kafka-exporter, Prometheus
  y Grafana en Fase 6. Ver `docs/decisions.md`.
- ✅ **Fase 1** — `market-data-simulator` y `ingestion-normalizer` (Spring Boot 4.1.1,
  Java 21, multi-módulo Maven en `services/spring/`). Verificado end-to-end:
  topic `market.ticks.raw` creado con 6 particiones, ~53 ticks/s con key = symbol,
  productor idempotente sin fallos, consumidor con commit manual y lag 0,
  rebalanceo 3/3 al arrancar una segunda instancia, y reprocesión de offsets
  tras un `kill -9` (at-least-once). 17 tests unitarios en verde.
- ✅ **Fase 1b** — datos reales con **dos proveedores**, porque ningún plan gratuito
  cubre los tres husos horarios del spec: **Twelve Data** para acciones de EEUU,
  forex y oro (endpoint por lotes, poll cada 15 min) y **Alpha Vantage** para
  Euronext París y Shanghai (una petición por símbolo, 1/segundo, poll cada 3 h).
  Verificado símbolo a símbolo: **12 de 12 instrumentos con precio real** y ticks
  `fuente=REFERENCE` en el topic. El crudo spot no está gratis en ninguno de los dos,
  así que se usa el ETF `USO` (precio vivo, pero **es el ETF, no el barril**).
  Detalle en `docs/decisions.md`. Las keys van en `TWELVEDATA_API_KEY` y
  `ALPHAVANTAGE_API_KEY`, y **no se escriben en ningún fichero del repo**.
- ✅ **Fase 2** — Avro + Schema Registry de punta a punta. Imagen subida a
  `confluentinc/cp-schema-registry:8.3.1` (alineada con la librería Avro 8.3.1),
  esquemas en `services/schemas/*.avsc` de los que se **generan** las clases Java
  (no se escriben a mano), y el normalizer ya no es solo consumidor: valida el tick
  Avro y **republica el evento canónico** con su propio esquema.
  Verificado: 2 subjects (`market.ticks.raw-value`, `market.ticks.canonical-value`),
  mensajes binarios en el crudo, 7.000 mensajes procesados con 0 descartes y 0
  errores, y lag 0. El `eventTime` ya viaja como fecha declarada (adiós al número
  opaco de la Fase 1).
- ✅ **Fase 3** — Kafka Streams con la API a pelo en el modulo `analytics-streams`:
  - **Metricas por ventana**: VWAP, media y volatilidad de `market.ticks.canonical` con
    **dos tipos de ventana** (fija de 30 s y movil de 60 s/15 s) hacia `market.analytics`,
    con **state stores** y sus topics de changelog (el estado sobrevive a un reinicio).
  - **Spread de arbitraje** de las dos cotizaciones de ASML: join **stream-stream** con
    ventana de 5 s para cruzar los dos precios, y join **stream-globalTable** para
    convertir el precio europeo a dolares con el tipo de cambio de `market.fx.reference`
    (topic compactado). Salida en `market.arbitrage`.
  - **Consultas interactivas**: `GET /analytics?symbol=EUR/USD&minutes=3` (puerto 8085)
    lee el state store en marcha y devuelve las ultimas ventanas ya calculadas.
  Verificado: **6 tests** de topologia con `TopologyTestDriver` y registry `mock://`
  (sin broker), metricas y consultas funcionando en vivo. El spread en vivo solo
  aparece en el solape NASDAQ+Euronext (13:30-15:30 UTC), que es cuando los dos
  mercados cotizan a la vez.
- ✅ **Fase 4** — `order-matching-engine`: libro de ordenes **por instrumento** (prioridad
  precio-tiempo) que cruza las ordenes simuladas de `orders.incoming` y publica las
  ejecuciones en `orders.executions` con **exactly-once**: productor transaccional +
  gestor de transacciones en el contenedor + consumidor `read_committed`. Las ordenes las
  genera el simulador (`OrderFlowGenerator`, una por segundo).
  Verificado: **4 tests** del libro (cruce al precio pasivo, ejecucion parcial, sin cruce
  y prioridad por tiempo) y el experimento de exactly-once en vivo: con fallos
  inyectados cada 10 ordenes, un consumidor `read_committed` veia **121** mensajes y uno
  `read_uncommitted` **168** (los 47 abortados existen en el log pero no cuentan).
- 🔄 **Fase 5 (casi)** — `portfolio-risk` ya funciona: abre cada ejecucion en **dos**
  movimientos (el comprador suma y el vendedor resta), los re-clava por cuenta+simbolo y
  los acumula en una **KTable** de posiciones (coste medio, P&L realizado, exposicion y
  aviso de margen) hacia `portfolio.updates`.
  Verificado: **3 tests** (coste medio ponderado, cierre parcial con P&L y vuelta de
  posicion), y en vivo posiciones reales con su divisa. De paso, el **primer cambio de
  esquema compatible** del proyecto: se anadio `currency` con valor por defecto a Order y
  Execution, y el registry lo acepto como **version 2** sin romper a nadie.
  - `alerting-service` (hecho): tres reglas hacia `alerts.raised` — **pico de precio**
    (comparando el ultimo precio con la media de su ventana), **margen superado** (la marca
    que pone portfolio-risk) y **feed parado** (con un *punctuator*, porque hay que detectar
    la AUSENCIA de datos). En vivo salieron dos lecciones y las dos estan arregladas: avisar
    una vez por episodio y usar **histeresis** (disparar a 40 puntos basicos, rearmar a 20),
    que bajo el ruido de ~36.000 avisos a 686 en el mismo tiempo.
  - `audit-log` (hecho): **transactional outbox** con Postgres. El evento y su recado de
    publicacion se escriben en la MISMA transaccion de base de datos; un publicador los manda
    despues al topic **compactado** `audit.events`. Verificado en vivo: 122.000 eventos
    auditados y la bandeja de salida drenandose a `pendientes = 0`.
  - Verificacion: los comandos de abajo.
- 🔄 **Fase 6 (en curso)** — Resiliencia:
  - **Dead-letter topics y reintentos** en tres consumidores: `ingestion-normalizer`
    (los ticks invalidos van a `market.ticks.raw.DLT` **con el motivo en una cabecera**),
    `order-matching-engine` (reintentos cortos y, si la orden es venenosa, a
    `orders.incoming.DLT`) y `audit-log` (patron de **retry topics** con
    `@RetryableTopic`: topics `.retry-500`, `.retry-1000` y `.DLT`).
    Verificado en vivo: 7 ticks invalidos descartados con su motivo, 24 ordenes venenosas
    descartadas **sin bloquear la particion** (que era el problema de la Fase 4), y la
    auditoria recuperandose sola tras parar Postgres.
  - **Reinicio del broker** documentado: con `replicas: 1` no hay failover, pero los
    clientes hacen buffer y reintentan, asi que el pipeline se recupera solo (los offsets
    siguieron avanzando durante el reinicio).
  - **Clúster de 3 brokers** (hecho): 3 nodos en KRaft con quórum, **3 réplicas** por
    partición y `min.insync.replicas: 2`. Experimentos verificados:
    con **1 broker caído** el líder se elige solo y se sigue escribiendo (offsets 639→705);
    con **2 caídos** los offsets se **congelan** (751→751) y el productor da
    `NOT_ENOUGH_REPLICAS`: es la garantía, mejor parar que arriesgar datos; y al volver los
    brokers se recupera **sin perder nada** (el offset salta a 1310 con los mensajes que el
    productor tenía en el buffer).
    Los topics se declaran **sin réplicas explícitas** (manda el default del broker) y los
    topics internos de Streams llevan `replication.factor: 3`.
- 🐛 **Fallo silencioso encontrado y arreglado** — Kafka Streams, ante un error que no puede
  manejar, **para el cliente pero deja vivo el proceso**: el endpoint respondía y el servicio
  no procesaba nada. Ahora los tres servicios de Streams definen un
  `StreamsUncaughtExceptionHandler` que **sustituye el hilo** (`REPLACE_THREAD`) en vez de
  matar el cliente, y el endpoint de consultas devuelve un **503 explicando** el estado ERROR
  en lugar de un 500 genérico. Verificado recreando el escenario que lo provocaba (borrar el
  topic compactado): el servicio sigue calculando métricas y respondiendo.
- 🧱 **Operación (aprendido a golpes)** — Se reinició el entorno (Docker Desktop/WSL) y se
  cayeron los 7 servicios y los 3 contenedores. Al volver, **no se perdió nada**: 29 topics,
  22 esquemas y 131.932 eventos auditados seguían ahí, porque el estado vive en Kafka y
  Postgres, no en los servicios. Se arreglaron tres cosas: políticas de reinicio
  (`restart: unless-stopped`) en la infraestructura, los scripts de arranque/parada, y **la
  carrera de topics** (Kafka Streams falla con `MissingSourceTopicException` si su topic de
  origen aún no existe: ahora el arranque espera a que cada servicio confirme que está
  listo). Los detalles, en el capítulo 14 de `docs/kafka-101.md`.
- ✅ **Observabilidad (Fase 6)** — **kafka-exporter + Prometheus + Grafana**, con el panel
  **provisionado desde el repo** (no hay que configurar nada a mano):
  **http://localhost:3000/d/aggora-kafka** (entrada anónima en dev; `admin/admin` para la API).
  Paneles: **lag por grupo** y por topic, **throughput por topic**, **mensajes descartados en
  los DLT** y brokers vivos. Verificado con datos reales: lag de los 6 grupos y throughput
  (market.analytics ~308 msg/s, market.ticks.raw ~62 msg/s).
  Pendiente: metricas de las aplicaciones (actuator + micrometer) y el clúster de 3 brokers.
- 🧱 **Infra añadida en la Fase 2** — `KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
  (un typo en un nombre de topic debe fallar, no crear un topic fantasma de 1
  partición) y un servicio `kafka-init` que crea los topics internos que no declara
  el código (`_schemas`). Las dos cosas tienen su porqué en `docs/decisions.md`.
- ✅ **Migración a Spring Boot 4.1.1** — pedida por el usuario por el aviso de
  soporte OSS. Verificado: 17 tests en verde, los dos servicios en marcha y 63.500
  mensajes procesados. Lo que cambió está en `docs/decisions.md`; en resumen:
  Jackson 3 (paquete `tools.jackson`), la autoconfiguración de Kafka pasa al módulo
  `spring-boot-kafka` (starter `spring-boot-starter-kafka`) y los serializadores
  JSON de Spring Kafka se llaman ahora `JacksonJson*`.
- ✅ **Fase 7 — Laboratorio de evolución de esquemas** (hecho) —
  `scripts/schema-evolution-lab.sh` pregunta al registro de verdad por seis cambios
  distintos y enseña los veredictos **en las dos direcciones** (BACKWARD y FORWARD), los dos
  rechazos con el mensaje literal del registro (`READER_FIELD_MISSING_DEFAULT_VALUE`,
  `HTTP 409`) y los arreglos que existen. Medido, no supuesto: lo único que funciona en las
  dos direcciones es **añadir un campo con valor por defecto**; **borrar un campo pasa el
  filtro BACKWARD** (el lector nuevo ignora lo que no conoce) y sin embargo rompe a los
  consumidores que ya estaban desplegados. El laboratorio no produce ni un mensaje con los
  esquemas de prueba (un mensaje apunta al ID de su esquema: borrarlo lo deja ilegible para
  siempre) y deja el subject como estaba (`versiones: [1] (baseline: [1])`).
  La traducción de Avro se comprueba sin broker en `SchemaEvolutionTest` (5 tests, con la
  clase `CanonicalTick` ya compilada haciendo de consumidor antiguo): **41 tests en verde**
  en total. Detalle en `docs/schema-evolution-lab.md` y capítulo 17 de `docs/kafka-101.md`.
- 🔄 **Fase 8 (en curso) — Port a Quarkus**: el trabajo previo está hecho y verificado.
  - **El árbol se partió para que las dos implementaciones convivan** (no es una migración:
    la versión Spring se queda, y el port será una segunda implementación de la misma
    plataforma, generando sus clases desde los MISMOS contratos Avro).
    `services/pom.xml` es ahora un agregador que no hereda de nadie; `services/spring/` cuelga
    de `aggora-spring-services` y `services/quarkus/` tendrá su padre con el BOM de Quarkus.
    Los 7 módulos se movieron con `git mv` (historia conservada) y los **41 tests siguen en
    verde** tras el movimiento.
  - **Métricas de aplicación** (`actuator` + `micrometer-registry-prometheus`) en los dos
    servicios que ya tienen servidor web: `market-data-simulator` (8080) y `analytics-streams`
    (8085). A los otros cinco habría que añadirles `spring-boot-starter-web` solo para mirarles
    la memoria, y eso cambiaría justo lo que se quiere medir. Las series llevan las etiquetas
    `stack="spring"` y `service=...`, que son las que permitirán poner las dos
    implementaciones en el mismo panel de Grafana. Prometheus ya las recoge
    (`host.docker.internal:8080` y `:8085`, porque los servicios corren en el devcontainer y su
    nombre de contenedor no resuelve desde `aggora-net`).
  - **Sonda de salud del motor** (`StreamsHealth`): `/actuator/health` baja a **DOWN** cuando
    Kafka Streams está en ERROR y publica `aggora_kafka_streams_running` (1/0). Verificado
    rompiendo el checkpoint del GlobalKTable a propósito: el proceso sigue vivo, el endpoint
    devuelve 503 y la sonda dice DOWN con `estado: ERROR`. Es lo que convierte un fallo
    silencioso en un reinicio por parte del supervisor.
  - **Dos hallazgos por el camino**, los dos arreglados o documentados: el endpoint de
    consultas devolvía **500** cuando el state store aún se estaba reconstruyendo (es un
    transitorio, ahora es **503**, verificado en un arranque real: 503…503, 200, y ningún 500);
    y `SHUTDOWN_APPLICATION` como respuesta al error del hilo global **no mata el proceso**
    dentro de Spring: enreda el cierre, escribe 429 MB de log en 28 segundos y el JVM sigue
    vivo. Los detalles, en `docs/decisions.md`.
  - **Línea base medida** (`bash scripts/measure-service.sh`, RSS en reposo y arranque):
    simulador 531 MB / 2,8 s · normalizer 441 MB / 2,0 s · analytics 516 MB / 3,2 s ·
    matching 297 MB / 2,6 s · portfolio 434 MB / 2,7 s · alerting 469 MB / 2,3 s ·
    audit 336 MB / 2,6 s. Son los números contra los que se medirá Quarkus en la Fase 9.
- ✅ **Fase 8 — el primer servicio portado: `ingestion-normalizer` en Quarkus** (hecho).
  `services/quarkus/ingestion-normalizer` con Quarkus 3.39.3 + SmallRye Reactive Messaging,
  los MISMOS contratos Avro, los mismos topics, el mismo `group.id` y los mismos
  serializadores de Confluent, así que **es intercambiable**: se para uno y arranca el otro y
  el pipeline ni se entera. Verificado en vivo: el port tomó las 6 particiones (lo contó su
  listener de rebalanceo), los offsets del canónico siguieron subiendo, **`analytics-streams`
  (el de Spring) respondió 200 con ventanas nuevas** y los ticks inválidos fueron al DLT con
  la misma cabecera `x-dlt-reason` y el mismo texto. 7 tests del validador en verde.
  Primeros números, mismo servicio y misma máquina:
  arranque **1,123 s (Quarkus)** frente a 2,099 s (Spring) y RSS **332 MB** frente a 428 MB.
- ✅ **Fase 8 — segundo servicio portado: `market-data-simulator`** (hecho).
  `services/quarkus/market-data-simulator` con la **misma configuración YAML** (bloque `aggora:`
  copiado entero, 14 instrumentos incluidos), clientes REST declarativos para los dos
  proveedores, el scheduler de Quarkus y métricas en `/q/metrics`. Los **17 tests** del
  simulador de Spring, portados y en verde. Verificado en vivo: parado el de Spring y
  arrancado el de Quarkus, el pipeline de Spring siguió sin enterarse (**44 msg/s** de ticks,
  órdenes cada segundo, `analytics` 200).
  Números: arranque **1,425 s** frente a 2,847 s y RSS **304 MB** frente a 375 MB.
  Los hallazgos del port (el scheduler de Quarkus **no baja de un segundo**, la config
  estricta con las propiedades vacías, `skipExecutionIf`, las claves con puntos en YAML,
  Jackson 2 vs 3 y el `RecordMetadata` que no existe en un `Emitter`) están en
  `docs/decisions.md`.
- ✅ **Fase 8 — tercer servicio portado: `analytics-streams`** (hecho).
  `services/quarkus/analytics-streams` con la **extensión de Kafka Streams** de Quarkus: la
  topología se *produce* (`@Produces Topology`) y **las dos topologías se copiaron tal cual**
  (solo cambia el tipo de la configuración), que es la conclusión de la fase: la API de Streams
  no cambia, cambia quién la envuelve. La extensión trae la **sonda de salud del motor**
  (`/q/health`: estado y topics disponibles) que en Spring fueron 40 líneas propias, y
  `quarkus.kafka-streams.topics` **espera a que existan los topics**, que es la carrera que en
  la Fase 6 se resolvió a mano en el script.
  Verificado en vivo: con el mismo `application-id` y los mismos topics, el port **consumió lo
  que producía el normalizer de Spring**, calculó métricas (`market.analytics` a 242 msg/s) y la
  **consulta interactiva devolvió el mismo JSON**. Los **6 tests de topología**, portados y en
  verde. Números: arranque **1,548 s** frente a 3,464 s; el RSS salió algo más alto (505 frente
  a 455 MB), pero en este servicio manda el estado y las dos medidas no se tomaron en el mismo
  momento.
  **Hallazgo grande, y no es de frameworks**: Avro 1.12.2 (el que trae el BOM de Quarkus)
  **enciende el validador de clases**, y los serdes de Streams revientan con
  `Forbidden com.aggora.avro.canonical.CanonicalTick`; la implementación Spring usa 1.12.1 y por
  eso hoy no le pasa. Arreglado con `quarkus.avro.trusted-packages` y, en los tests,
  `org.apache.avro.SERIALIZABLE_PACKAGES`.
- ✅ **Fase 8 — cuarto servicio portado: `portfolio-risk`** (hecho). La receta de la analítica se
  repite: la topología se copia tal cual y el montaje es `@Produces Topology`. La decisión propia de
  este servicio es **no** tener servidor web, métricas ni sonda, porque la versión Spring tampoco
  las tiene. Verificado en vivo con el detalle bonito: el port de Quarkus consumió
  `orders.executions` **publicado por el motor de matching de Spring con transacciones** y calculó
  posiciones, o sea el exactly-once de la Fase 4 cruzando las dos implementaciones. Números:
  arranque **1,066 s** frente a 2,023 s y RSS **306 MB** frente a 374 MB (la mayor diferencia de
  arranque de todo el port, −47%).
- ✅ **Fase 8 — quinto servicio portado: `order-matching-engine`** (hecho). El del **exactly-once**, y
  donde el montaje deja de parecerse: Spring cuelga un `KafkaTransactionManager` del contenedor
  (configuración), SmallRye lo pide en el código con
  `@Channel("executions") KafkaTransactions<Execution>` y `withTransactionAndAck`. El libro de
  órdenes se copió tal cual. **El experimento de la Fase 4 repetido en Quarkus**: el mismo topic
  leído con los dos niveles de aislamiento dio **read_committed 83.572** frente a **read_uncommitted
  83.582** (diez ejecuciones abortadas que existen y no se ven), y la orden venenosa llegó a
  `orders.incoming.DLT` con la cabecera `x-dlt-reason`. Números: arranque **1,290 s** frente a
  1,987 s y RSS **253 MB** frente a 268 MB. Dos gotchas apuntados: el nombre de la variable de
  entorno (**Spring admite `AGGORA_FAILEVERYNORDERS`, Quarkus exige `AGGORA_FAIL_EVERY_N_ORDERS`**)
  y el validador de Avro 1.12.2, que vuelve a aparecer porque este módulo también construye
  registros Avro en sus tests.
- ✅ **Fase 8 — sexto servicio portado: `alerting-service`** (hecho). El port más rápido de todos: los
  tres ficheros de topología se copiaron tal cual, **incluido el punctuator** del detector de feed
  parado (que es API de Kafka Streams, no del framework). Verificado en vivo con datos reales:
  levantó los dos tipos de alerta, `[alerta] pico de precio en USD/CNY` y
  `[alerta] CRITICAL MARGIN_BREACH`. Números: arranque **1,073 s** frente a 1,870 s y RSS **256 MB**
  frente a 383 MB.
- ✅ **Fase 8 — séptimo servicio portado: `audit-log`** (hecho), y con este **están los siete**. El
  outbox con Postgres: JDBC a pelo sobre el `DataSource` de Agroal y `@Transactional` de Jakarta con
  Narayana (el equivalente de `JdbcTemplate` + `@Transactional` de Spring), el **mismo `schema.sql`**
  ejecutado al arrancar (Quarkus no lo hace solo; el sitio bueno sería Flyway) y **tres canales
  tipados** en vez de un `@KafkaListener` con el valor como `Object` (aquí Quarkus obliga a algo más
  seguro de tipos). Verificado en vivo: creó las tablas, consumió los tres topics, **190.628 eventos
  auditados** y la outbox drenándose de verdad (**190.622 publicados, 6 pendientes**), sin errores.
  Números: arranque **1,468 s** frente a 2,157 s y RSS **296 MB** frente a 354 MB.
- ⏭️ **Fase 8 (lo que queda)** — la **imagen nativa de GraalVM** para dos servicios y las medidas, y
  correr los dos stacks a la vez (con `group.id` y topics de salida propios).
  El nativo **intentado y bloqueado en esta máquina**: la receta corregida está en
  `scripts/build-native.sh` y los cuatro tropiezos (la imagen del builder no trae Maven, `-Dnative`
  no hace nada sin el perfil `native` en el pom, el contenedor de Maven necesita el CLI de docker
  dentro, y el tag del builder tiene que cuadrar con la versión de Quarkus) están en
  `docs/decisions.md`. Se compila en una máquina con Mandrel instalado, y luego se mide con
  `scripts/measure-service.sh` igual que la JVM.

## Arranque rápido (todo desde el devcontainer)
```bash
# 0) Infraestructura: Kafka + Schema Registry.
#    Ojo: los datos viven dentro de los contenedores, así que recrearlos los borra;
#    los topics y los esquemas se vuelven a crear solos al arrancar los servicios.
docker compose -f infra/docker-compose.yml up -d

# 1) Compilar
cd /workspaces/aggora/services
mvn -q -DskipTests package

# 2) Los 7 servicios, en orden y esperando a que cada uno esté listo.
#    El orden importa: Kafka Streams falla si su topic de origen no existe todavía,
#    y los topics los crea el servicio que escribe en ellos.
export TWELVEDATA_API_KEY=...      # EEUU, forex, oro y ETFs
export ALPHAVANTAGE_API_KEY=...    # Euronext y Shanghai (25 peticiones/día)
cd /workspaces/aggora
bash scripts/start-services.sh     # logs en /tmp/<servicio>.log
bash scripts/stop-services.sh      # para pararlos todos

# 3) Comprobación rápida: los contratos registrados y una consulta al state store
curl -s localhost:8081/subjects | head -c 200
curl -s 'localhost:8085/analytics?symbol=EUR/USD&minutes=3' | head -c 300
```
Comprobaciones (desde WSL/Windows, que es donde tienes la CLI de Kafka):
```bash
docker exec aggora-kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --topic market.ticks.raw

docker exec aggora-kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group ingestion-normalizer

# Ver mensajes en crudo, con su key
docker exec aggora-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic market.ticks.raw \
  --max-messages 3 --property print.key=true
```
Experimentos de la fase:
1. **Rebalanceo** — arranca una segunda instancia del normalizer (otra terminal,
   mismo comando): el grupo reparte 3/3 y ambos logs lo cuentan.
2. **At-least-once** — arranca el normalizer con
   `AGGORA_PROCESSINGDELAYMS=400 SPRING_KAFKA_LISTENER_ACK_MODE=manual SPRING_KAFKA_CONSUMER_MAXPOLLRECORDS=10`,
   mátalo con `kill -9` a media faena y vuelve a arrancarlo: reanuda en el último
   offset confirmado y **reprocesa** lo que había leído sin confirmar.
3. **Datos reales** — exporta las dos keys antes de arrancar el simulador y observa
   los logs `[reference]` y los ticks con `fuente=REFERENCE`:
   ```bash
   export TWELVEDATA_API_KEY=...      # EEUU, forex, oro y ETFs
   export ALPHAVANTAGE_API_KEY=...    # Euronext París y Shanghai
   ```
   Si guardas la de Alpha Vantage en `~/.secrets/alphavantage` (fuera del repo):
   `export ALPHAVANTAGE_API_KEY=$(cat ~/.secrets/alphavantage)`.

## Si el IDE se queja (marcadores rojos en los `.pom`)
m2e (el compilador automático de VSCode/Cursor) puede quedarse con un error viejo
aunque Maven en la terminal funcione. **No es un fallo del proyecto.** Arreglo:

1. `Ctrl+Shift+P` → **Maven: Reload All Projects**.
2. Si el marcador sigue: `Ctrl+Shift+P` → **Java: Clean Java Language Server
   Workspace** y acepta la recarga de la ventana.

Causa típica: carpetas `target/` generadas por **otro usuario** (root) y luego
recompiladas; m2e conserva el mensaje de `Permission denied` aunque el fichero ya
sea suyo.

**Regla para agentes:** compilar y ejecutar SIEMPRE con `docker exec -u vscode`
(el usuario del devcontainer), nunca como root. Si se compila como root, los
`target/` quedan sin permiso de escritura para el usuario y el IDE falla.

### Verificar la Fase 3 (analitica)
```bash
# Consulta interactiva al state store en marcha (puerto 8085)
curl -s "localhost:8085/analytics?symbol=EUR/USD&minutes=3"

# Los topics nuevos de la fase
docker exec aggora-kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --topic market.fx.reference   # compactado
docker exec aggora-kafka-1 /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 --topic market.arbitrage
```
# Arrancar el tercer servicio (necesita el pipeline de la Fase 1-2 en marcha)
cd /workspaces/aggora/services/spring/analytics-streams
java -jar target/analytics-streams-0.1.0-SNAPSHOT.jar

# Lo mas comodo: sus logs muestreados, con las metricas ya calculadas
#   [metricas] EUR/USD HOPPING ... | ticks=40 volumen=8536 vwap=1.1510 media=1.1512 volatilidad=0.0018

# Los topics internos que crea Kafka Streams para el estado (changelog)
docker exec aggora-kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list | grep analytics

# Cuantos mensajes lleva el topic de metricas
docker exec aggora-kafka-1 /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 --topic market.analytics

curl -s localhost:8081/subjects   # ahora tambien market.analytics-value
```

### Verificar la Fase 6 (clúster de 3 brokers)
```bash
# El quórum de KRaft: 3 votantes y quién manda
docker exec aggora-kafka-1 /opt/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-server localhost:9092 describe --status | head -6

# Un topic: 3 réplicas y quién está al día (ISR)
docker exec aggora-kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --topic market.ticks.raw | head -3

# EXPERIMENTO: matar un broker y ver que el ISR baja a 2 y el líder se mueve
docker stop aggora-kafka-2
docker exec aggora-kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --topic market.ticks.raw | head -3
docker start aggora-kafka-2

# EXPERIMENTO: matar dos y ver que la escritura se para (offsets congelados)
docker stop aggora-kafka-2 aggora-kafka-3
docker exec aggora-kafka-1 /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 --topic market.ticks.raw
docker start aggora-kafka-2 aggora-kafka-3
```

### Verificar la Fase 6 (descartes y reintentos)
```bash
# Los topics de reintento y descarte que se crean solos
docker exec aggora-kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list \
  | grep -E "retry|DLT"

# Ticks invalidos descartados con su motivo (arrancar el simulador con
# AGGORA_INVALIDTICKEVERYN=500)
docker exec -u vscode eager_allen bash -lc 'grep "DLT\]" /tmp/norm3.log | tail -3'
docker exec aggora-kafka-1 /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 --topic market.ticks.raw.DLT

# Ordenes venenosas al DLT (arrancar el motor con AGGORA_FAILEVERYNORDERS=20), y comprobar
# que el motor SIGUE procesando ordenes: la particion no se bloquea
docker exec aggora-kafka-1 /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 --topic orders.incoming.DLT
docker exec -u vscode eager_allen bash -lc 'grep "matching\]" /tmp/matching4.log | tail -2'

# Reintentos de la auditoria: parar Postgres y ver como los eventos esperan
docker stop aggora-postgres && sleep 40
docker exec aggora-kafka-1 /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 --topic alerts.raised.retry-500
docker start aggora-postgres
```

### Verificar la Fase 5 (cartera, alertas y auditoria)
```bash
# Alertas (los tres tipos) y feed parado
docker exec -u vscode eager_allen bash -lc 'grep "alerta\]" /tmp/alerting.log | tail -5'

# Auditoria: lo que hay guardado en Postgres, en SQL y legible
docker exec aggora-postgres psql -U aggora -d aggora \
  -c "select entity_type, count(*) from audit_events group by 1 order by 2 desc;"
docker exec aggora-postgres psql -U aggora -d aggora \
  -c "select count(*) filter (where published_at is null) as pendientes from audit_outbox;"
docker exec aggora-postgres psql -U aggora -d aggora \
  -c "select entity_id, left(payload, 80) from audit_events order by id desc limit 3;"

# El topic de auditoria es compactado
docker exec aggora-kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --describe --topic audit.events | head -2
```

### Verificar la Fase 5 (cartera)
```bash
cd /workspaces/aggora/services/spring/portfolio-risk
java -jar target/portfolio-risk-0.1.0-SNAPSHOT.jar
# Sus logs muestreados son la forma comoda de verlo:
#   [cartera] ACC-05 ASML.AMS | cantidad=-141 coste medio=1384.4712 P&L realizado=369.7456 exposicion=195210.4389 EUR

curl -s localhost:8081/subjects/orders.executions-value/versions   # [1,2]: el cambio compatible

docker exec aggora-kafka-1 /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 --topic portfolio.updates
```

### Verificar la Fase 4 (exactly-once)
```bash
# Arrancar el motor (el simulador ya genera ordenes)
cd /workspaces/aggora/services/spring/order-matching-engine
java -jar target/order-matching-engine-0.1.0-SNAPSHOT.jar

# El experimento: arrancarlo inyectando un fallo cada 10 ordenes, DESPUES de publicar
AGGORA_FAILEVERYNORDERS=10 java -jar target/order-matching-engine-0.1.0-SNAPSHOT.jar

# Y comparar lo que ve cada tipo de consumidor sobre el MISMO topic:
docker exec aggora-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic orders.executions --from-beginning --timeout-ms 8000 \
  --consumer-property isolation.level=read_committed | wc -l
docker exec aggora-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic orders.executions --from-beginning --timeout-ms 8000 \
  --consumer-property isolation.level=read_uncommitted | wc -l
# read_uncommitted cuenta mas: son las ejecuciones abortadas, que no cuentan para nadie.
```

### Verificar la Fase 2 (con el pipeline arrancado)
```bash
# Los esquemas que se han registrado, con su versión e ID
curl -s localhost:8081/subjects
curl -s localhost:8081/subjects/market.ticks.raw-value/versions
curl -s localhost:8081/config

# El topic ya NO es texto legible: byte mágico 0x00 + 4 bytes de ID de esquema + Avro
docker exec aggora-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic market.ticks.raw \
  --from-beginning --max-messages 1 --timeout-ms 8000 | head -c 60 | cat -v

# Y el evento canónico, que produce el normalizer
docker exec aggora-kafka-1 /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 --topic market.ticks.canonical
```

### Verificar la Fase 8 (el port de Quarkus)
```bash
# Compilar las dos implementaciones (el agregador las conoce a las dos)
cd /workspaces/aggora/services
mvn -q -DskipTests package

# Parar el normalizer de Spring y arrancar el de Quarkus: mismos topics, mismo group.id
pkill -TERM -f "target/ingestion-normalizer-spring"
cd /workspaces/aggora/services/quarkus/ingestion-normalizer
setsid java -jar target/quarkus-app/quarkus-run.jar > /tmp/ingestion-normalizer-quarkus.log 2>&1 &

# Lo que hay que ver:
#   [topics] market.ticks.canonical ya existe, no se toca
#   [rebalance] ASIGNADAS 6 particiones: [...]
#   [canonico] EUR/USD FX USD precio=... -> canonical(part=.. offset=..)
grep -E "\[topics\]|\[rebalance\]|\[canonico\]" /tmp/ingestion-normalizer-quarkus.log | head

# La prueba de fuego: la analitica de SPRING sigue funcionando con el normalizer de QUARKUS
curl -s -o /dev/null -w "%{http_code}\n" 'localhost:8085/analytics?symbol=EUR/USD&minutes=3'

# Y el camino de error: con el simulador inyectando ticks invalidos
#   (arrancarlo con AGGORA_INVALIDTICKEVERYN=300), el DLT lleva el motivo en una cabecera
docker exec aggora-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic market.ticks.raw.DLT --from-beginning --max-messages 1 \
  --property print.headers=true --property print.key=true

# Para volver a la version Spring
pkill -TERM -f "quarkus-app/quarkus-run.jar"
bash scripts/start-services.sh
```

### Verificar la Fase 8 (el port de la cartera)
```bash
pkill -TERM -f "target/portfolio-risk-spring"
cd /workspaces/aggora/services/quarkus/portfolio-risk
setsid java -jar target/quarkus-app/quarkus-run.jar > /tmp/portfolio-risk-quarkus.log 2>&1 &
#   portfolio-risk 0.1.0-SNAPSHOT on JVM (powered by Quarkus 3.39.3) started in 1.066s
#   [cartera] ACC-01 USO | cantidad=806 coste medio=... P&L realizado=... exposicion=...
grep -E "started in|\[cartera\]" /tmp/portfolio-risk-quarkus.log | tail -3

# Para volver a la versión Spring
pkill -TERM -f "quarkus-app/quarkus-run.jar"
cd /workspaces/aggora && bash scripts/start-services.sh
```

### Verificar la Fase 8 (el port de la analítica)
```bash
# Parar la analítica de Spring y arrancar la de Quarkus (mismo puerto, mismo application-id)
pkill -TERM -f "target/analytics-streams-spring"
cd /workspaces/aggora/services/quarkus/analytics-streams
setsid java -jar target/quarkus-app/quarkus-run.jar > /tmp/analytics-streams-quarkus.log 2>&1 &

# Lo que hay que ver:
#   analytics-streams 0.1.0-SNAPSHOT on JVM (powered by Quarkus 3.39.3) started in 1.548s
#   [topics] market.analytics ya existe, no se toca
#   [metricas] JPM HOPPING 2026-09-15T19:37:... | ticks=... vwap=... volatilidad=...
grep -E "started in|\[topics\]|\[metricas\]" /tmp/analytics-streams-quarkus.log | head

# La consulta interactiva: el MISMO JSON que devuelve la de Spring
curl -s 'localhost:8085/analytics?symbol=EUR/USD&minutes=3' | head -c 300

# La sonda de salud del motor, que trae la extensión (en Spring eran 40 líneas propias)
curl -s localhost:8085/q/health

# Y las métricas, con las mismas etiquetas que la versión Spring
curl -s localhost:8085/q/metrics | grep jvm_threads_live_threads

# Para volver a la versión Spring
pkill -TERM -f "quarkus-app/quarkus-run.jar"
cd /workspaces/aggora && bash scripts/start-services.sh
```

### Verificar la Fase 8 (el port del simulador)
```bash
cd /workspaces/aggora/services/quarkus/market-data-simulator
export ALPHAVANTAGE_API_KEY=$(cat ~/.secrets/alphavantage)
setsid java -jar target/quarkus-app/quarkus-run.jar > /tmp/market-data-simulator-quarkus.log 2>&1 &

grep -E "started in|\[universo\]|\[reference\]|\[topics\]" /tmp/market-data-simulator-quarkus.log | head
#   market-data-simulator 0.1.0-SNAPSHOT on JVM (powered by Quarkus 3.39.3) started in 1.425s
#   [universo] 14 instrumentos configurados
#   [reference] ALPHA_VANTAGE configurado | 6 instrumentos | presupuesto diario 18
#   [topics] market.ticks.raw ya existe, no se toca

# El ritmo de ticks (deberia rondar los 40/s con los mercados de EEUU y FX abiertos)
A=$(docker exec aggora-kafka-1 /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 \
     --topic market.ticks.raw | awk -F: '{s+=$3} END {print s}'); sleep 10
B=$(docker exec aggora-kafka-1 /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 \
     --topic market.ticks.raw | awk -F: '{s+=$3} END {print s}'); echo "$(( (B-A)/10 )) msg/s"

curl -s -o /dev/null -w "metrics: %{http_code}\n" localhost:8080/q/metrics

# Para volver a la version Spring
pkill -TERM -f "quarkus-app/quarkus-run.jar"
cd /workspaces/aggora && bash scripts/start-services.sh
```

### Verificar la Fase 8 (arranque, memoria y salud)
```bash
cd /workspaces/aggora
bash scripts/measure-service.sh                       # arranque, RSS, hilos y ficheros

# La sonda de salud del motor y la metrica que ve Prometheus
curl -s localhost:8085/actuator/health
curl -s localhost:8085/actuator/prometheus | grep aggora_kafka_streams_running

# Y desde Prometheus/Grafana
curl -s 'localhost:9090/api/v1/targets?state=active' | grep -o '"job":"aggora-apps"'
http://localhost:3000/d/aggora-kafka      # paneles de motor, arranque, hilos, memoria y CPU
```

### Verificar la Fase 7 (evolución de esquemas)
```bash
# El laboratorio entero: pregunta, enseña los veredictos de las dos direcciones, intenta
# registrar lo que rompe (409), registra lo que no, y deja el registro como estaba.
bash scripts/schema-evolution-lab.sh

# La traducción de Avro, sin broker ni registro
cd /workspaces/aggora/services
mvn -q -pl spring/ingestion-normalizer test -Dtest=SchemaEvolutionTest
```

## Decisiones tomadas (link a docs/decisions.md para el detalle)
- Kafka en modo KRaft (sin Zookeeper) — más simple, es lo moderno.
- Imagen `apache/kafka` y no `confluentinc/cp-kafka` — Apache puro,
  sin dependencias de Confluent para el broker (SR sí lo es).
- Red `aggora-net` externa y compartida — evita `host.docker.internal`.
- Doble listener INTERNAL/EXTERNAL — necesario por WSL + devcontainer.
- Java 21 LTS para ambas implementaciones (Spring y Quarkus).
- Datos reales con **dos proveedores**: Twelve Data (EEUU, forex, oro, ETFs) y
  Alpha Vantage (Euronext y Shanghai), elegidos tras comprobar que ningún plan
  gratuito cubre los tres husos horarios del spec (ver `docs/decisions.md`).
- Sin módulo `common` compartido: cada servicio define su copia del contrato,
  que es el dolor que justifica el Schema Registry de la Fase 2.

## Cosas que NO hacer
- No meter todo el proyecto de golpe. Fase por fase.
- No cambiar `docker-compose.yml` sin explicar por qué en la respuesta.
- No usar `host.docker.internal` (no es portable fuera de Docker Desktop).
- No cambiar versiones de imágenes "porque son más nuevas" sin avisar.
- No asumir que el usuario conoce Kafka Streams, Schema Registry,
  transacciones, etc. Explicar antes de usar.

## Cuando termines una fase
1. Pide al usuario que verifique con comandos concretos (no "pruébalo").
2. Espera a que confirme antes de proponer la siguiente fase.
3. Actualiza la sección "Estado actual" de este archivo.