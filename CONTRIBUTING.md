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
    (`host.docker.internal:8080`, `:8085` y `:8089` de Spring; `:8185` y `:8189` de Quarkus en un
    trabajo aparte, porque su ruta es `/q/metrics`; los servicios corren en el devcontainer y su
    nombre de contenedor no resuelve desde `aggora-net`). **Trampa que costo un rato**: solo
    responde para los puertos reenviados al host de WSL2, asi que un puerto sin reenviar da
    `connection refused` y parece un servicio caido; los dos gateways estan en `forwardPorts`.
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
- ✅ **Fase 9 — el informe comparativo** (hecho): `SPRING_VS_QUARKUS.md`, con los números de los siete
  servicios, el modelo imperativo frente al reactivo, los tropiezos y una recomendación honesta.
- ✅ **Tests de integración con Testcontainers** (hecho, y es lo que faltaba): dos `*IT` en failsafe
  (`mvn verify`) —`ExactlyOnceKafkaIT` con Kafka y el Schema Registry de verdad, y
  `OutboxPostgresIT` contra un Postgres real— más el **workflow de CI** (`.github/workflows/ci.yml`):
  unitarios en cada push, integración en cada PR. **No se han podido ejecutar aquí**: el devcontainer
  no tiene Docker, igual que con el nativo. Compilan y corren en el CI.
  - **El IT de Quarkus nunca se ejecutaba**, y se descubrió al revisar qué corre de verdad en el CI:
    surefire ignora `*IT` por convención, el padre de Quarkus no tenía failsafe y el job de
    integración solo listaba dos módulos de Spring. Arreglado: failsafe en `<plugins>` del padre de
    Quarkus (en `<pluginManagement>` no vale: configura, no activa) y el módulo añadido al job.
    Verificado en local, con contenedores de verdad:
    ```bash
    docker exec -u vscode -e TESTCONTAINERS_RYUK_DISABLED=true <devcontainer> \
      bash -lc 'cd /workspaces/aggora/services && mvn verify -pl quarkus/ingestion-normalizer -am'
    # -> Tests run: 1, Failures: 0, Errors: 0  (38 s, con Kafka y Schema Registry por Dev Services)
    ```
    `TESTCONTAINERS_RYUK_DISABLED=true` es lo que faltaba: el motor de Docker sí se alcanza desde el
    devcontainer; lo que no se alcanza es **Ryuk**. Y los `*IT` de **Spring también** corren en local
    desde que su Testcontainers subió a la 2.x (la misma línea que Quarkus): antes, la 1.21.3 que fija
    Spring Boot no negociaba con el socket de Docker Desktop. Los tres IT, en local:
    ```bash
    docker exec -u vscode -e TESTCONTAINERS_RYUK_DISABLED=true <devcontainer> \
      bash -lc 'cd /workspaces/aggora/services && mvn verify -pl spring/audit-log,spring/order-matching-engine,quarkus/ingestion-normalizer -am'
    # ExactlyOnceKafkaIT 1/1 · OutboxPostgresIT 2/2 · NormalizerKafkaIT 1/1
    ```
    Detalle (y las tres aristas de la migración a 2.x) en `docs/dev-environment.md`.
  - Ojo con `ExactlyOnceKafkaIT`: falló en CI dos veces y **el primer diagnóstico fue falso** ("es una
    carrera al leer"). La causa real es que `send()` es asíncrono y `abortTransaction()` descarta lo
    que el hilo emisor no ha mandado, así que el registro abortado no llegaba a existir. El test ahora
    **comprueba** que está en el log (`read_uncommitted` con la transacción abierta) antes de abortar.
    Medido con `scripts/ExactlyOnceRaceCheck.java` contra el clúster local (no necesita Testcontainers):
    secuencia vieja **8 fallos de 8**, secuencia nueva **0 de 8**. Detalle en `docs/decisions.md`.
- ✅ **Los dos stacks a la vez** (hecho): `bash scripts/start-quarkus-stack.sh` levanta la
  implementación de Quarkus **en paralelo** con la de Spring, misma entrada y salidas propias
  (`market.analytics.q`, `portfolio.updates.q`, …), cada motor de Streams con su `application-id` y
  su estado. Verificado: los dos pipelines procesan a la vez. Detalle en `docs/decisions.md`.
- ✅ **Fase 8 — cerrada**: los siete servicios portados, los dos stacks corriendo a la vez y la
  **imagen nativa de GraalVM para dos servicios** (`ingestion-normalizer` y `market-data-simulator`):
  arranque **0,022 s** y RSS **114-124 MB**, frente a 1,123 s y 332 MB de la JVM de Quarkus y
  2,099 s y 428 MB de Spring. Receta en `scripts/build-native.sh`, gotchas en `docs/decisions.md` y
  el análisis del entorno en `docs/dev-environment.md`.
- ✅ **Test de estrés de throughput** (hecho): `scripts/throughput-test.sh` + `scripts/AvroLoadGenerator.java`
  empujan los dos pipelines a la vez con la MISMA entrada (el generador escribe Avro válido en
  `market.ticks.raw`; el simulador se para durante el experimento) y miden mensajes procesados,
  pico de lag por grupo y CPU por mensaje desde `/proc`.
  - Medido (ráfagas de 20 s): a 1.500 msg/s y a 4.000 msg/s **los dos procesan el 100%** (29.998 y
    79.996 en el canónico, 149.990 y 399.980 en la analítica, idénticos en los dos stacks) sin un
    solo descarte.
  - **El cuello de botella es el normalizer**, no el motor de Streams: pico de lag 67.003 (Spring) y
    62.979 (Quarkus) frente a 1.639 y 0 en la analítica.
  - **El coste sí difiere**: el normalizer de Quarkus gasta 2,8 veces más CPU que el de Spring para
    el mismo trabajo; los dos motores de Streams gastan lo mismo (es la misma librería).
  - Y el primer intento **no midió nada, y fue el hallazgo**: sin límites de heap, 13 JVM sobre 23 GB
    se asfixian (75.055 GC completos, Tomcat sin contestar, productor colgado con **lag 0** y
    ~18.000 mensajes perdidos en silencio). Arreglado con los límites de heap que ya tenía la unidad
    de systemd del despliegue, ahora también en los scripts locales. Informe completo en
    `docs/throughput-lab.md`.
- ✅ **Fase 9 (cierre)** — `gateway-ws`: el **fan-out** a un frontend, en las dos implementaciones.
  Consume `market.ticks.canonical`, `portfolio.updates` y `alerts.raised` con **grupo de consumo
  propio** (por eso recibe su copia de cada registro en vez de quitarle particiones a la analitica)
  y reparte cada evento por WebSocket a todos los navegadores conectados.
  - **Spring** (`services/spring/gateway-ws`, puerto 8089): Jakarta WebSocket (`@ServerEndpoint`) +
    `getAsyncRemote()` para no frenar el consumo, y `static/index.html` como pagina de demo.
  - **Quarkus** (`services/quarkus/gateway-ws`, puerto 8189): WebSockets Next (`@WebSocket`), donde
    `sendText` ya devuelve `Uni<Void>`, y el **mismo** `index.html` copiado a `META-INF/resources`.
  - Se puede arrancar el de Quarkus a mano mientras el stack de Spring corre, para ver las dos
    paginas a la vez (es la demostracion de que un grupo de consumo reparte y otro copia):
    ```bash
    cd /workspaces/aggora/services/quarkus/gateway-ws
    nohup java -jar target/quarkus-app/quarkus-run.jar > /tmp/gateway-ws-quarkus.log 2>&1 &
    ```
  - Verificacion (dentro del devcontainer; los dos comandos valen para el 8089 y el 8189):
    ```bash
    curl -s -o /dev/null -w "%{http_code}\n" localhost:8089/          # 200: la pagina
    java scripts/GatewayLiveCheck.java localhost 8089 12               # 101 + eventos por tipo
    ```
    El chequeo habla WebSocket con la libreria del JDK, asi que no anade ninguna dependencia, y
    falla con codigo 1 si no llega ningun tick. Medido: 180 ticks, 10 posiciones y 30 alertas
    (Spring) / 11 (Quarkus) en 12 s con los dos stacks en marcha.

- ✅ **Fase 10** — despliegue a AWS, en artefactos validados (no desplegado: no hay cuenta).
  - `deploy/README.md` es el runbook (en ingles); `deploy/terraform/` es la IaC; `deploy/systemd/aggora@.service`
    es **una** unidad plantilla para los siete servicios; `deploy/user-data.sh` es el arranque de la VM;
    `deploy/docker-compose.vm.yml` es Postgres + Prometheus + Grafana + kafka-exporter (sin Kafka ni SR:
    son gestionados); `deploy/collect-artifacts.sh` compila y junta los jars.
  - `services/lambda/` es el **tercer arbol**: el normalizer sin estado como funcion de Lambda.
  - Decisiones: el broker y los topics se crean **a mano** (Terraform no debe poder crear lo que
    factura por hora); los secretos van a SSM `SecureString`; Lambda fuera de la VPC (broker publico
    con SASL); `Restart=always` de systemd en vez del script de arranque con esperas.
  - Verificacion (la buena de Terraform monta la **raiz** del repo, no solo `deploy/terraform`):
    ```bash
    cd /workspaces/aggora/services && mvn -pl lambda/ingestion-normalizer-lambda -am package
    cd /workspaces/aggora
    docker run --rm -v "$PWD":/w -w /w/deploy/terraform hashicorp/terraform:1.9 fmt -check -recursive
    docker run --rm -v "$PWD":/w -w /w/deploy/terraform hashicorp/terraform:1.9 init -backend=false -input=false
    docker run --rm -v "$PWD":/w -w /w/deploy/terraform hashicorp/terraform:1.9 validate
    docker compose -f deploy/docker-compose.vm.yml config -q
    ```
    Resultado: 5 tests de la Lambda en verde, `Success! The configuration is valid.` y compose en 0.
  - Al reconciliar el handler con la IaC aparecieron **tres desajustes** que ninguna de las dos partes
    veia por separado: los nombres de las variables de entorno, la referencia de divisas
    (`market.fx.reference`, que el pipeline desplegado necesita para el join) y el jar que se subia
    (el fino en vez del gordo del shade). Los tres arreglados; el detalle esta en `docs/decisions.md`.

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
- No usar `host.docker.internal` (no es portable fuera de Docker Desktop) salvo para el scrape de
  Prometheus, que es el unico caso donde no hay alternativa (esta documentado en `decisions.md`).
- No compartir `group.id` entre un servicio de trabajo y el gateway: un grupo reparte, no copia.
- No meter el broker gestionado (ni los topics) en Terraform: lo que factura por hora y guarda
  estado se crea a mano y se documenta, no se describe como algo que se puede `destroy`.
- No versionar nunca `deploy/artifacts/*.jar` (son artefactos de build) ni el estado de Terraform
  (lleva las credenciales de Kafka dentro).
- No cambiar versiones de imágenes "porque son más nuevas" sin avisar.
- No asumir que el usuario conoce Kafka Streams, Schema Registry,
  transacciones, etc. Explicar antes de usar.

## Cuando termines una fase
1. Pide al usuario que verifique con comandos concretos (no "pruébalo").
2. Espera a que confirme antes de proponer la siguiente fase.
3. Actualiza la sección "Estado actual" de este archivo.