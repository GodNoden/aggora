# README.md — Instrucciones para agentes de código

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
  - `kafka:9092` (listener INTERNAL)
  - `schema-registry:8081`
  - `postgres:5432`
  - `prometheus:9090`
- Desde WSL/Windows (fuera de Docker), los servicios se acceden por `localhost`
  y el puerto publicado:
  - `localhost:29092` (listener EXTERNAL de Kafka)
  - `localhost:8081` (Schema Registry)
  - `localhost:5432` (Postgres)
  - `localhost:8090` (Redpanda Console)
  - `localhost:9090` (Prometheus)
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
- `README.md` — este archivo. Actualizar al cerrar cada fase.
- `docs/` — decisiones e infra detallada (`decisions.md`) y guía de conceptos
  Kafka en lenguaje llano (`kafka-101.md`).
- `infra/` — docker-compose, configs de Prometheus/Grafana.
- `services/` — microservicios (multi-módulo Maven):
  - `market-data-simulator` — produce ticks a `market.ticks.raw`.
  - `ingestion-normalizer` — los consume con commit manual.

## Estado actual (actualizar al cerrar cada fase)
- ✅ **Fase 0a** — Kafka solo, red `aggora-net` creada y verificada
  desde el devcontainer (`nc -zv kafka 9092` → succeeded).
- ⚠️ **Fase 0b** — el compose perdió los servicios auxiliares respecto a lo que
  decía este README. Se reconstruyen en la fase que los use: **Schema Registry ya
  está (Fase 2)**; Postgres en Fase 5; Redpanda Console, kafka-exporter, Prometheus
  y Grafana en Fase 6. Ver `docs/decisions.md`.
- ✅ **Fase 1** — `market-data-simulator` y `ingestion-normalizer` (Spring Boot 4.1.1,
  Java 21, multi-módulo Maven en `services/`). Verificado end-to-end:
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
- 🔄 **Fase 3 (en curso)** — Y ya estan las **métricas por ventana**: el modulo
  `analytics-streams` (Kafka Streams con la API a pelo) lee `market.ticks.canonical` y
  calcula **VWAP, media y volatilidad** con **dos tipos de ventana** (fija de 30 s y
  movil de 60 s recalculada cada 15 s) hacia `market.analytics`. Los agregados viven en
  **state stores** con su topic de changelog, asi que sobreviven a un reinicio.
  Verificado: 3 tests de la topologia con `TopologyTestDriver` (sin broker), servicio
  arrancado y metricas reales publicandose.
  **Pendiente de la fase**: el **spread de arbitraje de ASML** (join entre sus dos
  cotizaciones, con conversion EUR->USD) y las **consultas interactivas** al state store.
- 🧱 **Infra añadida en la Fase 2** — `KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"`
  (un typo en un nombre de topic debe fallar, no crear un topic fantasma de 1
  partición) y un servicio `kafka-init` que crea los topics internos que no declara
  el código (`_schemas`). Las dos cosas tienen su porqué en `docs/decisions.md`.
- ✅ **Migración a Spring Boot 4.1.1** — pedida por el usuario por el aviso de
  soporte OSS. Verificado: 17 tests en verde, los dos servicios en marcha y 63.500
  mensajes procesados. Lo que cambió está en `docs/decisions.md`; en resumen:
  Jackson 3 (paquete `tools.jackson`), la autoconfiguración de Kafka pasa al módulo
  `spring-boot-kafka` (starter `spring-boot-starter-kafka`) y los serializadores
  JSON de Spring Kafka se llaman ahora `JacksonJson*`.

## Arranque rápido (todo desde el devcontainer)
```bash
# 0) Infraestructura: Kafka + Schema Registry.
#    Ojo: los datos viven dentro de los contenedores, así que recrearlos los borra;
#    los topics y los esquemas se vuelven a crear solos al arrancar los servicios.
docker compose -f infra/docker-compose.yml up -d

# 1) Compilar
cd /workspaces/aggora/services
mvn -q -DskipTests package

# 2) Productor (terminal 1). Al arrancar crea el topic de 6 particiones y,
#    al primer mensaje, registra el esquema Avro en el Schema Registry.
cd market-data-simulator
export TWELVEDATA_API_KEY=...      # EEUU, forex, oro y ETFs
export ALPHAVANTAGE_API_KEY=...    # Euronext y Shanghai (25 peticiones/día)
java -jar target/market-data-simulator-0.1.0-SNAPSHOT.jar   # su web queda en :8080

# 3) Consumidor + productor del canónico (terminal 2)
cd ../ingestion-normalizer
java -jar target/ingestion-normalizer-0.1.0-SNAPSHOT.jar

# 4) Comprobación rápida: los dos contratos registrados
curl -s localhost:8081/subjects
```
Comprobaciones (desde WSL/Windows, que es donde tienes la CLI de Kafka):
```bash
docker exec aggora-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --topic market.ticks.raw

docker exec aggora-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group ingestion-normalizer

# Ver mensajes en crudo, con su key
docker exec aggora-kafka /opt/kafka/bin/kafka-console-consumer.sh \
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
# Arrancar el tercer servicio (necesita el pipeline de la Fase 1-2 en marcha)
cd /workspaces/aggora/services/analytics-streams
java -jar target/analytics-streams-0.1.0-SNAPSHOT.jar

# Lo mas comodo: sus logs muestreados, con las metricas ya calculadas
#   [metricas] EUR/USD HOPPING ... | ticks=40 volumen=8536 vwap=1.1510 media=1.1512 volatilidad=0.0018

# Los topics internos que crea Kafka Streams para el estado (changelog)
docker exec aggora-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list | grep analytics

# Cuantos mensajes lleva el topic de metricas
docker exec aggora-kafka /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 --topic market.analytics

curl -s localhost:8081/subjects   # ahora tambien market.analytics-value
```

### Verificar la Fase 2 (con el pipeline arrancado)
```bash
# Los esquemas que se han registrado, con su versión e ID
curl -s localhost:8081/subjects
curl -s localhost:8081/subjects/market.ticks.raw-value/versions
curl -s localhost:8081/config

# El topic ya NO es texto legible: byte mágico 0x00 + 4 bytes de ID de esquema + Avro
docker exec aggora-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic market.ticks.raw \
  --from-beginning --max-messages 1 --timeout-ms 8000 | head -c 60 | cat -v

# Y el evento canónico, que produce el normalizer
docker exec aggora-kafka /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 --topic market.ticks.canonical
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