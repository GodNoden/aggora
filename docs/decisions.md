# Decisiones — Aggora

Registro corto de por qué las cosas están como están. Una entrada por decisión,
con la alternativa que se descartó.

## Fase 0 — Infraestructura

| Decisión | Por qué | Descartado |
|---|---|---|
| Kafka en modo **KRaft** | Sin Zookeeper: un proceso menos que mantener y es el modo actual de Kafka | Zookeeper |
| Imagen `apache/kafka` | Kafka puro; el broker no depende de Confluent | `confluentinc/cp-kafka` |
| Red **`aggora-net` externa** | La comparten devcontainer e infra sin `host.docker.internal` (no portable fuera de Docker Desktop) | red por defecto del compose |
| **Doble listener** INTERNAL/EXTERNAL | El devcontainer resuelve `kafka:9092`; el host Windows/WSL entra por `localhost:29092` | un solo listener |
| **Java 21 LTS** | Misma base para Spring Boot y Quarkus | 17 / 25 |

## Fase 1 — Spring Boot: fundamentos

| Decisión | Por qué | Descartado |
|---|---|---|
| Proveedor de datos: **Twelve Data** | De los candidatos del spec es el que más cubre en plan gratuito (EEUU + forex + oro + ETFs), y tiene endpoint por lotes | Finnhub (free tier centrado en US), Marketstack (EOD y ~100/mes) |
| **Segundo proveedor: Alpha Vantage** | Twelve Data cobra por Euronext y Shanghai, y Alpha Vantage los da gratis. Cada uno donde gana, en vez de pagar | pagar el plan de Twelve Data, o dejar esas patas sintéticas |
| **Sin API key todavía** | El simulador arranca igual: precios semilla + ticks 100% sintéticos, y avisa por log. Con `TWELVEDATA_API_KEY` / `ALPHAVANTAGE_API_KEY` exportadas pasa a datos reales sin tocar código | bloquear la fase esperando las keys |
| **Maven multi-módulo** | Ya está en el devcontainer y es el camino con menos sorpresas en Spring y Quarkus | Gradle |
| **Sin módulo `common` compartido** | Cada servicio define su propia copia del contrato. Es lo que pasa de verdad entre equipos y es exactamente el dolor que justifica el Schema Registry de la Fase 2 | `aggora-common` con los DTOs |
| **Infra: solo Kafka por ahora** | La Fase 1 es JSON plano: no necesita Schema Registry ni Postgres. Se reconstruye cada servicio en la fase que lo use (SR en Fase 2, Postgres en Fase 5, Grafana en Fase 6) | reconstruir ya los 7 servicios de la Fase 0b |
| Topic **creado por el productor** (`NewTopic`) | Particiones y nombre versionados en el repo; el consumidor nunca crea topics | crear a mano por CLI |
| **Key = symbol** | Todos los ticks de un instrumento caen en la misma partición y se procesan en orden | key aleatoria / sin key (round-robin) |
| Productor **idempotente** (`enable.idempotence`, `acks=all`) | Un reintento por fallo transitorio no duplica el mensaje | productor por defecto |
| **JSON sin cabeceras `__TypeId__`** | El contrato es el JSON, no la clase Java del emisor; cada servicio declara a qué clase deserializa | cabeceras de tipo de Spring |
| Consumidor con **commit manual** (`manual_immediate`) | El offset se confirma después de procesar: si el proceso muere, se relee (at-least-once) | auto-commit |
| **Sin DLT todavía** | El mensaje inválido se descarta con warning y se confirma. El topic de descartes es la Fase 6 | adelantar DLT |
| Horarios de mercado **sin festivos** | Un enum por exchange con su zona y sesiones (incluido el descanso de mediodía de Shanghai). Los festivos y las medias sesiones quedan pendientes | calendario completo desde el principio |
| Presupuesto de cuota **por proveedor** | Cada fuente declara su tope diario y el scheduler se para al llegar: 750 créditos en Twelve Data, 18 peticiones en Alpha Vantage | confiar en que el proveedor corte |
| **Una interfaz `ReferenceSource`** | Hay dos proveedores de verdad, con formatos y cuotas distintos. La abstracción se justifica ahora, no antes | `if (proveedor == ...)` repartido por el feed |

## Datos reales: dos proveedores (verificado 2026-09-14/15)

El plan gratuito de Twelve Data **no sirve todos los mercados del spec**. Probado
símbolo a símbolo contra las dos APIs:

| Mercado | Instrumentos | Twelve Data free | Alpha Vantage free |
|---|---|---|---|
| NASDAQ / NYSE | AAPL, JPM, XOM | ✅ | — |
| Forex | EUR/USD, USD/CNY | ✅ | — |
| Oro spot | XAU/USD | ✅ | — |
| ETFs de EEUU | USO (petróleo) | ✅ | — |
| Euronext París | MC, OR, AIR | ❌ pide Grow/Venture | ✅ `MC.PAR`, `OR.PAR`, `AIR.PAR` |
| SSE Shanghai | 600519, 601398 | ❌ pide Pro/Venture | ✅ `600519.SHH`, `601398.SHH` |
| Crudo spot | WTI/USD | ❌ pide Grow/Venture | solo serie **diaria**, no viva |
| Índices | SPX, NDX | ❌ pide Grow/Venture | — |

Decisión: **dos proveedores, cada uno donde gana**. Twelve Data para EEUU, forex y
oro (tiene endpoint por lotes y aguanta un poll cada 15 min); Alpha Vantage para
Euronext y Shanghai, que allí sí son gratis (una petición por símbolo, 1/segundo,
así que cada 3 h). Resultado: **los 12 instrumentos con precio real**.

**Crudo**: el spot no está gratis en ninguno de los dos. Alpha Vantage tiene la serie
`WTI` pero es diaria (cierre, no precio vivo), así que se usa el ETF `USO`, que sí da
precio vivo en Twelve Data. Está documentado en el YAML: **es el precio del ETF, no
el del barril**, y cotiza en horario de EEUU.

Cuotas descubiertas a golpes (y respetadas por el código):

- Twelve Data: **8 créditos/minuto y ~800/día**, y un crédito = **un símbolo pedido**,
  no una petición. Un poll de los 7 instrumentos = 7 créditos en 3 peticiones.
- Alpha Vantage: **25 peticiones/día y 1 petición/segundo**, sin endpoint por lotes.
  Cuentas: Euronext abierta 8,5 h -> 3 polls x 3 símbolos = 9; Shanghai 4,5 h ->
  2 polls x 2 = 4. Total ~13/día, con margen.
- Alpha Vantage **no usa códigos HTTP** para decir "te pasaste de cuota": responde
  200 con `{"Information": "..."}`. Si no se mira el cuerpo, ese aviso se cuela como
  si fuera un precio. Hay test para eso.

Tres correcciones al código que salieron de estas pruebas:

- `/price` de Twelve Data responde en **dos formatos**: `{"price":"333.47"}` si pides
  un solo símbolo y `{"AAPL":{"price":"..."}}` si pides varios. El cliente solo
  entendía el segundo, así que AAPL (único símbolo de su grupo) se perdía.
- La propia clave del endpoint `/price` es `price`, así que confundir los formatos
  hacía que "price" pareciera un símbolo.
- En los lotes con error, Twelve Data añade una clave `meta` que tampoco es un símbolo.

## Formato del timestamp en el topic (deuda conocida)

En `market.ticks.raw` el campo `eventTime` viaja como **número decimal de segundos
epoch** (`1789395320.744403168`), no como texto ISO-8601. Es lo que hace por defecto
el serializador JSON de Spring, y el consumidor Java lo reconstruye sin pérdida.

ponytail: se deja así a propósito en la Fase 1. Que un campo tan importante viaje
como un número opaco, sin nadie que declare su significado, es exactamente el
problema que resuelve el Schema Registry — y en la Fase 2 el esquema Avro le da un
`logicalType` de timestamp explícito. La alternativa (pelear con el ObjectMapper del
serializador ahora) es trabajo que la Fase 2 tira a la basura.

## Fase 2 — Schema Registry, Avro y migración a Spring Boot 4.1.1

| Decisión | Por qué | Descartado |
|---|---|---|
| **Avro** (y no Protobuf) | Los `.avsc` son JSON legible mientras se aprende, el formato de mensaje con ID de esquema es el estándar de este registry, y los ejercicios BACKWARD/FORWARD/FULL de la Fase 7 salen limpios | Protobuf |
| **Migrar a Spring Boot 4.1.1** | El aviso de soporte OSS era legítimo: Maven Central confirma que la línea viva es 4.x. Es el momento más barato (2 servicios, ~800 líneas) | quedarse en 3.5.16 con el aviso, o dejarlo documentado para después |
| **Copia de seguridad antes de migrar** | El repo no tiene git, así que un `cp -a services /tmp/aggora-before-boot4` es la red de seguridad más barata | migrar a pelo |
| **Schema Registry 7.6.1** (la que fija el README) | Ya estaba pineada y cumple; al añadir Avro habrá que alinear la librería con el servidor | — |

### Qué cambió de verdad en la migración a Boot 4

1. **Jackson 2 a Jackson 3.** El paquete pasa de `com.fasterxml.jackson.databind` a
   `tools.jackson.databind`. Toca a los dos clientes HTTP y a sus tests. Además:
   - `ObjectMapper` sigue siendo instanciable con `new`, pero la entrada recomendada
     es `JsonMapper.builder().build()`.
   - Las excepciones de Jackson 3 son **unchecked** (`JacksonException extends
     RuntimeException`), así que sobraban los `throws JsonProcessingException`.
   - El soporte de fechas (`Instant`) ya viene dentro de databind: desaparece el
     módulo `jackson-datatype-jsr310`.
2. **La autoconfiguración de Kafka salió del monolito.** Ya no está en
   `spring-boot-autoconfigure`: vive en el módulo **`spring-boot-kafka`**, que se
   trae con el starter **`spring-boot-starter-kafka`** (antes bastaba con declarar
   `org.springframework.kafka:spring-kafka`).
   `ConcurrentKafkaListenerContainerFactoryConfigurer` cambia de paquete:
   `org.springframework.boot.autoconfigure.kafka` a
   `org.springframework.boot.kafka.autoconfigure`.
3. **Spring Kafka 3.3 a 4.1.1 y kafka-clients 3.9 a 4.2.1.** Los serializadores JSON
   de Jackson 3 se llaman ahora `JacksonJsonSerializer` / `JacksonJsonDeserializer`;
   los `JsonSerializer`/`JsonDeserializer` de siempre siguen ahí pero son de Jackson 2
   y con Boot 4 revientan al arrancar con
   `NoClassDefFoundError: com/fasterxml/jackson/databind/JavaType` (nos pasó).
   Los nombres de las propiedades (`spring.json.*`) **no** cambian.
4. Los clientes de Kafka 4.2.1 hablan sin problema con el broker 3.9.0 del compose.

Verificado después de migrar: 17 tests en verde, simulador produciendo con precios
reales de los dos proveedores, y consumidor con 63.500 mensajes procesados, 0
descartes y 0 errores.

### Pendiente de la Fase 2

- `.avsc` de `tick` y `canonical` (con el timestamp como `logicalType`).
- Serializador/deserializador Avro. Ojo: la librería `io.confluent:kafka-avro-serializer`
  **no está en Maven Central**, hay que añadir el repositorio
  `https://packages.confluent.io/maven/`. La última es la **8.3.1**, alineada con
  kafka-clients 4.x; si se usa esa, el contenedor del registry debería subir a
  `confluentinc/cp-schema-registry:8.3.1` para no dejar cliente y servidor a muchas
  versiones de distancia.
- Republicación del evento canónico a `market.ticks.canonical` (el normalizer pasa a
  ser consumidor **y** productor, como dice el spec).

### Fase 2 completada: cómo quedó y qué aprendimos

| Decisión | Por qué |
|---|---|
| Esquemas en **`services/schemas/`** (una sola copia) | Los `.avsc` son la fuente de verdad del contrato y de ellos se generan las clases; dos copias acabarían divergiendo. La verdad compartida en runtime es el registry, no el fichero |
| **Namespace distinto** para el canónico (`com.aggora.avro.canonical`) | Los dos esquemas definen enums con los mismos nombres (`AssetClass`, `Exchange`, `TickSource`). En el mismo paquete chocarían al generar las clases |
| `price` como **decimal lógico** y `eventTime` como **timestamp-millis** | En dinero no se usa coma flotante, y el `logicalType` es justo lo que declara que ese `long` es una fecha. Con el plugin configurado (`enableDecimalLogicalType`, JSR310) Avro genera `BigDecimal` e `Instant` |
| El normalizer **republica el canónico** con un objeto nuevo | Los dos subjects evolucionan por separado: el canónico puede ganar campos sin tocar el contrato del crudo |
| `schema.registry.url` en `spring.kafka.properties` | Es común a productor y consumidor, así que se declara una vez |

Dos incidentes que valen más que la teoría:

1. **La autocreación de topics es una trampa.** Encontré un topic `market.ticks.raw`
   con **1 partición** que nadie había declarado: un `kafka-console-consumer` mío que
   quedó vivo pedía metadatos del topic y el broker lo creaba solo. Se apagó con
   `KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"`: un typo en un nombre debe fallar, no
   crear un topic silencioso con las particiones equivocadas.
2. **Con la autocreación apagada, los topics internos hay que declararlos.**
   El Schema Registry no crea su propio `_schemas`, así que se quedó atascado
   (su consumidor interno en bucle con `unknown topic or partition`) y el
   **productor de la aplicación se quedó colgado**: el serializador Avro se ejecuta
   en el mismo hilo que hace el `send`, así que un registry que no responde bloquea
   la producción entera, sin errores en el log. Se arregla con el servicio
   `kafka-init`, que declara `_schemas` (1 partición, `cleanup.policy=compact`).
   Lección para producción: los timeouts del cliente del registry importan tanto
   como los del broker.

## Fase 3 — Kafka Streams: métricas por ventana

| Decisión | Por qué |
|---|---|
| **API de Kafka Streams a pelo**, no el binder de Spring Cloud Stream | Se ve la topología que construyes, y es exactamente lo que usa Quarkus en la Fase 8: así la comparación entre frameworks es justa. El binder ahorraría código pero escondería la maquinaria justo mientras se aprende |
| **ASML en sus dos cotizaciones reales** (`ASML` en NASDAQ y `ASML.AMS` en Euronext) | El spec pide arbitraje de instrumentos con doble cotización y no teníamos ninguno. El sufijo `.AMS` hace que cada cotización sea un instrumento distinto: si compartieran símbolo, sus precios (uno en USD y otro en EUR) se mezclarían en los mismos agregados |
| **El acumulador es un registro Avro** | Kafka Streams guarda el estado de la ventana en un state store y en un topic de changelog, así que necesita un serde. Se reutiliza Avro en vez de meter JSON solo para esto, y el changelog queda legible con el esquema en el registry |
| Importes en **double dentro del acumulador**, **decimal en el contrato** | El acumulador es aritmética interna que puede cambiar sin avisar; el contrato de salida sí lleva decimal, que es lo que consumen otros |
| Ventanas **cortas y configurables** (30 s fijas, 60 s/15 s móviles) | En dev hay que poder ver resultados en segundos. Los tamaños están en `application.yml` |
| `statestore.cache.max.bytes: 0` (sin cache) | Cada tick emite una métrica actualizada y se ve la ventana llenarse. Con cache habría menos escrituras pero resultados a plazos. ponytail: lo "profesional" es encadenar `.suppress(untilWindowCloses(...))` para emitir una sola vez cuando la ventana cierra |
| Los tests de la topología usan `TopologyTestDriver` y una URL **`mock://`** del registry | Se prueba la aritmética (VWAP, media, volatilidad, solape de ventanas) sin broker, sin registry y sin esperar ventanas de verdad. 3 tests que fallan si la fórmula se rompe |
| El topic de salida lo declara el servicio, los internos los crea Kafka Streams | Consistente con la regla del proyecto: cada topic lo declara quien escribe. Los changelog y de repartición los crea Streams por su cuenta con el AdminClient |

### Fase 3 (continuacion): spread de arbitraje y consultas interactivas

| Decisión | Por qué |
|---|---|
| El tipo de cambio va a un topic **compactado** (`market.fx.reference`) y se lee como **GlobalKTable** | Es dato de referencia: al leerlo interesa el último valor por par, no el historial. La GlobalKTable se copia entera en cada instancia (son dos pares), así que **no** hay que co-particionar: la clave de búsqueda se calcula del propio registro. Es el patrón para enriquecer un stream con referencia |
| Conversión de divisa **antes** del cruce de precios | Comparar euros con dólares no significa nada. El precio europeo se convierte a USD con el tipo de cambio de referencia y el contrato `ArbitrageSpread` queda todo en USD. El precio original en EUR sigue en `market.ticks.canonical` |
| El join stream-stream lleva **ventana de 5 s** | Los dos mercados no publican en el mismo milisegundo. La ventana es lo que permite emparejar dos precios que sí son comparables, y evita cruzar un precio con otro de hace horas |
| `StreamJoined.with(...)` con los serdes explícitos | El join guarda la pata izquierda en un state store con ventana; sin decirle los serdes, Kafka Streams falla al arrancar. Costó un test en rojo |
| El endpoint devuelve un **DTO propio**, no la clase Avro | Jackson intenta serializar también el `getSchema()` del registro Avro y revienta (visto en vivo: `HttpMessageNotWritableException`). Además, la API HTTP no debe quedar atada al contrato de Kafka, que evoluciona aparte |
| El símbolo va como **parámetro de consulta**, no en la ruta | Los pares de divisas llevan barra (`EUR/USD`) y la barra parte la URL en dos segmentos: `/analytics/EUR/USD` daba 404 |
| Puerto **8085** para la API de analítica | El 8080 lo ocupa el simulador y el 8081 el registry. Ojo: `server.port` va en la RAÍZ del yml, no dentro de `spring:` (lo puse mal y Tomcat se fue al 8080) |

## Fase 4 — Motor de matching con exactly-once

| Decisión | Por qué |
|---|---|
| Las ordenes van a `orders.incoming` con key = **SIMBOLO**, no cuenta | **Desviación consciente del spec** (que propone accountId). El motor mantiene un libro por instrumento: para que un libro esté completo, todas las órdenes de un símbolo tienen que caer en la misma partición. Con la cuenta como key, cada partición tendría un libro incompleto y habría que repartir antes de cruzar. Se documenta en vez de sufrirlo en silencio |
| **Exactly-once** con transacciones: `transaction-id-prefix`, gestor de transacciones en el contenedor y `read_committed` | Publicar la ejecución y confirmar el offset son **una sola operación**: o las dos, o ninguna. Sin eso, una caída entre publicar y confirmar duplica la ejecución (posición contada dos veces) |
| Manejador de errores que **aborta** en vez de descartar | El `DefaultErrorHandler` por defecto, tras agotar reintentos, **descarta** el mensaje (confirma el offset). Con transacciones eso es perder la orden en silencio. Se configura sin reintentos y relanzando, para que la transacción se deshaga y el mensaje siga pendiente. El destino correcto de un mensaje imposible es un DLT (Fase 6) |
| El libro de ordenes vive **en memoria** | El spec lo permite y es lo más simple. Contrapartida conocida: si el motor se reinicia, el libro se pierde (solo se reconstruye con las órdenes que vuelvan a entrar). El camino de mejora está claro: un state store (como hace Kafka Streams) o persistirlo junto al outbox |
| Las órdenes las genera el **simulador** (`OrderFlowGenerator`), no un módulo nuevo | Ya es el generador de tráfico del proyecto y ya tiene los precios de referencia para poner precios límite creíbles. Un servicio aparte sería un despliegue más sin nada nuevo que enseñar |
| Inyección de fallo **configurable** (`fail-every-n-orders`) en vez de solo un test | Exactly-once no se demuestra con un test unitario, se demuestra viendo que un mensaje abortado no llega a nadie. Es el mismo criterio que el `kill -9` de la Fase 1 |

## Fase 5 — Cartera, alertas y auditoria (en curso)

| Decisión | Por qué |
|---|---|
| **Postgres vuelve a la infraestructura** | El patron transactional outbox del `audit-log` necesita una base de datos donde escribir el evento y el registro de salida en la MISMA transaccion. Se recupera el `postgres:16-alpine` que se perdio en la Fase 0b |
| Se anade **`currency` a Order y Execution** con **valor por defecto** | Sin divisa, sumar exposiciones de instrumentos en EUR, USD y CNY no significa nada. Se hace como campo con default: eso es un cambio **COMPATIBLE** (el registry lo acepta sin romper a nadie y los mensajes antiguos se leen con el valor por defecto). Es el primer cambio de esquema real del proyecto, y el caso "bueno" que en la Fase 7 se contrastara con uno roto a proposito |
| El estado de cartera se clavea por **`cuenta\|simbolo`** y en el topic por **cuenta** | La posicion que interesa es por cuenta Y instrumento, pero el spec pide `portfolio.updates` con key = accountId. Se usa una clave compuesta para el estado y se re-clava a la cuenta antes de publicar. El re-clavado obliga a reparticionar, y Kafka Streams lo hace solo (topic `positions-store-repartition`) |
| `isolation.level=read_committed` tambien en el **Streams** de cartera | Las ejecuciones se publican en transacciones. Si aqui se leyera con `read_uncommitted`, entrarian las ejecuciones ABORTADAS y la cartera contaria operaciones que no ocurrieron. Es la mitad de exactly-once que se olvida |
| Coste medio solo cambia al **abrir o aumentar**; al cerrar se materializa el resultado | Es la aritmetica de una cartera de verdad. Si la operacion da la vuelta a la posicion, el resto abre al precio nuevo. Los 3 tests cubren los tres casos |
| Limite de margen **global** (configurable) | Suficiente para el objetivo (producir `marginBreach` y que alerting levante la alerta). En un sistema real seria por cuenta e instrumento, con reglas de margen de verdad |

### Fase 5 (continuacion): alertas y auditoria

| Decisión | Por qué |
|---|---|
| El feed parado se detecta con un **punctuator**, no con un filtro | Detectar que algo NO llega no se puede hacer reaccionando a los mensajes. El punctuator se ejecuta por reloj, haya datos o no |
| Picos **una vez por episodio** + **histéresis** (dispara a 40 bps, rearma a 20) | La primera versión produjo 36.000 alertas en dos minutos: un aviso repetido no es un aviso. Con un solo umbral, un precio que oscila alrededor avisa sin parar. Entre las dos cosas el ruido bajó ~65x. Pendiente fino: comparar contra la **volatilidad medida** (que ya calculamos) en vez de un umbral fijo |
| `audit.events` **compactado** con key = entidad | El publicador del outbox puede repetir un mensaje (at-least-once). Con topic compactado y clave por entidad, el duplicado es inofensivo: se queda como el mismo último estado |
| El evento auditado se guarda en **JSON** (codificador de Avro) | Una auditoría se lee: con SQL se ve qué pasó sin descodificar. Limitación conocida: los campos `decimal` salen como bytes escapados por el codificador JSON de Avro; el resto es legible y todo es consultable por entidad y fecha |
| Índice **único** por (topic, partición, offset) + `on conflict do nothing` | Kafka entrega at-least-once. Así la tabla de auditoría también es idempotente por su cuenta, sin depender del consumidor |
| Fecha al insertar como **`OffsetDateTime`** | El driver de Postgres no sabe convertir un `Instant` y falla solo en marcha (`Can't infer the SQL type`). Costó 327 errores en el log descubrirlo |
| Deuda: el camino con Postgres no tiene test de integración | Lo correcto es **Testcontainers**, como pide la estrategia de pruebas del spec. De momento se verifica en vivo; queda anotado |

## Fase 6 — Descartes, reintentos y operación (en curso)

| Decisión | Por qué |
|---|---|
| **Tres mecanismos** según el fallo: reintento en el sitio, topic de reintento y DLT | Un fallo de un instante se resuelve reintentando; uno que tarda (la base de datos caída) se aparta a un topic de reintento; uno que no se va a resolver nunca (mensaje venenoso) va al DLT. Reintentar un veneno para siempre **bloquea la partición**, que es lo que pasaba en la Fase 4 |
| El fallo inyectado del motor se decide por el **orderId**, no por un contador | Lección en vivo: con un contador, al reintentar el contador avanza, el fallo desaparece y el mensaje se procesa bien, así que **nunca llega al DLT**. Los DLT son para venenos (el fallo pertenece al mensaje), no para fallos de un momento |
| El DLT se **declara** en el `KafkaTopicConfig` de cada servicio | Con la autocreación apagada (Fase 2), el publicador de descartes no puede crear el topic: intenta escribir y el broker contesta que no existe. Se descubrió en vivo; es la misma regla de siempre (cada topic lo declara quien escribe) |
| `@RetryableTopic` en la auditoría (y manejador manual en el motor) | La auditoría no es transaccional y el patrón declarativo crea los topics de reintento y el DLT sin código. En el motor, que sí es transaccional, la publicación al DLT tiene que ir **dentro de la misma transacción** que el commit, y eso se monta a mano |
| El motivo del descarte viaja en la **cabecera `x-dlt-reason`** | Dentro de tres semanas, quien mire el DLT agradecerá saber por qué se descartó cada mensaje |
| Reintentos *largos* en la auditoría, *cortos* en el resto | Es una decisión de negocio: un tick de hace dos minutos ya no sirve, pero un registro de auditoría perdido es inaceptable. Mismo mecanismo, configuración opuesta |
| **Pendiente**: cluster de 3 brokers y Grafana | `min.insync.replicas` y el failover necesitan varios brokers (con `replicas: 1` no hay nada que conmutar); el panel de lag necesita kafka-exporter + Prometheus + Grafana |

### Incidente de operación (reinicio del entorno) y lo que se arregló

Se reinició Docker Desktop/WSL: cayeron los 7 servicios y los 3 contenedores. Las lecciones
y los arreglos:

| Hallazgo | Arreglo |
|---|---|
| El estado **no** estaba en los servicios: 29 topics, 22 esquemas y 131.932 eventos sobrevivieron porque al levantar de nuevo los contenedores se *arrancan*, no se recrean | Nada que arreglar: es la consecuencia de que el estado viva en Kafka, Postgres y los state stores con changelog. Pero conviene saberlo: `docker compose down` **sí** se llevaría los datos (no hay volúmenes) |
| Los contenedores no volvieron solos tras el reinicio | `restart: unless-stopped` en Kafka, Schema Registry y Postgres |
| **Carrera de arranque**: Kafka Streams falla al arrancar si su topic de origen no existe todavía (`MissingSourceTopicException`), y los topics los crea quien escribe en ellos | `scripts/start-services.sh` arranca en orden y **espera a que cada servicio confirme** que está listo antes de lanzar el siguiente. Los scripts también evitan tener que abrir 7 terminales |
| **GlobalKTable + topic compactado**: el checkpoint del store global apuntaba a offsets que la compactación ya había borrado (`OffsetOutOfRangeException`), y el hilo global murió | La recuperación documentada es reiniciar la aplicación. Queda anotado que un topic compactado no es un histórico fiable, solo el último estado por clave |
| `TaskCorruptedException` puso ERROR pero **se arregló solo**: Streams reinicializó la tarea y la reconstruyó desde su changelog | Nada: es el mecanismo funcionando. Sirve para distinguir "error mortal" de "error auto-reparable" |
| Los procesos Java no los supervisa nadie: si uno se cae, no vuelve | Deuda consciente: en producción lo hace Kubernetes o systemd. Aquí, ejecutar el script |

### Fase 6: observabilidad (kafka-exporter + Prometheus + Grafana)

| Decisión | Por qué |
|---|---|
| **kafka-exporter** delante de Prometheus | Prometheus no habla el protocolo de Kafka: hace falta algo que traduzca los grupos y sus offsets a métricas. El exporter es ese traductor |
| Panel y fuente de datos **provisionados desde el repo** | Abrir Grafana y configurar un dashboard a mano no se puede revisar ni repetir. Con los ficheros en `infra/grafana/` el panel está versionado y aparece solo al arrancar |
| Entrada **anónima** en Grafana (rol Admin) | Es un entorno de desarrollo: el panel se abre sin login. Las credenciales del README siguen valiendo para la API |
| Solo métricas **de Kafka**, no de las aplicaciones | El spec pide lag y throughput por topic, y eso lo da el exporter. Las métricas de JVM/Kafka Streams necesitan `actuator` + `micrometer` en los 7 servicios: queda anotado como la mitad que falta |
| Permisos de los ficheros montados: `chmod 644` | Los ficheros se crearon con permisos 600 y **dentro del contenedor no corre root** (Prometheus uid 65534, Grafana 472), así que no podían leer su configuración y los contenedores entraban en bucle de reinicio. Lección de Docker, no de Kafka |

### Fase 6: clúster de 3 brokers y replicación

| Decisión | Por qué |
|---|---|
| **3 nodos** KRaft, cada uno broker **y** controller, con quórum entre ellos | Sin Zookeeper y con la misma imagen. Es el mínimo para sobrevivir a una caída (con 3, mayoría = 2) |
| **`KAFKA_DEFAULT_REPLICATION_FACTOR: 3`** y topics declarados **sin réplicas explícitas** | El código deja de fijar `replicas(1)`: manda el default del broker. Así el mismo código sirve para un broker suelto o para un clúster de tres, y no hay que tocarlo al cambiar el tamaño |
| **`KAFKA_MIN_INSYNC_REPLICAS: 2`** (+ `acks=all`, que ya estaba) | Es lo que convierte "tengo copias" en "no pierdo datos": una escritura se confirma con dos copias al día. Con dos brokers caídos, el productor **deja de escribir** en vez de arriesgar |
| `replication.factor: 3` en los topics internos de **Kafka Streams** | Los changelog de los state stores son el estado de las ventanas: también tienen que sobrevivir a una caída de broker |
| El puerto `29092` de cada broker se publica como 29092/29093/29094 | Para poder mirar el clúster desde WSL. Los servicios usan los nombres internos y no les afecta |
| `--kafka.server` del exporter **repetido** por broker | No acepta una lista con comas: intenta resolver la cadena entera como un solo host y falla (`too many colons in address`). Descubierto en vivo |
| El clúster nuevo empezó con los topics vacíos | El almacenamiento del broker único no se puede reutilizar (era otro clúster). Lo que importaba —los 131.932 eventos auditados— está en Postgres y sigue ahí. Los topics y esquemas se recrean solos al arrancar los servicios |

### Fallo silencioso de Kafka Streams (encontrado operando) y su arreglo

| Hallazgo | Arreglo |
|---|---|
| Kafka Streams, ante un error no recuperable, **para el cliente** (estado ERROR) pero **el proceso Java sigue vivo**: el endpoint HTTP responde y el servicio parece sano mientras no procesa nada | Un `StreamsUncaughtExceptionHandler` que devuelve **`REPLACE_THREAD`**: se sustituye el hilo que falló en vez de matar el cliente, y el servicio se recupera solo. Es lo que recomienda la documentación de Kafka Streams |
| El endpoint de consultas devolvía un **500 genérico** cuando el motor estaba en ERROR | Comprueba `streams.state()` y devuelve **503 con el motivo**. Si algo va a fallar en silencio, que al menos lo diga |
| La causa concreta era el **GlobalKTable sobre un topic compactado**: su checkpoint apunta a offsets que la compactación (o recrear el topic) se lleva por delante | Se queda documentado: un topic compactado no es un histórico fiable, solo el último estado por clave. Con el manejador nuevo, el hilo se sustituye y el estado global se relee |
| Se perdió un rato persiguiendo un `NoSuchMethodError` que era **basura de compilación**: el `.class` decía una cosa y el código otra | Conclusión práctica: ante un error raro de firma o de clase que no cuadra con el código, `mvn clean` antes de investigar nada más |


## Fase 7 — Laboratorio de evolucion de esquemas

El objetivo de la fase es cambiar un contrato que ya esta en marcha y **enterarse antes de
romperlo**. Lo primero fue medir, porque casi todo lo que se cuenta de la compatibilidad de
Avro se aprende mal: el laboratorio pregunta al registro de verdad por seis cambios y anota
los veredictos en las dos direcciones.

| Decision | Por que |
|---|---|
| El laboratorio **no produce mensajes** con los esquemas de prueba | Cada mensaje del topic apunta al **ID de su esquema**. Si produces con un esquema y luego lo borras, esos mensajes quedan ilegibles para siempre (no se pueden reescribir y su unica llave desaparece). Registrando y borrando sin producir, no se deja ninguna mina |
| El laboratorio **deja el registro como estaba** y se limpia solo si se corta | Es una prueba sobre un sistema vivo. Un `trap` en `EXIT` borra lo que haya registrado, y el ultimo paso comprueba `versiones: [1] (baseline: [1])`. Si no coincide, el script **falla** en vez de callarse |
| El schema propuesto vive en `src/test/resources` y no en `services/schemas` | El `avro-maven-plugin` genera clases a partir de todos los `.avsc` de `services/schemas`: un segundo `CanonicalTick` con el mismo nombre en el mismo namespace rompe la generacion. En `test/resources` no se genera nada y el test lo lee del classpath |
| El "consumidor antiguo" del test es la clase `CanonicalTick` ya compilada | Es la prueba honesta: no se simula un lector viejo, se usa el que el servicio tiene en produccion. El escritor usa el esquema nuevo y el lector el viejo, que es exactamente lo que hace el deserializador de Confluent con el esquema que le da el registro |
| El nivel de compatibilidad se queda en **BACKWARD** | Es el que corresponde a un proyecto donde los consumidores se despliegan despues que los productores. El laboratorio cambia el nivel un momento (dentro de `comprobar_con_nivel`) para enseñar la otra columna y lo restaura |
| Los esquemas de prueba se generan con `jq` desde `canonical.avsc` | No se quedan desfasados si el contrato cambia, y deja a la vista que un cambio que rompe es una linea de `jq`, no un fichero que alguien mantiene a proposito |

Lo medido, que es lo que justifica la fase:

| Cambio | BACKWARD | FORWARD |
|---|---|---|
| Añadir un campo **con** valor por defecto | COMPATIBLE | COMPATIBLE |
| Añadir un campo **sin** valor por defecto | RECHAZADO | COMPATIBLE |
| Renombrar un campo sin alias | RECHAZADO | RECHAZADO |
| Renombrar un campo con alias | COMPATIBLE | RECHAZADO |
| Borrar un campo | **COMPATIBLE** | RECHAZADO |
| Campo opcional (`["null","long"]`, default `null`) | COMPATIBLE | RECHAZADO |

La fila que hay que recordar es la del borrado: **el registro lo acepta** (con BACKWARD) y
sin embargo **rompe a los consumidores ya desplegados**, porque ellos si conocen el campo y
dejan de recibirlo. "Compatible" no significa nada sin decir en que direccion, y el error de
un borrado no aparece en el registro sino en produccion. Es el mismo tipo de leccion que el
`MissingSourceTopicException` de la Fase 6: el sistema te avisa donde puede, no donde
quisieras.
