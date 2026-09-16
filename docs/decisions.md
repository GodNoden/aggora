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

## Fase 8 — Port a Quarkus (preparacion)

La Fase 8 no es una migracion: las dos implementaciones viven juntas para poder compararlas.
Eso manda en la estructura y en lo que se comparte.

| Decision | Por que |
|---|---|
| `services/pom.xml` es un **agregador que no hereda de nadie**, y cada implementacion tiene su padre (`spring/pom.xml` hereda de `spring-boot-starter-parent`; el de Quarkus importara su BOM) | El comentario original del pom imaginaba modulos Quarkus hermanos del mismo padre. No vale: ese padre mete el `dependencyManagement` y los plugins de Spring, y las versiones de `kafka-clients`, Jackson o JUnit las decidiria Spring en vez del BOM de Quarkus |
| Los 7 modulos se movieron con `git mv` a `services/spring/` y `services/schemas/` **no se movio** | Los contratos Avro son de las dos implementaciones: cada una genera sus clases de los mismos `.avsc`, que es lo que hace que las dos hablen el mismo idioma en el topic (y lo que permite que el port de Quarkus consuma lo que produce el simulador de Spring). Los 41 tests verdes son la red de seguridad del movimiento |
| Metricas (`actuator` + `micrometer-registry-prometheus`) **solo en los dos servicios con servidor web** | `market-data-simulator` (8080) y `analytics-streams` (8085). A los otros cinco habria que anadirles `spring-boot-starter-web` solo para poder mirarles la memoria, y eso cambiaria justo lo que se quiere medir. Para esos cinco, el RSS se mide con `ps` (`scripts/measure-service.sh`) |
| Etiquetas `stack` y `service` **en la aplicacion**, no en el scrape de Prometheus | Es lo que permite poner las dos implementaciones en el mismo panel. Ponerlas tambien en el scrape haria que Prometheus renombrase las de dentro a `exported_stack`, que es el lio que se quiere evitar |
| El scrape apunta a `host.docker.internal:8080` y `:8085` | Los servicios corren en el devcontainer, no como contenedores de `aggora-net`. Se probo el nombre del contenedor (`eager_allen`) y **no resuelve** desde Prometheus; `host.docker.internal` si (Docker Desktop lo da hecho en Windows/WSL2; en Linux hay que anadir `extra_hosts: host-gateway`) |
| El volumen de Prometheus monta la **carpeta**, no el fichero | Montar un solo fichero funciono hasta que se reemplazo: el montaje queda atado al inodo viejo y al recrear el contenedor Docker Desktop falla con "no such file or directory". Con la carpeta montada, editar `prometheus.yml` no rompe nada |

### El fallo silencioso, otra vez: la sonda de salud

Trabajando en las metricas volvio a aparecer el fallo de la Fase 6, y esta vez con la leccion
completa. El GlobalKTable sobre `market.fx.reference` (topic **compactado**) fallo con
`OffsetOutOfRangeException`: la compactacion avanzo el principio del log por delante del
checkpoint local, Kafka Streams limpio el estado y pidio un reinicio que nadie le dio. El
servicio se quedo **vivo, escuchando y devolviendo 503**, con el motor en ERROR.

| Lo que se probo | Resultado |
|---|---|
| `REPLACE_THREAD` (lo que ya habia) | El cliente se para igual: sustituir el hilo no arregla un estado global inconsistente. El servicio queda en ERROR, pero se recupera al reiniciarlo (Kafka Streams ya limpio el estado local) |
| `SHUTDOWN_APPLICATION` (que sobre el papel para la aplicacion entera) | **Peor**: dentro de Spring el cierre se enreda, el consumidor entra en un bucle de `Request joining group due to: Shutdown requested` que escribio **429 MB de log en 28 segundos**, y el proceso **tampoco muere**. Se revirtio |
| `StreamsHealth`: `/actuator/health` a DOWN y `aggora_kafka_streams_running` a 0 | **Es la respuesta**: un servicio no deberia decidir suicidarse. Quien levanta un proceso caido es el supervisor (la politica de reinicio de Docker, systemd, Kubernetes) y quien le dice que esta roto es la sonda. Verificado rompiendo el checkpoint a proposito: el proceso sigue vivo, `/analytics` da 503, la sonda dice DOWN con `estado: ERROR` y la metrica vale 0 |

### Detalle que costo un rato: 500 no es un transitorio

Cada vez que el servicio arranca, el state store existe pero no se puede leer todavia
(`InvalidStateStoreException: the stream thread is STARTING, not RUNNING`). El controlador
devolvia **500** porque el `try` solo envolvia el momento de *abrir* el store: abrirlo
funcionaba y el fallo llegaba despues, en el `fetch`. Ahora el `try` envuelve la consulta
entera y se responde **503** con el motivo. Verificado en un arranque real, muestreando el
endpoint cada medio segundo: 503, 503, 200 y **ningun 500**.

### Linea base (medida, no estimada)

`bash scripts/measure-service.sh`, con el sistema en reposo. El "proceso" es lo que tarda
desde que arranca el proceso hasta que esta listo, que es lo que espera un contenedor; el
"ctx" es lo que tardo el contexto de Spring.

| Servicio | ctx (s) | proceso (s) | RSS (MB) | hilos | ficheros |
|---|---|---|---|---|---|
| market-data-simulator | 2,399 | 2,775 | 531 | 54 | 18 |
| ingestion-normalizer | 1,635 | 2,009 | 441 | 37 | 20 |
| analytics-streams | 2,862 | 3,229 | 516 | 78 | 210 |
| order-matching-engine | 2,125 | 2,560 | 297 | 37 | 20 |
| portfolio-risk | 2,227 | 2,688 | 434 | 60 | 87 |
| alerting-service | 0,338 | 2,342 | 469 | 39 | 25 |
| audit-log | 2,163 | 2,556 | 336 | 49 | 45 |

Ojo con la fila de `alerting-service`: su contexto arranca en 0,338 s pero el proceso tarda
2,342 s. Mirar solo la cifra de Spring ("Started X in N seconds") da una foto incompleta: lo
que un contenedor espera de verdad es el tiempo del proceso.

### Fase 8: el port de `ingestion-normalizer` a Quarkus

El primer servicio portado es el normalizer, que es el mas sencillo (no tiene estado) y el que
mas cosas toca: consume, valida, publica dos topics y descarta al DLT.

| Decision | Por que |
|---|---|
| Cada implementacion lleva su sufijo en el `artifactId` (`ingestion-normalizer-spring` / `-quarkus`) | En un mismo reactor (y en el mismo `.m2`) no puede haber dos artefactos con el mismo `groupId:artifactId`: Maven se niega con "duplicated in the reactor". Las **carpetas** siguen llamandose igual (`services/spring/ingestion-normalizer` y `services/quarkus/ingestion-normalizer`); lo que cambia es el nombre del jar |
| Mismos topics, mismo `group.id`, mismos serializadores de Confluent | Es lo que hace que el port sea **intercambiable**: se para uno y se arranca el otro y el pipeline ni se entera. Verificado en vivo (abajo) |
| La validacion vive en `TickValidator`, separada del consumo | Son las mismas 7 reglas que en Spring y asi se pueden probar sin Kafka. Es la unica logica no trivial del servicio |
| `quarkus-confluent-registry-avro` en vez de solo `quarkus-avro` | Quarkus **falla el build** si detecta clases de Avro de Confluent sin su extension: *"Confluent Avro classes detected, please use the quarkus-confluent-registry-avro extension"*. La extension trae dentro `quarkus-avro`, que es lo que registra para reflexion las clases generadas (`@AvroGenerated`) |
| Los topics se crean con el `AdminClient` en un `@Observes StartupEvent` | En Quarkus no hay autoconfiguracion de topics (en Spring son beans `NewTopic`). Se mantiene la misma regla: cada topic lo declara quien escribe en el, y sin replicas explicitas para que mande el default del broker |
| **Sin** health ni metricas en el normalizer de Quarkus | El de Spring tampoco las tiene (no tiene servidor web). Anadirlas solo a un lado falsearia justo lo que se quiere medir: el RSS y el arranque |

Cuatro cosas de la API de SmallRye que cuestan un rato y quedan apuntadas:

| Lo que se intento | Lo que pasa de verdad |
|---|---|
| `@Incoming` en un metodo `void` que recibe un `Message` | Quarkus no arranca: *"the method consumes a Message, so the returned type must be `CompletionStage<Void>` or `Uni<Void>`"*. Se devuelve el resultado del `ack()`: es el equivalente del `Acknowledgment` de Spring, pero devuelto |
| `Emitter<T>.send(mensaje)` para enterarse de los fallos de publicacion | El `send(Message)` de MicroProfile devuelve `void`, asi que no hay donde enganchar el error. Se usa el `MutinyEmitter` de SmallRye, cuyo `sendMessage` devuelve un `Uni` |
| `record.getOffset()` | `KafkaRecord` no tiene offset: viaja en `IncomingKafkaRecordMetadata`, que ademas vive en el paquete `...kafka.api`, no en `...kafka` |
| El `ConsumerRebalanceListener` de Kafka | SmallRye tiene el suyo (`KafkaConsumerRebalanceListener`, que pasa el `Consumer` en cada evento) y es el que busca por el nombre del bean |

**Verificado en vivo, y esto es lo importante:** se paro el normalizer de Spring, se arranco el
de Quarkus con el MISMO topic de entrada, el MISMO `group.id` y los MISMOS topics de salida, y:

- el motor de Quarkus arranco en **1,123 s** y el listener de rebalanceo conto las **6 particiones** asignadas;
- los offsets de `market.ticks.canonical` siguieron subiendo y el lag del grupo se quedo en 0-2;
- **`analytics-streams` (que sigue siendo el de Spring) respondio 200 con ventanas nuevas**:
  las dos implementaciones son intercambiables en el topic porque comparten los contratos Avro;
- con el simulador inyectando ticks invalidos (`AGGORA_INVALIDTICKEVERYN=300`), el port mando
  **6 mensajes al DLT con la cabecera `x-dlt-reason: precio ausente o no positivo`**, el mismo
  texto y la misma cabecera que la version Spring.

Los primeros numeros de la comparacion, medidos con `scripts/measure-service.sh` el mismo dia y
en la misma maquina, para el MISMO servicio:

| | Spring Boot 4.1.1 | Quarkus 3.39.3 (JVM) |
|---|---|---|
| Arranque (proceso hasta listo) | 2,099 s | **1,123 s** |
| Contexto / arranque propio | 1,725 s | — |
| RSS en reposo | 428 MB | **332 MB** |
| Hilos | 37 | 45 |
| Descriptores abiertos | 24 | 89 |

Quarkus arranca antes y ocupa menos, que es lo esperable cuando el trabajo de CDI se hace en el
build en vez de en el arranque. Los 45 hilos y los 89 descriptores son el precio: el motor
reactivo de Vert.x y SmallRye montan mas piezas moviles que un contenedor de Spring. La
comparacion completa, con el binario nativo, va en la Fase 9.

### Fase 8: el port de `market-data-simulator` a Quarkus

El segundo servicio, y el que mas piezas tiene: dos proveedores de precios por HTTP, el motor de
ticks programado, el generador de ordenes y el calendario de mercados. La logica de dominio
(`Exchange`, `PriceWalk`) se copia tal cual: no depende del framework, y cambiarla habria roto la
comparacion.

| Decision | Por que |
|---|---|
| La configuracion es **el mismo YAML** que el de Spring (bloque `aggora:` copiado entero) | Si al portar tambien se cambian los instrumentos, los precios semilla o los intervalos, la Fase 9 compararia dos cosas a la vez. Se usa `quarkus-config-yaml` solo para eso |
| Los dos proveedores son **clientes REST declarativos** (`@RegisterRestClient`) | Es la diferencia de filosofia mas grande del port: en Spring el cliente se arma a mano con un `RestClient` imperativo; aqui se describe la llamada en una interfaz y la implementa Quarkus. Menos codigo, menos control del builder |
| Los dos clientes siguen separando la **traduccion de la respuesta** en un metodo estatico probado | Los dos formatos de `/price` y el `Information` de cuota agotada de Alpha Vantage son los casos que muerden, y asi se prueban sin HTTP. Los 17 tests del simulador de Spring se portaron enteros |
| `Instance<ReferenceSource>` en vez de `List<ReferenceSource>` | Spring inyecta una lista con todas las implementaciones; en CDI se pide `Instance<T>` y se recorre. Misma idea, otra forma |
| Metricas (`quarkus-micrometer-registry-prometheus` + `/q/metrics`) | El simulador de Spring tiene servidor web (8080) para exponer `/actuator/prometheus`. Sin metrics, el port tendria menos cosas que el original y la comparacion de memoria seria tramposa |

Y cinco cosas que solo se descubren haciendolo, que es de lo que va esta fase:

| Lo que paso | Lo que hay que hacer |
|---|---|
| **El scheduler de Quarkus no baja de un segundo**: `An every() value less than 1000 ms is not supported`, y Spring si admite `fixedRate = 200ms` | Se programa **una vuelta por segundo** y en cada vuelta se emiten `1000 / tick-interval-ms` ticks (5 con el valor del proyecto). El ritmo es el mismo (medido: 44 msg/s, como Spring) pero llega en rafagas de 5 en vez de uniforme, y eso cambia un poco lo que ven las ventanas de la analitica. Queda dicho |
| La primera ejecucion del job fallaba con `SRMSG00019: Unable to connect an emitter with the channel ticks-raw` | El job disparaba antes de que los canales estuvieran conectados: `skipExecutionIf = Scheduled.ApplicationNotRunning.class`. En Spring no pasa porque el scheduler arranca con el contexto ya listo |
| Un `String` obligatorio con **valor vacio** no arranca: `defined as the empty String which the Converter considered to be null` | La api key (que viene de `${TWELVEDATA_API_KEY:}`) es `Optional<String>` y se comprueba con `filter(...).isPresent()`. En Spring esa misma variable deja una cadena vacia y basta un `isBlank()` |
| `feed-exchange` falta en los instrumentos de Alpha Vantage y Quarkus exige el valor | `Optional<String>` y `orElse("")`. Y ojo: `@WithDefault("")` **no** vale, porque mete una cadena vacia que el conversor tambien rechaza |
| Los intervalos que solo usan las anotaciones `@Scheduled` no son miembros del `@ConfigMapping`, y Quarkus **falla la validacion** de propiedades desconocidas del prefijo | `quarkus.config.mapping.validate-unknown=false`, que es justo lo que hace la version Spring dejandolos fuera del record. Salvo `tick-interval-ms`, que el port si lee en codigo y por eso si esta en el mapping |
| Las claves con puntos puestas en **YAML** salen del parser entre comillas y Quarkus no las reconoce (`Unrecognized configuration key ""quarkus.rest-client...""`) | Las propiedades planas van a `application.properties`; el YAML se queda para la parte estructurada (la lista de instrumentos) |
| El parser del port es **Jackson 2** (`com.fasterxml.jackson`) porque es el que trae Quarkus; el de Spring es **Jackson 3** (`tools.jackson`) | La logica de traduccion es identica, los nombres cambian (`asText()` vs `asString()`, `fields()` vs `properties()`). Es la misma friccion de la migracion a Boot 4, en la direccion contraria |
| El callback del `Emitter` de SmallRye solo tiene un `CompletionStage<Void>`: **no hay `RecordMetadata`** | El log de cada 1000 ticks cuenta cuantos van y cuantos fallan, pero ya no puede decir en que particion y offset cayeron, cosa que el `KafkaTemplate` de Spring si da en su callback |

**Verificado en vivo:** se paro el simulador de Spring y arranco el de Quarkus con los mismos
topics y la misma configuracion, y el pipeline de Spring (normalizer, analitica, matching) siguio
funcionando sin enterarse: **44 msg/s** de ticks, ordenes cada segundo, `analytics` respondiendo
200 y `/q/metrics` publicando. Los 17 tests, en verde.

Los numeros, para el mismo servicio y la misma maquina:

| | Spring Boot 4.1.1 | Quarkus 3.39.3 (JVM) |
|---|---|---|
| Arranque (contexto) | 2,474 s | — |
| Arranque (proceso hasta listo) | 2,847 s | **1,425 s** |
| RSS en reposo | 375 MB | **304 MB** |
| Hilos | 53 | 64 |
| Descriptores abiertos | 20 | 81 |

Un aviso honesto sobre la verificacion: a la hora de probarlo, Euronext y Shanghai estaban
cerrados y Twelve Data se quedo sin key, asi que **el camino HTTP real no se pudo ejercitar** (en
las dos implementaciones, que se comportan igual: `open.isEmpty()` y no se pide nada). Lo que si
se comprobo contra la API de verdad es el caso de cuota agotada: Alpha Vantage responde 200 con
`Information` en vez de dar un error HTTP, que es exactamente el caso que cubre
`AlphaVantageClientTest.cuota_agotada_no_es_un_precio`.

### Fase 8: el port de `analytics-streams` a Quarkus

El servicio que el spec señalaba como el interesante, porque aquí la comparación no es de
configuración sino de **quién envuelve la misma API de Kafka Streams**.

| Decision | Por que |
|---|---|
| La topología se **produce** ({@code @Produces Topology}) y ya está | Es TODA la diferencia de montaje: en Spring hay `@EnableKafkaStreams`, un bean que recibe el `StreamsBuilder` y `spring.kafka.streams.*`; en Quarkus la extensión configura el motor con `quarkus.kafka-streams.*`, lo arranca, lo para y **expone el `KafkaStreams` como bean** para las consultas interactivas |
| `MetricsTopology` y `ArbitrageTopology` se copian **tal cual** (solo cambia el tipo de la config) | Son código de Kafka Streams puro. Es el resultado más importante de la fase: **la API de Streams no cambia entre frameworks**, lo que cambia es el envoltorio |
| El resto del motor va con el prefijo `kafka-streams.*` | `kafka-streams.replication.factor=3`, `commit.interval.ms`, `statestore.cache.max.bytes=0`, `state.dir`, los serdes por defecto... la extensión los pasa a la configuración de Streams |
| `quarkus.kafka-streams.topics=market.ticks.canonical,market.fx.reference` | La extensión **espera a que existan** antes de arrancar. Es la carrera que en la Fase 6 hubo que resolver a mano en el script (`MissingSourceTopicException`): aquí la resuelve el framework y aparece en `/q/health` como *Kafka Streams topics health check* |
| Sonda de salud y métricas **de la extensión** | `/q/health` trae dos comprobaciones de Kafka Streams (estado del motor y topics disponibles) sin escribir una línea; en Spring esto fue `StreamsHealth`, 40 líneas propias. Es la diferencia de DX más clara de todo el port |
| Etiquetas `stack`/`service` con un `MeterFilter` | Micrometer no tiene una propiedad global de etiquetas en Quarkus (solo para el binder de HTTP), así que van con un `MeterFilter.commonTags` |
| El endpoint es un recurso JAX-RS con **la misma lógica** de 503 | Mismo JSON (fechas ISO, mismos campos), mismo 503 si el motor está en ERROR o el store se está reconstruyendo |

Y el hallazgo que se lleva la palma, porque no es de frameworks sino de **dependencias**:

| Lo que pasó | Lo que hay detrás |
|---|---|
| Los tests fallaban con `SecurityException: Forbidden com.aggora.avro.canonical.CanonicalTick! This class is not trusted to be included in Avro schemas` | **Avro 1.12.2 enciende el validador de clases** (`ClassSecurityValidator`) y el BOM de Quarkus trae 1.12.2, mientras que la implementación Spring usa **1.12.1**, donde venía apagado. Con los serdes de Kafka Streams salta seguro, porque `SpecificAvroSerde` resuelve la clase **a partir del esquema** (`ClassUtils.forName`) y ahí está la validación. El productor de Avro normal no lo pisa porque ya tiene el objeto |
| El arreglo, en dos sitios | En la aplicación: `quarkus.avro.trusted-packages=com.aggora.avro` (la extensión de Quarkus instala su propio predicado). En los tests de topología, que **no** levantan Quarkus: `org.apache.avro.SERIALIZABLE_PACKAGES=com.aggora.avro` en el surefire |

Esto último es un aviso para la implementación Spring: hoy pasa porque su Avro es 1.12.1; el día
que se suba a 1.12.2+ va a fallar igual y habrá que poner la misma propiedad. Queda escrito.

**Verificado en vivo:** parada la analítica de Spring y arrancada la de Quarkus con el mismo
`application-id` y los mismos topics, el port **consumió lo que producía el normalizer de Spring**
y calculó métricas (`market.analytics` a 242 msg/s), la **consulta interactiva devolvió el mismo
JSON** (mismo formato de fechas y mismos campos), y `/q/health` informó del motor en RUNNING y de
los topics disponibles. Los **6 tests de topología** (los mismos que Spring, con `TopologyTestDriver`
y registry `mock://`) pasan.

Los números, mismo servicio y misma máquina:

| | Spring Boot 4.1.1 | Quarkus 3.39.3 (JVM) |
|---|---|---|
| Arranque (proceso hasta listo) | 3,464 s | **1,548 s** |
| RSS en reposo | 455 MB | 505 MB |
| Hilos | 77 | 85 |
| Descriptores abiertos | 185 | 238 |

Ojo con la memoria de este servicio en concreto: aquí manda el estado (RocksDB + el motor), así que
la cifra depende de **cuánto estado había cargado** en el momento de medir, y las dos
implementaciones se midieron en momentos distintos. Para el registro: en las otras dos, donde el
peso es el framework, Quarkus salió por debajo.

### Fase 8: el port de `portfolio-risk` a Quarkus

El segundo servicio de Kafka Streams, y el que confirma que la receta de la analítica se repite:
topología producida, config en `kafka-streams.*`, el `ActionConfig` de Avro declarado y los tests
con `TopologyTestDriver`. **La topología se copió tal cual** (180 líneas de aritmética de cartera,
con `KState`/`KTable`, ventanas de estado y re-clavado por cuenta|símbolo) y lo único que cambió fue
el tipo de la configuración y tres líneas de montaje.

| Decision | Por que |
|---|---|
| **Sin servidor web, sin métricas y sin sonda**, en las dos implementaciones | La de Spring tampoco las tiene (es un motor de Streams y nada más). Añadirlas solo a un lado falsearía justo lo que se mide: el RSS y el arranque |
| `kafka-streams.isolation.level=read_committed` | El motor de matching publica las ejecuciones en **transacciones**. Leyendo `read_uncommitted` entrarían también las ejecuciones ABORTADAS y las posiciones contarían operaciones que no ocurrieron |
| Directorio de estado propio (`/tmp/aggora-portfolio-state-quarkus`) | Para no mezclarlo con el de Spring mientras se comparan; el estado se reconstruye desde el changelog |

**Verificado en vivo, y esto es lo interesante de este servicio:** el port de Quarkus se puso a
consumir `orders.executions`, que es lo que publica el **motor de matching de Spring, con
transacciones**, y calculó posiciones (`[cartera] ACC-01 USO | cantidad=806 coste...`) escribiendo
en `portfolio.updates` a ~1 msg/s. O sea: un consumidor `read_committed` de Quarkus leyendo lo que
un productor transaccional de Spring confirmó — el exactly-once de la Fase 4 cruzando las dos
implementaciones sin enterarse.

Los números, mismo servicio y misma máquina:

| | Spring Boot 4.1.1 | Quarkus 3.39.3 (JVM) |
|---|---|---|
| Arranque (proceso hasta listo) | 2,023 s | **1,066 s** |
| RSS en reposo | 374 MB | **306 MB** |
| Hilos | 61 | 66 |
| Descriptores abiertos | 81 | 147 |

Es el servicio donde la diferencia de arranque es mayor (−47%), y tiene sentido: no hay contexto de
Spring ni servidor web de por medio, solo el motor de Streams.

### Fase 8: el port de `order-matching-engine` a Quarkus (exactly-once)

El servicio del exactly-once, y donde el montaje deja de parecerse entre los dos frameworks.

| | Spring Boot | Quarkus (SmallRye) |
|---|---|---|
| **Dónde se pide la transacción** | En un bean: `KafkaTransactionManager` colgado del contenedor de escucha (`KafkaTransactionConfig`), y el listener no se entera | **En el código**: se inyecta `@Channel("executions") KafkaTransactions<Execution>` y se envuelve el procesamiento en `withTransactionAndAck(record, emitter -> ...)` |
| **Los offsets del consumidor** | Se confirman dentro de la transacción (`sendOffsetsToTransaction`), lo hace el contenedor | También: es justo lo que añade el `AndAck` de `withTransactionAndAck` |
| **El DLT** | `DefaultErrorHandler` + `DeadLetterPublishingRecoverer` (2 reintentos de 200 ms y, si sigue fallando, al topic) | Publicación explícita al topic de descartes desde el propio manejador, con `markForAbort()` en la transacción |
| **El libro de órdenes** | Copiado tal cual (`OrderBook`, `OrderBooks`, 156 líneas con prioridad precio-tiempo) | Igual |

| Decision | Por que |
|---|---|
| El veneno se decide **por el `orderId`**, no por un contador | Igual que en la Fase 4: con un contador, al reintentar el contador avanza, el fallo desaparece y el mensaje nunca llega al DLT. Un DLT es para mensajes venenosos, no para fallos del momento |
| La orden venenosa se detecta **después de publicar** las ejecuciones y se aborta con `markForAbort()` | Es lo que hace demostrable el exactly-once: las ejecuciones quedan escritas en el topic y **abortadas**, así que solo las ve quien lee con `read_uncommitted`. Detectar antes de publicar no dejaría rastro que enseñar |
| El motivo del descarte va en la cabecera `x-dlt-reason` | Los dos servicios de Aggora que tienen DLT usan ya esa cabecera; el `DeadLetterPublishingRecoverer` de Spring añade las suyas (`kafka_dlt-exception-*`), así que la etiqueta común es esta |

**Verificado en vivo, con el experimento de la Fase 4 repetido en Quarkus:** con el veneno inyectado
por `orderId`, el mismo topic `orders.executions` leído dos veces dio **read_committed 83.572** y
**read_uncommitted 83.582**: diez ejecuciones abortadas que existen en el log y que nadie que lea
con `read_committed` va a ver. Y la orden venenosa llegó a `orders.incoming.DLT` con
`x-dlt-reason: orden venenosa inyectada: esta orden no se puede procesar` (el mismo texto que
inyecta la versión Spring), con la partición avanzando en vez de atascarse.

**Dos cosas que costaron un rato y quedan apuntadas:**

| Lo que pasó | Lo que hay detrás |
|---|---|
| `AGGORA_FAILEVERYNORDERS=10` no hacía nada | Ese nombre es el que acepta **Spring** (relaxed binding: `AGGORA_FAILEVERYNORDERS` → `aggora.failEveryNOrders`). Quarkus mapea el entorno de forma **exacta**: `AGGORA_FAIL_EVERY_N_ORDERS`. Para experimentar es más cómodo `-Daggora.fail-every-n-orders=10` |
| Los 4 tests del libro fallaron con `Forbidden com.aggora.avro.orders.Side` | El mismo validador de Avro 1.12.2 de la analítica. Confirma que el hallazgo no era de Streams sino de **cualquier módulo cuyos tests construyan registros Avro**: mismo arreglo en el surefire |

Los números, mismo servicio y misma máquina:

| | Spring Boot 4.1.1 | Quarkus 3.39.3 (JVM) |
|---|---|---|
| Arranque (proceso hasta listo) | 1,987 s | **1,290 s** |
| RSS en reposo | 268 MB | 253 MB |
| Hilos | 39 | 44 |
| Descriptores abiertos | 24 | 82 |

### Fase 8: el port de `alerting-service` a Quarkus

El tercer servicio de Kafka Streams, y el port más rápido de todos: la receta ya estaba establecida y
**los tres ficheros de topología se copiaron tal cual** (el `SpikeDetector` con su histéresis, el
`StaleFeedDetector` con su **punctuator** y la `AlertingTopology` que los engancha). Lo único que
cambió fue el tipo de la configuración.

| Decision | Por que |
|---|---|
| Los dos detectores se copian enteros, **incluido el punctuator** | El punctuator es lo que permite detectar la AUSENCIA de datos (no llega un mensaje que diga "este símbolo se ha parado"), y no depende del framework: es API de Kafka Streams. Es la mejor prueba de la conclusión de la fase |
| Mismos umbrales: 40 bps para disparar, 20 para rearmar, 30 s de feed parado | Están en el bloque `aggora:` copiado del yml de Spring. Cambiarlos habría hecho incomparables las dos implementaciones |
| Sin servidor web, métricas ni sonda | Igual que en la cartera: la versión Spring tampoco las tiene |

**Verificado en vivo:** el port arrancó en **1,073 s**, se puso a consumir `market.analytics` y
`portfolio.updates`, y **levantó los dos tipos de alerta con datos reales**:
`[alerta] pico de precio en USD/CNY` (el SpikeDetector comparando el último precio con la media de su
ventana) y `[alerta] CRITICAL MARGIN_BREACH` (el aviso que publica portfolio-risk al superar el
límite de exposición).

| | Spring Boot 4.1.1 | Quarkus 3.39.3 (JVM) |
|---|---|---|
| Arranque (proceso hasta listo) | 1,870 s | **1,073 s** |
| RSS en reposo | 383 MB | **256 MB** |
| Hilos | 39 | 43 |
| Descriptores abiertos | 31 | 91 |

### Fase 8: el port de `audit-log` a Quarkus (el outbox con Postgres)

El séptimo y último servicio, y la única comparación que toca base de datos.

| | Spring Boot | Quarkus |
|---|---|---|
| **Acceso a datos** | `JdbcTemplate` con el SQL a pelo | **JDBC a pelo** sobre el `DataSource` de Agroal: la misma idea, sin capa intermedia (la alternativa idiomática sería Panache/Hibernate, pero habría cambiado el patrón que se está comparando) |
| **La transacción del outbox** | `@Transactional` de Spring | `@Transactional` de Jakarta, con Narayana (viene con el JDBC de Quarkus) |
| **El esquema** | `spring.sql.init.mode=always` ejecuta `schema.sql` | Quarkus no ejecuta el schema: se lee el **mismo `schema.sql`** y se ejecuta al arrancar (todo es `create ... if not exists`, así que es idempotente). El sitio bueno sería Flyway |
| **El consumidor** | Un `@KafkaListener` con los tres topics y el valor como `Object` | **Tres canales tipados** (`@Incoming("executions")`, `"portfolio-updates"`, `"alerts"`), cada uno con su tipo Avro, delegando en un método común: aquí Quarkus obliga a algo más seguro de tipos |
| **El publicador** | `@Scheduled(fixedDelayString=...)` | `@Scheduled(every=...)` + `SKIP` + `ApplicationNotRunning` |

| Decision | Por que |
|---|---|
| El evento y el recado van en la **misma transacción** | Es el patrón entero: si se cae entre las dos escrituras, no queda ni el evento ni el recado. Con JDBC a pelo se consigue con `@Transactional` |
| La idempotencia la da el **índice único** `(source_topic, source_partition, source_offset)`, no el código | Si Kafka reentrega, la inserción no se repite. Lo mismo que en Spring, y por eso el `insert ... on conflict do nothing` se copia tal cual |
| El contador de pendientes se lee **antes** de publicar | El envío es asíncrono: contar después da los que aún no se han marcado y parece que la outbox no se vacía (pasó al probarlo, y el log lo decía mal) |

**Verificado en vivo:** el port creó las tablas, consumió los tres topics y **auditó 190.628 eventos**,
con la bandeja de salida drenándose de verdad: **190.622 publicados** y **6 pendientes** (los de la
última vuelta del publicador) ✅, sin un solo error en el log.

| | Spring Boot 4.1.1 | Quarkus 3.39.3 (JVM) |
|---|---|---|
| Arranque (proceso hasta listo) | 2,157 s | **1,468 s** |
| RSS en reposo | 354 MB | **296 MB** |
| Hilos | 49 | 58 |
| Descriptores abiertos | 57 | 100 |

Con esto **los siete servicios están portados**. Lo que queda de la fase es lo que pide el spec y no
es copiar código: correr los dos stacks a la vez (con `group.id` y topics de salida propios), la
**imagen nativa de GraalVM** para dos servicios y las medidas que van al informe de la Fase 9.

## Fase 8: el binario nativo (lo que falta, y por qué)

El spec pide la imagen nativa de GraalVM para dos servicios. **En esta maquina no se ha podido
completar**, y el intento deja cuatro tropiezos que son exactamente los "native image gotchas" que
el informe de la Fase 9 tiene que documentar:

| Intento | Lo que pasó |
|---|---|
| `docker run` con la imagen del builder y `mvn` dentro | La imagen `ubi9-quarkus-mandrel-builder-image` **no trae Maven**: solo `/opt/mandrel`. Está pensada para que la use el plugin de Quarkus, no para ejecutarla a mano |
| `mvn package -Dnative` desde un contenedor de Maven con el socket de Docker | **BUILD SUCCESS y ningún binario**: los poms de este proyecto están escritos a mano y **no tienen el perfil `native`**, así que `-Dnative` no activa nada. La propiedad que de verdad manda es `-Dquarkus.package.type=native` |
| Lo mismo, con `-Dquarkus.package.type=native` | `ContainerRuntimeUtil.detectContainerRuntime`: el contenedor de Maven **no tiene el ejecutable de `docker` dentro**, y no basta con montarle el socket |
| Lo mismo, montando también el CLI de docker | `checkGraalVMVersion`: el tag del builder (`jdk-21`) **no es el que espera Quarkus 3.39**. El arreglo es no fijarlo y dejar que Quarkus use su imagen por defecto |

El script `scripts/build-native.sh` queda con la receta corregida (y con los cuatro tropiezos
comentados, para no volver a pisarlos). Lo que falta para cerrar la fase:

1. Compilar el nativo en una máquina con **Mandrel instalado** (`sdk install java 21.x-mandrel`) o
   con el builder correcto, para `ingestion-normalizer` y `market-data-simulator` (los dos sin
   RocksDB, que es lo que hace pesado el nativo de los servicios de Streams).
2. Medir con `scripts/measure-service.sh`: arranque en frío y RSS. La misma vara que se usó con la
   JVM, y la comparación que de verdad separa los dos mundos.

## Tests de integración con Testcontainers (lo que faltaba)

Hasta aquí los 81 tests eran **unitarios** (aritmética, reglas y parsing) y la integración se había
hecho **a mano**: los ports intercambiables, el experimento de exactly-once, el DLT con su cabecera.
Eso demuestra que funciona, pero no queda automatizado: el día que alguien toque la configuración de
un canal, nada avisa.

| Decision | Por que |
|---|---|
| Los `*IT` van en **failsafe**, no en surefire | `mvn test` (lo que corre en cada guardado) sigue en milisegundos; los que levantan contenedores se ejecutan con `mvn verify`. Es la separación de siempre y la que espera un equipo |
| Los `*IT` viven en los módulos cuyas fronteras prueban | `OutboxPostgresIT` en `audit-log` (base de datos) y `ExactlyOnceKafkaIT` en `order-matching-engine` (broker, Schema Registry y transacciones) |
| En el test se usa la imagen `confluentinc/cp-kafka` para el broker | Es la que el `KafkaContainer` de Testcontainers trae probada. En producción el proyecto usa `apache/kafka`: lo que se prueba aquí es el **cableado del cliente**, no la imagen |
| El CI de GitHub corre unitarios en cada push e integración en cada PR | Es donde tiene que correr: el **devcontainer de este proyecto no tiene socket de Docker**, así que Testcontainers no puede ejecutarse ahí dentro (el mismo muro que paró el nativo) |

Los dos tests, y lo que cazan que un mock no puede:

- **`ExactlyOnceKafkaIT`**: publica una ejecución en una transacción que se confirma y otra en una
  que se **aborta**, y comprueba que el consumidor `read_committed` solo ve la primera mientras el
  `read_uncommitted` ve las dos. Es el experimento de la Fase 4, automatizado, y de paso prueba el
  ida y vuelta de Avro contra el Schema Registry de verdad.
- **`OutboxPostgresIT`**: ejecuta el **mismo `schema.sql`** contra un Postgres real e insiste en lo
  que de verdad protege la auditoría: el mismo `(topic, partición, offset)` dos veces se audita una,
  porque lo garantiza el **índice único de la tabla**, no el código.

**Estado honesto:** los dos están escritos y **compilan** (`mvn test-compile` en verde), pero **no se
han podido ejecutar en esta máquina**: el devcontainer no tiene Docker y sacarlos por un contenedor
de Maven desde WSL se quedó en el intento (`Could not find a valid Docker environment`). Corren en
el CI de GitHub, que es donde tienen que correr.

## Fase 8: los dos stacks a la vez (el experimento que cierra la fase)

Es lo que pidió el spec y lo que demuestra que el port es una comparación de verdad y no dos repos
paralelos: **los dos stacks procesando la misma entrada al mismo tiempo**, con
`bash scripts/start-quarkus-stack.sh`.

| Como se evita que se pisen | Por que |
|---|---|
| La **entrada es la misma** (`market.ticks.raw` y `orders.incoming`) | Es lo que hace la comparación A/B: los dos ven los mismos datos. El **simulador corre solo en la versión Spring**, a propósito: dos simuladores duplicarían el tráfico y ya no se compararía lo mismo |
| **Grupo de consumo distinto** en cada consumidor (`-q`) | Kafka reparte las particiones **dentro de cada grupo**, así que los dos leen todos los datos; con el mismo grupo se las repartirían entre ellos y ninguno tendría el pipeline completo |
| **Topic de salida propio** (sufijo `.q`) | Cada stack escribe en sus topics y las cifras se pueden leer por separado |
| **`application-id` y directorio de estado propios** en los tres motores de Streams | Dos aplicaciones de Streams con el mismo `application-id` se reparten las particiones y **ninguna de las dos tiene el estado completo**: sería la peor forma de comparar |
| Propiedades con `-D` y no con variables de entorno | Los nombres de canal llevan guiones y el mapeo de Quarkus para el entorno es exacto; `-D` usa el nombre literal |

**Resultado medido** (con los dos stacks en marcha, 13 JVMs): los dos pipelines procesan a la vez y
cada uno en sus topics —`market.analytics` 3.681.665 mensajes frente a `market.analytics.q`
2.899.871, y `portfolio.updates` 42.130 frente a `portfolio.updates.q` 1.920`—. Los servicios de
Quarkus arrancaron entre 1,1 s y 4,2 s: **más lentos que en las medidas en solitario** (1,0-1,5 s),
porque ahora compiten por CPU con los siete de Spring. Eso también es un dato: las cifras de arranque
de `SPRING_VS_QUARKUS.md` son de una máquina en reposo, y en un servidor compartido la diferencia se
estrecha.

Y para pararlo, `bash scripts/stop-quarkus-stack.sh` (el de Spring se para con `stop-services.sh`).

## El devcontainer con Docker dentro (y los dos tropiezos que aparecieron)

El devcontainer ya trae el socket y el CLI de Docker, que era lo que faltaba para dos cosas: los
tests de integración con Testcontainers y la compilación del nativo. Se añadió con la feature
estándar `ghcr.io/devcontainers/features/docker-outside-of-docker`, porque hacían falta **las dos**
(el socket para Testcontainers y el CLI porque el plugin de Quarkus lo invoca).

Al reconstruirlo aparecieron dos cosas que conviene tener escritas:

| Tropiezo | Detalle |
|---|---|
| **El nombre del devcontainer cambia al reconstruirlo** | De `eager_allen` pasó a `charming_spence`. Todos los comandos de este documento que decían `docker exec -u vscode eager_allen` ahora usan una variable, `DEVCONTAINER=$(docker ps --format '{{.Names}}' \| grep -v aggora \| head -1)` |
| **`target/` quedó de `root`** | Por compilar con Maven desde el host (en un contenedor de Maven, que corre como root) al intentar el nativo. El devcontainer fallaba con `...jar is read-only`, que es **la misma trampa que ya está documentada en la Fase 1**, esta vez por la puerta de atrás. Se arregla con un `chown -R vscode:vscode` y la regla de siempre: compilar con `docker exec -u vscode` |

Y el estado de los tests de integración, que es lo honesto: **llegaron a ejecutarse** (los unitarios
del motor de matching dieron 4 en verde y el `*IT` intentó levantar los contenedores), pero
**Testcontainers sigue sin encontrar el Docker del devcontainer**: el socket está montado como
enlace (`/var/run/docker.sock -> /var/run/docker-host.sock`) y ni con `DOCKER_HOST` apuntando al
destino ni con `TESTCONTAINERS_HOST_OVERRIDE` se resolvió. El siguiente paso es leer la lista de
"Attempted configurations" del informe de failsafe, que dice **por qué** falla cada estrategia, y
ajustar desde ahí. En CI no hace falta nada de esto: el runner tiene Docker nativo y los tests
corren sin tocar nada.
