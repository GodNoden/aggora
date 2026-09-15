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

