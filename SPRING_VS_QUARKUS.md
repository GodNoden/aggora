# Spring Boot vs Quarkus: la misma plataforma, dos veces

Este informe cierra la Fase 8 con lo que se ha medido, no con lo que se opina. Los siete servicios
de Aggora existen **dos veces** —`services/spring/` y `services/quarkus/`— con los mismos contratos
Avro, los mismos topics, los mismos `group.id` y los mismos serializadores de Confluent. El port no
es una migración: es una comparación controlada, y de hecho las dos implementaciones son
intercambiables en marcha (se para una y arranca la otra y el pipeline ni se entera).

**Cómo se ha medido:** mismo servicio, misma máquina (WSL2, 16 núcleos, 24 GB), sin carga más allá
del propio pipeline, con `bash scripts/measure-service.sh` para las dos. El arranque es el tiempo del
**proceso hasta estar listo** (lo que espera un contenedor), no la cifra del contexto. El RSS se lee
de `ps`, que es la única cifra que existe también en un binario nativo.

---

## 1. Arranque y memoria

| Servicio | Arranque Spring | Arranque Quarkus | RSS Spring | RSS Quarkus |
|---|---|---|---|---|
| `ingestion-normalizer` | 2,099 s | **1,123 s** | 428 MB | **332 MB** |
| `market-data-simulator` | 2,847 s | **1,425 s** | 375 MB | **304 MB** |
| `analytics-streams` | 3,464 s | **1,548 s** | **455 MB** | 505 MB |
| `portfolio-risk` | 2,023 s | **1,066 s** | 374 MB | **306 MB** |
| `order-matching-engine` | 1,987 s | **1,290 s** | 268 MB | **253 MB** |
| `alerting-service` | 1,870 s | **1,073 s** | 383 MB | **256 MB** |
| `audit-log` | 2,157 s | **1,468 s** | 354 MB | **296 MB** |

Léelo así:

- **El arranque lo gana Quarkus en los siete**, entre un 24 % y un 47 % menos. No es magia: Spring
  monta el contexto en el arranque (escaneo de componentes, proxies, autoconfiguración), y Quarkus
  hace el trabajo de CDI en el build. Cuanto menos framework hay en el servicio, menos se nota:
  `order-matching-engine` (1,987 → 1,290) gana menos que `portfolio-risk` (2,023 → 1,066).
- **La memoria la gana Quarkus en seis de siete**, entre un 5 % y un 33 % menos. Y donde pierde
  —`analytics-streams`— la explicación no es el framework: ahí manda el **estado** (RocksDB más el
  motor de Streams), y las dos medidas no se tomaron con la misma cantidad de estado cargado. Es el
  aviso de siempre con un servicio con estado: **el RSS depende de cuándo mides**.
- Los hilos y descriptores abiertos van casi siempre **más altos en Quarkus** (Vert.x y SmallRye
  montan más piezas móviles): en `audit-log`, 58 hilos y 100 descriptores frente a 49 y 57 de
  Spring. Es el precio de la parte reactiva.

**Y el binario nativo**, que es donde la comparación se rompe de verdad (sección 5).

---

## 2. Imperativo (Spring) frente a reactivo (Quarkus)

**Dónde el modelo reactivo ayudó:**

- **El veneno se decide en el sitio correcto.** El exactly-once del motor de matching se pide en el
  código (`withTransactionAndAck`), no en la configuración: se lee de un vistazo dónde empieza y
  acaba la transacción. En Spring eso vive en un bean (`KafkaTransactionManager` colgado del
  contenedor) y el listener no se entera.
- **Los canales son tipados.** En el `audit-log`, Spring necesita un solo `@KafkaListener` con los
  tres topics y el valor como `Object`; en Quarkus hay tres canales con su tipo Avro. Menos
  sorpresas.
- **La salud del motor viene puesta.** `/q/health` trae dos comprobaciones de Kafka Streams (estado y
  topics disponibles) sin escribir una línea; en Spring fueron 40 líneas de `StreamsHealth` propias.
  Es la diferencia de DX más clara de todo el port.
- **Los topics de origen se esperan solos.** `quarkus.kafka-streams.topics` evita la carrera de
  `MissingSourceTopicException` que en la Fase 6 hubo que resolver a mano en el script de arranque.

**Dónde el reactivo estorbó:**

- **Un `Emitter` no tiene `RecordMetadata`.** El `KafkaTemplate` de Spring da partición y offset en
  su callback; el emisor de SmallRye solo un `CompletionStage<Void>`. El log de cada 1000 ticks
  perdió esa información.
- **Los métodos programados tienen que devolver el `ack()`** (si consumen un `Message`), y la
  primera ejecución puede adelantarse a los canales (`SRMSG00019`), cosa que se arregla con
  `skipExecutionIf`.
- **El scheduler simple no baja de un segundo**, y Spring sí admite `fixedRate = 200ms`. Se resolvió
  emitiendo `1000 / tick-interval-ms` ticks por vuelta: el ritmo es el mismo, pero llega en ráfagas.
- **La configuración es estricta y falla al arrancar**: una variable de entorno vacía es "ausente"
  (de ahí los `Optional`), un campo que puede faltar necesita `Optional`, y una propiedad del prefijo
  mapeado que no sea miembro del mapping rompe la validación. Spring traga; Quarkus te obliga a
  decidir. Eso es mejor a la larga, pero se paga al portar.

**Lo que no cambió en absoluto:** la API de Kafka Streams. Las tres topologías (`analytics-streams`,
`portfolio-risk`, `alerting-service`) se copiaron **tal cual**, con sus ventanas, sus state stores,
sus joins, su KTable y su **punctuator**. Lo único que cambió fue de dónde sale la configuración y
tres líneas de montaje (`@Produces Topology` en vez de `@EnableKafkaStreams` con el builder). Si te
llevas una conclusión de esta fase, que sea esa.

---

## 3. Los tropiezos, que valen más que las tablas

| Lo que pasó | Lo que hay que recordar |
|---|---|
| `Forbidden com.aggora.avro...! This class is not trusted to be included in Avro schemas` | **Avro 1.12.2 enciende el validador de clases** y el BOM de Quarkus lo trae, mientras que Spring usa 1.12.1. Los serdes de Streams resuelven la clase desde el esquema, así que salta siempre. No es un problema de frameworks: es de **dependencias**, y le pasará a Spring el día que suba Avro |
| `AGGORA_FAILEVERYNORDERS=10` no hacía nada | Ese nombre es de **Spring** (relaxed binding). Quarkus mapea el entorno de forma exacta: `AGGORA_FAIL_EVERY_N_ORDERS`. En una migración esto rompe despliegues en silencio |
| Las claves con puntos en YAML salen entrecomilladas y Quarkus las ignora | Las propiedades planas van a `application.properties`; el YAML se queda para lo estructurado |
| El parser del port es **Jackson 2** y el de Spring **Jackson 3** | Misma lógica, nombres distintos (`asText()` / `asString()`, `fields()` / `properties()`). Es la fricción de la migración a Boot 4 en la dirección contraria |
| `-Dnative` decía BUILD SUCCESS y no generaba binario | Los poms escritos a mano no tienen el perfil `native`: hace falta `-Dquarkus.package.type=native`. Un "éxito" que no produce nada es peor que un error |
| La imagen del builder no trae Maven; el contenedor de Maven necesita el CLI de docker dentro; el tag del builder tiene que cuadrar con la versión de Quarkus | Los tres están comentados en `scripts/build-native.sh` para no volver a pisarlos |

---

## 4. Recomendación honesta

**Si el equipo escribe Spring y no tiene un problema de arranque o de memoria: quédate en Spring.**
No hay ninguna razón para migrar siete servicios sanos. Spring tiene más gente, más documentación,
más respuestas y un ecosistema que Quarkus iguala pero no supera, y el ahorro medido aquí (un
segundo de arranque, decenas de MB) no paga un port.

**Quarkus se gana el sitio cuando el arranque o la huella son el problema:** funciones que escalan a
cero, contenedores que se levantan y se apagan, plataformas donde pagas por memoria, o cuando el
equipo ya usa Quarkus y valora que la salud del motor y la espera de topics vengan puestas. Ahí la
diferencia de arranque (−24 % a −47 %) y de RSS (−5 % a −33 %) es dinero.

**Y para el caso concreto de Kafka:** el trabajo de Streams es idéntico en los dos, así que la
decisión no se toma por el framework sino por lo de alrededor: quien te dé mejores sondas,
observabilidad y ciclo de vida para el motor. En esta fase, ese punto lo ganó Quarkus.

**Lo que yo haría en un proyecto nuevo con Kafka y Java:** Spring Boot si el equipo ya lo conoce y
son servicios de toda la vida; Quarkus si hay que escalar a cero o el arranque importa; y en los dos
casos, invertir el esfuerzo en lo que de verdad se rompe en producción —esquemas, transacciones,
reintentos y sondas— antes que en el framework.

---

## 5. El binario nativo

Dos servicios compilados con GraalVM (`ingestion-normalizer` y `market-data-simulator`, el mínimo
que pide el spec), con `scripts/build-native.sh`:

| Mismo servicio | Spring (JVM) | Quarkus (JVM) | **Quarkus (nativo)** |
|---|---|---|---|
| Arranque | 2,099 s | 1,123 s | **0,022 s** |
| RSS en reposo | 428 MB | 332 MB | **114-124 MB** |
| Hilos | 37 | 45 | **15** |
| Tamaño en disco | — | — | 91,7 MB |

**Cincuenta veces menos arranque que Quarkus en la JVM y noventa veces menos que Spring**, y un
tercio de la memoria. El binario **consume y publica de verdad**: Avro y el Schema Registry
funcionan dentro de una imagen nativa. El precio es el tamaño (91,7 MB) y, sobre todo, lo que cuesta
llegar hasta aquí.

### Los gotchas del nativo (lo que pide el spec, y son cuatro)

1. **BouncyCastle (TLS)**: el cliente del Schema Registry referencia `BCSSLSocket` para TLS opcional.
2. **Brotli (compresión)**: el cliente de Kafka y Netty referencian el decodificador.
3. **commons-compress + xz**: el cliente usa API nueva (`XZCompressorInputStream.builder()`) y hay que subir la versión que arrastra Confluent.
4. **La reflexión**: el serializador construye la estrategia de nombre de subject por reflexión
   (`Utils.newInstance`), y en una imagen nativa la reflexión no existe salvo que se declare.

La frase que resume los cuatro: **en una imagen nativa no existe lo "opcional en tiempo de
ejecución"**. Y una lección de método que vale más que el número: arreglar el cuarto **clase a
clase** costaba una compilación de cuatro minutos por error. Se resolvió **generando el fichero de
reflexión desde los propios jars** (169 clases a `META-INF/native-image/.../reflect-config.json`), y
con eso el **segundo binario salió a la primera**.

### Qué significa para la decisión

El nativo no cambia la recomendación de la sección 4: no migras siete servicios por un segundo de
arranque. La cambia para **serverless y para plataformas que cobran por memoria**: 0,022 s de
arranque es lo que hace viable escalar a cero, y 114 MB en reposo es la mitad de factura que 332 MB.
Si ese es tu problema, el nativo lo resuelve; si no, es un fin de semana de gotchas que no vas a
amortizar.
