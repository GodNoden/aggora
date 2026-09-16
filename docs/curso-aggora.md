# Aggora: curso magistral del proyecto

> Este documento es un **curso completo** sobre Aggora, escrito para el dueño del repo.
> Da igual si eres tú dentro de seis meses o un agente que lo use como material de
> clase: aquí está el recorrido entero, en orden, con el porqué de cada pieza y con los
> fallos reales que costaron tiempo.
>
> **Aggora es una herramienta de aprendizaje, no una demostración de expertiz.** No hay
> nada que presumir: hay cosas medidas, cosas que salieron mal y cosas que quedaron
> pendientes, y las tres se cuentan aquí.

---

## 0. Cómo usar este curso

### 0.1 Para el alumno (tú)

Sabes programar; no eres experto en Kafka ni en finanzas. Por eso cada concepto va en
tres capas y en este orden:

1. **La idea en lenguaje llano**, con una analogía cotidiana.
2. **El nombre técnico**, con la traducción entre paréntesis la primera vez.
3. **El ejemplo real de ESTE repo**: fichero, topic o línea de log.

Si un término aparece sin ejemplo del repo, es un fallo del curso, no tuyo.

### 0.2 Orden de lectura recomendado

| Momento | Qué leer | Con qué abierto al lado |
|---|---|---|
| 1.º | Sección 1 (qué es y qué no es) | `README.md` |
| 2.º | Sección 2 (dominio bursátil desde cero) | `services/spring/market-data-simulator/src/main/resources/application.yml` |
| 3.º | Sección 3 (Kafka desde cero) | `docs/kafka-101.md` como consulta, no como lectura seguida |
| 4.º | Sección 4 (las fases, en orden) | `docs/decisions.md`, que es el diario de a bordo |
| 5.º | Sección 5 (fichero a fichero) | el repo entero, es la sección de referencia |
| 6.º | Secciones 6 y 7 (patrones y problemas) | el código que citan |
| 7.º | Sección 8 (laboratorio) | el cluster levantado (`docker compose -f infra/docker-compose.yml up -d`) |
| 8.º | Secciones 9–12 (entrevista, mejoras, autoevaluación, glosario) | nada: son de repaso |

Lo que **no** hay que hacer: leer esto como un libro de texto de principio a fin sin
tocar el repo. Aggora se entiende ejecutándolo. Cada vez que una sección diga
"compruébalo así", hazlo.

### 0.3 Lo que se lee con el repo abierto y lo que no

- **Con el repo abierto**: secciones 2, 4, 5, 6, 7, 8. Todas citan ficheros concretos.
- **Sin el repo**: secciones 0, 9, 10, 11, 12. Son de método, repaso y honestidad.
- **`docs/kafka-101.md` no se copia aquí.** Es el material de consulta (21 capítulos de
  conceptos en lenguaje llano). Este curso es el **recorrido guiado**; aquel es el
  diccionario. Cuando quieras profundizar en un concepto suelto, ve allí.

### 0.4 El pacto de honestidad

Este curso se rige por tres reglas, y las tres importan más que cualquier tabla:

1. **Nada inventado.** Cada número, versión y anécdota sale del repo: de `docs/`,
   `README.md`, `CONTRIBUTING.md`, `SPEC.md`, de un comentario del código o de un
   script. Si algo no está claro, la frase es "**no está documentado**", no un relleno.
2. **Se dice lo medido y lo no medido.** Cuando hay una cifra, se dice en qué máquina y
   en qué condiciones se tomó. Cuando algo se probó "a ojo" o no se probó, se dice.
3. **El proyecto no está cerrado.** Quedan cosas sin hacer (sección 10) y hay
   mediciones con letra pequeña (una máquina compartida, ráfagas cortas, sin límites de
   heap al principio). Presentarlo como terminado sería mentir.

> **Aviso de vigencia.** Los documentos del repo se escribieron por fases y algunos se
> quedaron atrás: `docs/interview-notes.md`, por ejemplo, todavía dice que el nativo
> está pendiente y que los tests de integración no corren, cuando las fases posteriores
> los cerraron. **Manda siempre `docs/decisions.md` y `CONTRIBUTING.md`**, que se
> actualizaron al final. Este curso usa la versión final.

### 0.5 Si eres un agente y usas esto como maestro

- No resumas las secciones 4 y 7 en una tabla: son el corazón del curso y pierden todo
  el valor sin el "síntoma → diagnóstico → causa → arreglo → lección".
- Antes de afirmar un número, ábrelo en el repo. Este documento **cita la fuente** en
  la sección 0.6; si no encuentras la fuente, dilo.
- No inventes fases nuevas ni cierres las pendientes de la sección 10: están pendientes
  a propósito.

### 0.6 De dónde sale cada cosa (mapa de fuentes)

| Tipo de dato | Fuente principal |
|---|---|
| Qué se rompió y por qué | `docs/decisions.md` (diario de decisiones, con números) |
| Conceptos de Kafka en llano | `docs/kafka-101.md` (21 capítulos) |
| Rendimiento y CPU por mensaje | `docs/throughput-lab.md` + `scripts/throughput-test.sh` |
| Evolución de esquemas | `docs/schema-evolution-lab.md` + `scripts/schema-evolution-lab.sh` |
| Spring vs Quarkus | `SPRING_VS_QUARKUS.md` |
| Estado de cada fase | `CONTRIBUTING.md` ("Estado actual") + `README.md` |
| Despliegue | `deploy/README.md`, `docs/kafka-101.md` capítulo 21, `docs/decisions.md` |
| Entorno de desarrollo | `docs/dev-environment.md` |
| Especificación original (inmutable) | `SPEC.md` |

---

## 1. Qué es Aggora y qué no es

### 1.1 La plataforma en una página

Aggora es una **plataforma de eventos de mercados globales**: ingiere precios reales de
bolsa y divisas, los normaliza, calcula analítica por ventanas de tiempo, cruza órdenes
en un libro por instrumento, lleva la cartera de cada cuenta, levanta alertas y audita
todo. Por debajo, cada pieza existe para ejercitar **un concepto concreto de Kafka**.

La analogía de la plataforma entera: es una **red de agua**. Los proveedores de datos
son los manantiales; `market.ticks.raw` es la tubería de agua bruta; el normalizador es
la potabilizadora; los topics siguientes son la red de distribución; y cada servicio es
un grifo que hace una cosa (medir, cruzar, vigilar, auditar). Nadie llama a nadie por
teléfono: todos leen y escriben en las mismas tuberías. Eso es una **arquitectura
orientada a eventos** (event-driven).

### 1.2 El reparto de responsabilidades

Ocho servicios. Los mismos ocho existen **dos veces** (Spring y Quarkus) a propósito, y
el normalizador existe una tercera vez como función de AWS Lambda.

| Servicio | Responsabilidad | Papel en Kafka |
|---|---|---|
| `market-data-simulator` | Precios reales de dos proveedores + paseo aleatorio para dar volumen; genera órdenes simuladas | Productor (key = símbolo) |
| `ingestion-normalizer` | Valida, normaliza a UTC, republica el evento canónico y los tipos de cambio | Consumidor + productor, commit manual, DLT |
| `analytics-streams` | VWAP, media, volatilidad con ventanas; spread de arbitraje con joins | Kafka Streams (state store + consultas interactivas) |
| `order-matching-engine` | Libro de órdenes por instrumento con prioridad precio-tiempo | Consumidor + productor **transaccional** (exactly-once) |
| `portfolio-risk` | Posiciones, coste medio, P&L realizado, exposición y margen por cuenta | KTable con state store y re-clavado |
| `alerting-service` | Tres reglas: pico de precio, margen superado, feed parado | Streams + **punctuator** (vigila la ausencia) |
| `audit-log` | Guarda cada evento en Postgres y lo publica en un topic compactado | **Transactional outbox** + topic compactado |
| `gateway-ws` | Reparte los eventos a los navegadores por WebSocket | Consumidor **fan-out** (grupo propio) |

Y la tercera implementación, en `services/lambda/`: el `ingestion-normalizer` sin
estado como función de Lambda con un *event source mapping*.

### 1.3 Por qué es un proyecto de aprendizaje y no de producción

Míralo como un **simulador de vuelo**: reproduce los mandos y los sustos de un avión
real, pero no lleva pasajeros. En concreto, y esto hay que decirlo antes de que lo
pregunte nadie:

- **No hay autenticación** ni seguridad en local (sin SASL/TLS en el cluster de
  desarrollo). Las credenciales de Postgres y Grafana son de desarrollo.
- **No hay ejecución real de órdenes** ni conexión a un bróker de verdad.
- **El libro de órdenes vive en memoria**: si el motor se reinicia, se pierde (se
  reconstruye solo con las órdenes que vuelvan a entrar).
- **No se ha desplegado nada en AWS.** Los artefactos de `deploy/` están **validados**,
  no desplegados: no hay cuenta ni credenciales (lo dice `deploy/README.md`).
- Los datos reales son de planes gratuitos, con cuotas (ver sección 2).

Lo que **sí** es de verdad: los precios (dos proveedores reales), los fallos y las
mediciones. Todo lo que se afirma aquí se verificó contra el cluster en marcha.

### 1.4 El mapa de topics

Un **topic** es el canal con nombre por el que viajan los mensajes. El proyecto tiene
estos (particiones y política de limpieza tal como los declara el código):

| Topic | Particiones | Limpieza | Quién escribe | Quién lee hoy |
|---|---|---|---|---|
| `market.ticks.raw` | 6 | borrado por retención | simulador | normalizador |
| `market.ticks.canonical` | 6 | retención | normalizador | analítica, gateway |
| `market.fx.reference` | 1 | **compactado** | normalizador | analítica (GlobalKTable) |
| `market.analytics` | 6 | retención | analítica | alertas |
| `market.arbitrage` | 6 | retención | analítica | (sin consumidor en el repo) |
| `orders.incoming` | 6 | retención | simulador | motor de matching |
| `orders.executions` | 6 | retención | motor de matching | cartera, auditoría |
| `portfolio.updates` | 6 | retención | cartera | alertas, auditoría, gateway |
| `alerts.raised` | 3 | retención | alertas | auditoría, gateway |
| `audit.events` | 3 | **compactado** | auditoría (outbox) | nadie: es el registro consultable |
| `market.ticks.raw.DLT` | 1 | retención | normalizador | revisión manual |
| `orders.incoming.DLT` | 1 | retención | motor de matching | revisión manual |

Además, Kafka Streams crea por su cuenta los topics **internos**: los *changelog* de
cada state store y los de repartición (por ejemplo
`positions-store-repartition`). Y `@RetryableTopic` crea los topics de reintento de la
auditoría (`.retry-*`) más su `.DLT`.

**Cuidado con dos confusiones típicas** (están en `docs/kafka-101.md`, capítulo 5):
leer **no borra** el mensaje (los mensajes se borran por antigüedad o tamaño, nunca
porque alguien los lea), y el orden **solo se garantiza dentro de una partición**.

La clave de casi todos los topics es el **símbolo** (`key = symbol`). Eso hace que
todos los ticks de AAPL caigan siempre en la misma partición y se lean en orden; sin
orden por instrumento, una media móvil sería basura. La excepción es
`portfolio.updates`, cuya clave es la **cuenta** (lo pide el spec), y `audit.events`,
cuya clave es la **entidad**.

---

## 2. El dominio bursátil desde cero

Este capítulo es el vocabulario que necesitas para leer el código sin tropezar. Cada
término lleva un ejemplo numérico **sacado del repo**. Cuando el ejemplo sea una cuenta
hecha a mano con los valores del repo (y no una medición), se dice explícitamente.

### 2.1 Tick: una foto de un precio

Un **tick** es una observación de precio en un instante: "AAPL valía 333,47 a las
10:00:00,123". En Aggora, un tick real del topic `market.ticks.raw` se ve así
(recortado; ejemplo literal de `docs/kafka-101.md`):

```json
XAU/USD  {"eventId":"d01ea957-...","symbol":"XAU/USD","assetClass":"COMMODITY",
"exchange":"COMMODITY","currency":"USD","price":4277.5259,"size":0,
"eventTime":1789395320.744403168,"source":"REFERENCE","sequence":32}
```

Fíjate en dos campos que son lecciones del proyecto:

- **`source`**: `REFERENCE` (precio real que acaba de llegar del proveedor) o
  `SYNTHETIC` (interpolado entre dos referencias). El spec lo pide para poder ser
  honesto con qué es real y qué no.
- **`eventTime`** en la Fase 1 viajaba como **número decimal de segundos desde 1970**
  (`1789395320.74...`), no como texto legible, y **nada en el mensaje decía que fuera
  una fecha**. Eso se arregla en la Fase 2 con un `logicalType` de Avro (ver 2.15).

### 2.2 Símbolo

El **símbolo** es el nombre corto del instrumento: `AAPL`, `XAU/USD`, `600519`. En
Aggora el símbolo es además la **clave de Kafka**, y de ahí sale el orden por
instrumento (sección 3.3).

Detalle con miga: **una misma empresa puede tener dos símbolos**, uno por mercado.
ASML cotiza como `ASML` en NASDAQ (en dólares) y como `ASML.AMS` en Euronext Ámsterdam
(en euros). Se les da símbolos distintos a propósito: si compartieran símbolo, sus
precios (uno en USD y otro en EUR) se mezclarían en los mismos agregados. El sufijo
`.AMS` es lo que los separa (`docs/decisions.md`, Fase 3).

### 2.3 Mercado y huso horario

Cada bolsa abre y cierra en **su** hora local, y el proyecto se calla cuando está
cerrada. Eso vive en el enum `Exchange`
(`services/spring/market-data-simulator/src/main/java/com/aggora/simulator/domain/Exchange.java`):

| Mercado | Zona | Horario local |
|---|---|---|
| NYSE / NASDAQ | `America/New_York` | 09:30–16:00 |
| Euronext | `Europe/Paris` | 09:00–17:30 |
| SSE Shanghai | `Asia/Shanghai` | 09:30–11:30 y 13:00–15:00 (descanso de mediodía) |
| FX / Commodities | `UTC` | 24x5 (fin de semana cerrado) |

**Por qué el proyecto necesita dos proveedores de datos.** (Ojo: la consigna de este
curso decía "tres proveedores"; en el repo hay **dos**, y conviene decirlo tal cual.)
Ningún plan gratuito cubre los tres husos horarios, así que se comprobó símbolo a
símbolo contra las dos APIs (2026-09-14/15) y se repartió el trabajo: **Twelve Data**
para EEUU, forex, oro y ETFs (tiene endpoint por lotes), y **Alpha Vantage** para
Euronext y Shanghai (allí sí son gratis). Las cuotas descubiertas a golpes: Twelve Data
8 créditos/minuto y ~800/día, donde **un crédito = un símbolo pedido**; Alpha Vantage
25 peticiones/día y 1 petición/segundo. El crudo spot no está gratis en ninguno, así
que se usa el ETF `USO` — **es el precio del ETF, no el del barril**, y lo dice el
propio YAML.

> Honestidad de datos: `docs/decisions.md` habla de "los 12 instrumentos con precio
> real", pero el YAML declara **14** entradas. El repo **no documenta** la diferencia
> (probablemente la tabla de proveedores se escribió antes de añadir `ASML.AMS` y
> `USO`). Se deja dicho en vez de inventar una explicación.

### 2.4 Precio de compra/venta y *spread*

En un mercado hay dos precios a la vez: el **bid** (lo que alguien paga por comprar) y
el **ask** (lo que alguien pide por vender). La diferencia es el **spread**, y es lo
que gana el intermediario.

Aggora **no** ingiere libros de órdenes reales (son producto de pago), así que el
spread que calcula es otro: el **spread entre las dos cotizaciones de la misma
empresa** en dos mercados. Vive en `market.arbitrage`, lo calcula
`ArbitrageTopology.java` y va en USD y en **puntos básicos** (bps, 1 bps = 0,01 %):

```text
spreadUsd = precio_americano - precio_europeo_convertido_a_USD
spreadBps = spreadUsd / precio_europeo * 10 000
```

Ejemplo aritmético con los precios semilla del YAML (**no es una medición**):
`ASML.AMS = 1383,40 EUR`, `EUR/USD = 1,1533` → 1.595,48 USD; `ASML = 1.574,62 USD` →
spread = −20,86 USD → ≈ **−131 bps** (la cotización europea está más cara).

### 2.5 Volumen y tamaño

El **tamaño** (`size`) es cuántas unidades se movieron en ese tick; el **volumen** es la
suma de tamaños en un periodo. En Aggora los ticks de referencia llegan con `size = 0`
(son cotizaciones, no operaciones) y por eso **no suman volumen**: hay un test que lo
comprueba (`MetricsTopologyTest.los_ticks_de_referencia_no_anaden_volumen`). Los ticks
sintéticos llevan tamaño aleatorio entre `min-size: 1` y `max-size: 500`.

### 2.6 VWAP

El **VWAP** (*volume-weighted average price*, precio medio ponderado por volumen) es el
precio al que realmente se ha negociado de media. La fórmula, en
`MetricsTopology.java`:

```text
VWAP = Σ(precio × tamaño) / Σ(tamaño)
```

Analogía: si compras 1 acción a 10 € y 99 a 12 €, tu precio medio **real** es
(10 + 99×12)/100 = 11,89 €, no 11 €. La media simple trata las dos compras igual; el
VWAP no.

### 2.7 Volatilidad

La **volatilidad** mide cuánto baila el precio dentro de la ventana. En este proyecto
es la **desviación típica de los precios** de la ventana, calculada con una suma de
cuadrados:

```text
varianza = Σ(precio²)/n − media²      (con max(0, …) para evitar un NaN)
volatilidad = √varianza
```

Dos matices honestos: **no está anualizada** y **no se calcula sobre rentabilidades**,
solo sobre precios (así está en el código). La `alerting-service` usa esta volatilidad
para nada todavía: la mejora pendiente es comparar el pico contra ella en vez de contra
un umbral fijo de puntos básicos (sección 10).

### 2.8 Ventana temporal: fija y móvil

Una **ventana** es "de qué trozo de tiempo quiero los datos". Aggora usa dos a la vez,
a propósito, para que se vea la diferencia en el propio topic
(`services/spring/analytics-streams/src/main/resources/application.yml`):

| Tipo | Tamaño | Avance | Solape | Para qué |
|---|---|---|---|---|
| **Fija** (*tumbling*) | 30 s | 30 s | ninguno | una foto por bloque: el VWAP de cada medio minuto |
| **Móvil** (*hopping*) | 60 s | 15 s | sí | una media que se refresca sin esperar al cierre |

El **grace** (5 s) es el margen que se espera antes de dar una ventana por cerrada:
los datos pueden llegar desordenados o tarde.

### 2.9 Libro de órdenes y prioridad precio-tiempo

El **libro de órdenes** es la lista de órdenes en espera: compras por un lado, ventas
por otro. La regla de oro de un mercado real es la **prioridad precio-tiempo**: primero
se cruza la mejor orden de precio y, a igual precio, la que llegó antes. Eso es
exactamente lo que implementa `OrderBook.java`:

- Los *bids* (compras) se ordenan de mayor a menor precio y los *asks* (ventas) de menor
  a mayor, con un `TreeMap` y una cola (`ArrayDeque`) por precio.
- El precio de la ejecución lo pone **la orden que ya estaba** en el libro (la parte
  pasiva), no la que llega.
- Solo hay órdenes **limitadas** (con precio); las de mercado quedan fuera.

Analogía: una cola del pan con dos reglas: primero quien paga más, y a igual pago,
quien llegó antes.

### 2.10 Ejecución

Una **ejecución** (o *fill*) es el resultado de cruzar dos órdenes: cuánto, a qué precio
y entre qué cuentas. Es el evento que publica el motor en `orders.executions` y el que
alimenta a la cartera. En el código, `Execution` (Avro) lleva `executionId`, `symbol`,
`price`, `quantity`, `currency`, `buyOrderId`, `sellOrderId`, `buyAccountId`,
`sellAccountId` y `executedAt`.

### 2.11 Cartera, posición y coste medio

Una **posición** es cuánto tienes de un instrumento. El **coste medio** es a cuánto te
salió de media lo que tienes abierto. La aritmética de Aggora está en
`PortfolioTopology.apply(...)` y tiene una regla que hay que interiorizar: **el coste
medio solo cambia al abrir o aumentar; al cerrar, lo que se materializa es el
resultado**.

Ejemplo aritmético con la fórmula del código:

| Paso | Cantidad | Precio | Coste medio | P&L realizado |
|---|---|---|---|---|
| Compras 100 | 100 | 10 | 10,00 | 0 |
| Compras 100 más | 200 | 12 | **11,00** | 0 |
| Vendes 50 | 150 | 13 | 11,00 (no cambia) | **+100** = (13−11)×50 |
| Vendes 200 | −50 | 14 | **14,00** (el resto abre al precio nuevo) | +100 + (14−11)×150 = **+550** |

### 2.12 Exposición, margen y `marginBreach`

La **exposición** es cuánto dinero tienes en riesgo: `|cantidad| × coste medio`. El
**margen** es el límite que no quieres superar. Cuando la exposición lo pasa, el sistema
marca `marginBreach` (aviso de margen superado) en cada actualización de posición, y
`alerting-service` lo convierte en una alerta `CRITICAL`.

En Aggora el límite es **global y configurable** (`aggora.margin-limit: 500000`), no por
cuenta ni por instrumento: suficiente para el objetivo de la fase (producir el aviso y
verlo saltar), pero en un sistema real sería por cuenta, con reglas de margen de verdad.
Es una simplificación consciente, no un olvido.

### 2.13 Arbitraje: el caso ASML

**Arbitraje** es ganar dinero con la misma cosa a dos precios distintos: comprar donde
está barata y vender donde está cara. Para que tenga sentido, las dos cotizaciones
tienen que ser comparables, y ahí está el trabajo:

1. `ASML.AMS` cotiza en **euros**; `ASML` en **dólares**. Comparar 1.383 EUR con 1.574
   USD no significa nada.
2. Se convierte el precio europeo a USD con el **tipo de cambio de referencia** y todo
   el contrato `ArbitrageSpread` queda en USD (el precio original en EUR sigue en
   `market.ticks.canonical`).
3. Los dos precios no llegan en el mismo milisegundo, así que el join lleva una
   **ventana de 5 s**: solo se cruzan precios que son comparables en el tiempo.
4. El spread real **solo existe cuando los dos mercados están abiertos a la vez**:
   13:30–15:30 UTC es el solape de NASDAQ y Euronext. Fuera de esa franja, una pata no
   cotiza y **no hay spread que calcular**. Que el sistema no invente un spread con un
   precio de hace horas es la respuesta correcta, no un fallo.

### 2.14 Tipos de cambio de referencia

El tipo de cambio con el que se convierte vive en un topic **compactado**,
`market.fx.reference`, con clave = par (`EUR/USD`). Es **dato de referencia**: interesa
el **último valor** de cada par, no el historial. Se lee como **GlobalKTable** (sección
3.12), que es el patrón para enriquecer un stream con una tabla pequeña.

### 2.15 Contrato y esquema (por qué el `eventTime` dejó de ser un número opaco)

En la Fase 1 cada servicio **adivinaba** qué era cada campo del JSON. En la Fase 2 se
introdujo **Avro**: cada mensaje lleva un byte mágico, el **ID de su esquema** y los
datos en binario; el esquema no viaja en el mensaje, se pide al **Schema Registry** por
ese ID. Lo que cambia de verdad:

- `price` es un **decimal lógico** (nunca coma flotante: en dinero 0,1 + 0,2 no es 0,3)
  y `eventTime` un **`timestamp-millis`**, que es lo que declara que ese número es una
  fecha. Ver `services/schemas/canonical.avsc`.
- El mismo dato puede tener **dos contratos distintos**: el normalizador lee el crudo
  (`market.ticks.raw-value`) y publica el canónico
  (`market.ticks.canonical-value`). Son subjects independientes y por eso el código
  construye un objeto nuevo en vez de reenviar el que leyó.

---

## 3. Kafka desde cero, con las analogías de este proyecto

Cada apartado: idea llana → nombre técnico → dónde vive en Aggora. Si quieres la
versión larga de un concepto suelto, está en `docs/kafka-101.md`.

### 3.1 Log y offset

**Idea.** Un topic es una **cinta de casillas que solo crece**. Cada mensaje que entra
recibe un número: su **offset**. Leer no borra nada; los mensajes se borran por
antigüedad o tamaño, nunca porque alguien los haya leído.

**Analogía.** El número de ticket de la panadería: el ticket 47 no desaparece porque lo
hayas usado.

**En este repo.** `market.ticks.raw` con 6 particiones; el offset se ve en cada línea de
log del consumidor (`offset=1785`). Ojo: "offset 1785" sin decir **partición** no
significa nada.

### 3.2 Topic

**Idea.** El canal con nombre por el que viajan los mensajes.

**Analogía.** La estantería de la panadería con su cartel.

**En este repo.** Los declara **quien escribe en ellos**, no el consumidor ni un
`kafka-topics.sh` a mano: en Spring con beans `NewTopic`
(`services/spring/*/src/main/java/**/config/KafkaTopicsConfig.java`), en Quarkus con el
`AdminClient` en un `@Observes StartupEvent`
(`services/quarkus/*/src/main/java/**/kafka/TopicCreator.java`), y en Lambda los crea el
despliegue. La autocreación de topics está **apagada**
(`KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"` en `infra/docker-compose.yml`): un error
tipográfico en un nombre debe **fallar**, no crear un topic fantasma de 1 partición
(pasó, y está en `docs/decisions.md`, Fase 2).

### 3.3 Partición y clave (y por qué la clave es el símbolo)

**Idea.** Un topic se corta en varios carriles paralelos, las **particiones**. La
**clave** decide en qué carril cae cada mensaje (Kafka calcula un hash de la clave).

**Analogía.** El código postal decide a qué centro de reparto va tu paquete; las cajas
del supermercado atienden en paralelo.

**En este repo.** `market.ticks.raw` tiene **6 particiones** y la clave es el **símbolo**.
Así todos los ticks de AAPL caen siempre en la misma partición y se leen **en orden**.
Contrapartida real que se midió: con 12 símbolos y 6 carriles, el hash **no** cae
repartido al 50 %; en una prueba el carril 0 se quedó vacío. El reparto hay que mirarlo,
no suponerlo.

### 3.4 Orden por partición

**Idea.** El orden **solo** se garantiza dentro de una partición. Entre el carril 2 y el
5 no hay ningún orden.

**Analogía.** Se respeta el orden de la cola de cada caja, no el orden entre cajas.

**En este repo.** De aquí sale la decisión del motor de matching: la clave de
`orders.incoming` es el **símbolo** y no la cuenta (aunque el spec proponía la cuenta),
porque el libro de órdenes es por instrumento y sus órdenes tienen que caer juntas. Es
una **desviación consciente del spec**, documentada en `docs/decisions.md`. Y de aquí
sale que el estado de cartera se clave por `cuenta|símbolo` pero se re-clave por cuenta
antes de publicar, lo que obliga a reparticionar (Kafka Streams lo hace solo, con el
topic `positions-store-repartition`).

### 3.5 Grupo de consumo y rebalanceo

**Idea.** Un **grupo de consumo** es un equipo de lectores que se reparten los carriles:
**una partición solo la lee un miembro del grupo a la vez**. Cuando entra o sale un
miembro, el reparto se rehace: eso es el **rebalanceo**.

**Analogía.** Varios repartidores con la misma ruta: se reparten los barrios, y ningún
barrio lo hace dos personas. Llega uno nuevo y el jefe reparte otra vez.

**En este repo.** El grupo `ingestion-normalizer` con 6 particiones: 1 instancia lee 6;
2 instancias leen 3 y 3; 8 instancias dejan 2 mirando (el paralelismo máximo es el
número de particiones). El reparto lo canta `RebalanceLogger.java`:
`REVOCADAS 6 particiones` → `ASIGNADAS 3`. Y un detalle de vida real que se midió:
matando con `kill -9`, el grupo no se entera hasta que pasa el *timeout* de sesión
(~45 s por defecto) y el rebalanceo tarda; cerrando bien (`Ctrl-C`), es inmediato.

### 3.6 Commit manual y at-least-once

**Idea.** El consumidor apunta en qué casilla va: ese apunte es el **commit** (la
confirmación del offset). Si apunta **después** de procesar, un fallo a media faena
provoca que al reiniciar se **repita** lo no confirmado: eso es *at-least-once* (al
menos una vez: puede repetir, no puede perder).

**Analogía.** El **marcapáginas** del libro.

**En este repo.** `TickConsumer.java` con `ack-mode: manual_immediate` y
`ack.acknowledge()` **después** de procesar. Cuánto se repite depende de cada cuánto
apuntes: apuntando tras cada mensaje, como máximo 1. El ejercicio para verlo: subir
`aggora.processing-delay-ms` a 300, matar el proceso con `kill -9` y ver cómo al
reiniciar se relee. La alternativa (auto-commit) apunta antes de procesar: si se cae,
el mensaje se da por hecho y **se pierde**.

### 3.7 Lag

**Idea.** El **lag** (retraso) es cuántas casillas lleva el lector por detrás de lo
escrito: `LOG-END-OFFSET − CURRENT-OFFSET`.

**Analogía.** Los libros que te quedan por leer de la pila.

**En este repo.** Es **la** métrica de salud, y sale en
`kafka-consumer-groups.sh --describe --group ingestion-normalizer`. Un lag que sube y
baja es normal; uno que sube siempre, no. Y el aviso más caro de todo el laboratorio:
**lag 0 con el productor muerto** (sección 7.12) — los offsets avanzan y no se publica
nada, así que el lag por sí solo **miente**.

### 3.8 Topic compactado

**Idea.** Un topic normal borra por antigüedad; uno **compactado** se queda con el
**último valor de cada clave**. Es para "el estado actual", no para "el historial".

**Analogía.** Una agenda de direcciones: no guardas todas las direcciones donde vivió
alguien, guardas la última.

**En este repo.** Tres usos: `market.fx.reference` (el último tipo de cambio por par),
`audit.events` (el último estado de cada entidad) y `_schemas` (el topic interno del
Schema Registry, que hubo que declarar a mano porque con la autocreación apagada el
registry se quedaba atascado). Dos lecciones medidas: un topic compactado **no es un
histórico fiable**, solo el último estado por clave (sección 7.2), y un duplicado en un
topic compactado con clave por entidad es **inofensivo**.

### 3.9 DLT y reintentos

**Idea.** Hay tres respuestas para lo que falla, de menor a mayor gravedad: **reintento
en el sitio** (fallo de un instante), **topic de reintento** (el fallo tarda, por
ejemplo la base de datos caída) y **DLT** (*dead-letter topic*, topic de descartes: el
mensaje es venenoso y no se va a poder procesar nunca).

**Analogía.** Un paquete roto: si el destinatario no estaba, se reintenta; si la
dirección es ilegible, se aparta con una etiqueta que dice qué le pasa.

**En este repo.** El normalizador manda los ticks inválidos a `market.ticks.raw.DLT`
**con el motivo en la cabecera `x-dlt-reason`**; el motor de matching hace lo mismo con
`orders.incoming.DLT` dentro de la transacción; y la auditoría usa el patrón
declarativo `@RetryableTopic` (topics `.retry-*` y `.DLT`). Dos lecciones que valen más
que la teoría: (1) el DLT **hay que declararlo** (con la autocreación apagada, el
publicador de descartes no puede crear el topic); (2) **el veneno se decide por el
mensaje, nunca por un contador** (sección 7.4). Cuánto se reintenta es una **decisión de
negocio**: un tick de hace dos minutos ya no sirve (reintentos cortos), un registro de
auditoría perdido es inaceptable (reintentos largos).

### 3.10 Productor transaccional y exactly-once

**Idea.** Publicar la ejecución y confirmar el offset son **dos cosas**. Si se cae entre
una y otra, o se duplica o se pierde. Con una **transacción** pasan a ser una sola
operación: o las dos, o ninguna. Eso es *exactly-once* (exactamente una vez).

**Analogía.** Pagar y quedarte con el ticket en un solo gesto: o tienes las dos cosas, o
ninguna.

**En este repo.** `KafkaTransactionConfig.java` (Spring) y
`@Channel("executions") KafkaTransactions<Execution>` + `withTransactionAndAck`
(Quarkus). Las tres piezas: `transaction-id-prefix`, gestor de transacciones en el
contenedor y consumidor con **`isolation.level=read_committed`**. La prueba:
`read_committed → 121` frente a `read_uncommitted → 168` (47 ejecuciones abortadas que
existen en el log y no las ve nadie). Repetido en Quarkus: **83.572 frente a 83.582**.

**Ojo con dos trampas ya vividas:** el manejador de errores por defecto de Spring
**descarta** el mensaje tras agotar reintentos, que con transacciones es perder la orden
en silencio; y **`send()` es asíncrono**, así que `abortTransaction()` inmediato descarta
lo que el hilo emisor no ha mandado (8 fallos de 8, sección 7.15).

### 3.11 Kafka Streams: KTable, ventanas, state store, changelog

**Idea.** Kafka Streams es una librería para procesar los topics **dentro** de tu
aplicación, con estado. Un **KStream** es una secuencia de hechos (dos ticks son dos
hechos); una **KTable** es una tabla que se actualiza (de esos dos ticks se queda con el
último valor por clave). Uno es la película, la otra la foto.

**Analogía.** El state store es la libreta donde apuntas las cuentas de la ventana; el
**changelog** es la fotocopia de esa libreta que guardas en Kafka, para poder
reconstruirla si pierdes la original.

**En este repo.** `MetricsTopology.java` (ventanas tumbling y hopping sobre un state
store en RocksDB con su changelog), `PortfolioTopology.java` (KTable de posiciones) y
`AlertingTopology.java` (reglas). Los topics internos los crea Streams solo
(`analytics-streams-metrics-tumbling-store-changelog`), y llevan
`replication.factor: 3` porque el estado también tiene que sobrevivir a un broker caído.

### 3.12 Joins: stream-stream y stream-GlobalKTable

**Idea.** Un **join** cruza dos fuentes por una clave. Si las dos son streams, hace
falta una **ventana de tiempo** (los dos datos no llegan a la vez). Si una es una tabla
**global** (copia entera en cada instancia), no hace falta que las claves coincidan: la
clave de búsqueda se calcula del propio registro.

**Analogía.** Cruzar dos listas: la de precios de dos mercados (con la condición "que
sean del mismo momento") y la de precios con el listín de cambios (que no cambia de
forma).

**En este repo.** `ArbitrageTopology.java` practica los dos: stream-**GlobalKTable**
para convertir el precio europeo a USD con `market.fx.reference`, y stream-**stream**
con ventana de 5 s para cruzar `ASML.AMS` y `ASML`. Detalle que costó un test en rojo:
el join stream-stream guarda la pata izquierda en un state store con ventana, así que
**hay que decirle los serdes** (`StreamJoined.with(...)`).

### 3.13 Punctuator (vigilar la ausencia)

**Idea.** Detectar que algo **no** llega no se puede hacer reaccionando a los mensajes:
si no llegan, no hay nada que reaccione. Un **punctuator** es una función que Streams
llama por reloj, haya datos o no.

**Analogía.** El vigilante que mira el reloj cada minuto, no el que espera a que suene
el timbre.

**En este repo.** `StaleFeedDetector.java`: `context.schedule(checkInterval,
PunctuationType.WALL_CLOCK_TIME, …)` y avisa **una vez por episodio** (si no, un mercado
cerrado generaría una alerta cada vuelta).

### 3.14 Consultas interactivas

**Idea.** Como el estado está en disco y en memoria, se le puede preguntar a la
aplicación **en marcha** por él, sin pasar por Kafka.

**Analogía.** Preguntarle al cocinero qué queda en la olla, en vez de esperar al menú.

**En este repo.** `GET /analytics?symbol=EUR/USD&minutes=3` en el puerto 8085. Dos
detalles con historia: el símbolo va como **parámetro de consulta** (los pares llevan
barra, `EUR/USD`, y en la ruta partía la URL), y el endpoint devuelve un **DTO propio**
porque Jackson revienta intentando serializar el `getSchema()` de un registro Avro.
Límite conocido: cada instancia solo conoce **sus** particiones, así que con varios
despliegues habría que preguntar a la que tiene la clave (o a todas).

### 3.15 Particiones, réplicas, ISR y `min.insync.replicas`

**Idea.** Una **réplica** es una copia de una partición en otro broker. El **ISR**
(*in-sync replicas*) son las copias que están al día; Kafka solo elige líder entre
ellas. `min.insync.replicas` es cuántas copias al día hacen falta para dar una escritura
por buena. Con `acks=all`, eso es lo que convierte "tengo copias" en "no pierdo datos".

**Analogía.** Tres copias de un documento en tres cajas fuertes: puedes perder una, pero
no firmas nada si solo queda una.

**En este repo.** Clúster de **3 brokers KRaft** (cada nodo broker y controller),
`KAFKA_DEFAULT_REPLICATION_FACTOR: 3` y `KAFKA_MIN_INSYNC_REPLICAS: 2`. Los
experimentos están en `docs/kafka-101.md` capítulo 16: con **un** broker caído se sigue
escribiendo (offsets 639→705); con **dos**, la escritura **se congela** (751→751) y el
productor da `NOT_ENOUGH_REPLICAS` — que **no es un fallo, es la garantía funcionando**;
al volver, el offset salta a 1310 con lo que estaba en el buffer y no se pierde nada. Un
clúster de 3 sobrevive a 1 caída, no a 2 (para dos hacen falta 5).

### 3.16 Fan-out con grupo propio (el gateway)

**Idea.** Hasta aquí todos los grupos eran de **trabajo**: se reparten las particiones y
cada mensaje lo procesa **uno**. Para una pantalla quieres lo contrario: **todos** los
mensajes. Y eso es lo que da un **grupo de consumo distinto**: cada grupo recibe **su
propia copia** de cada mensaje.

**Analogía.** El grupo de trabajo es un equipo que se reparte los barrios; el fan-out es
una suscripción al periódico: todos los suscriptores reciben el mismo ejemplar.

**En este repo.** `gateway-ws` usa su `group.id` propio (`gateway-ws`, y
`gateway-ws-quarkus` en la versión de Quarkus) sobre los mismos tres topics. Si
compartiera grupo con la analítica, le quitaría particiones y las dos cosas irían a
medias. Los dos gateways corren **a la vez a propósito**: es la demostración más barata
de que un grupo no es un canal exclusivo. Lo que se paga: cada grupo es un consumidor
más (lee de disco, ocupa memoria en el broker y aparece en el lag).

---

## 4. El recorrido fase por fase (Fase 0 a Fase 10)

Cada fase con la misma plantilla: **(a)** el problema, **(b)** qué se construyó,
**(c)** las decisiones y su porqué, **(d)** cómo se verificó, **(e)** qué se rompió. Al
final, preguntas de entrevistador con su respuesta. El orden importa: cada fase existe
porque la anterior dejó algo sin resolver.

### Fase 0 — Infraestructura

**(a) El problema.** Hace falta un Kafka para aprender, levantarlo con un comando, que se
vea desde dentro del devcontainer y desde WSL, y no pelearse con ZooKeeper.

**(b) Qué se construyó.** `infra/docker-compose.yml` (al principio un broker; hoy tres),
la red externa `aggora-net` y `.devcontainer/devcontainer.json`. Imágenes y versiones
exactas: `apache/kafka:3.9.0`, `confluentinc/cp-schema-registry:8.3.1`,
`postgres:16-alpine`, `danielqsj/kafka-exporter:v1.8.0`, `prom/prometheus:v2.54.1`,
`grafana/grafana:11.2.0`.

**(c) Las decisiones.** Kafka en modo **KRaft** (sin ZooKeeper: un proceso menos);
imagen `apache/kafka` (Kafka puro, sin Confluent en el broker); red **`aggora-net`
externa** (compartida por devcontainer e infra, y `external: true` para que
`docker compose down` no se lleve los datos); **doble listener** INTERNAL/EXTERNAL
(el devcontainer resuelve `kafka:9092`, el host entra por `localhost:29092`); **Java 21
LTS** para las dos implementaciones. Detalle completo en `docs/decisions.md`, Fase 0.

**(d) Cómo se verificó.** `docker network create aggora-net`, `docker compose -f
infra/docker-compose.yml up -d` y, desde el devcontainer, `nc -zv kafka 9092` →
`succeeded` (`CONTRIBUTING.md`, Fase 0a).

**(e) Qué se rompió.** El nombre del devcontainer **cambia en cada rebuild**
(`eager_allen` → `charming_spence` → `condescending_keldysh`), así que los comandos con
el nombre a pelo dejan de funcionar: de ahí el
`DEVCONTAINER=$(docker ps --format '{{.Names}}' | grep -v aggora | head -1)`. Y si se
purga Docker Desktop, la red desaparece: `docker network create aggora-net` otra vez.

**Preguntas de entrevistador**

1. *¿Por qué KRaft y no ZooKeeper?* Porque es el modo actual de Kafka, quita un proceso
   que mantener y en este proyecto el quórum de controllers vive en los propios brokers
   (`KAFKA_CONTROLLER_QUORUM_VOTERS` en `infra/docker-compose.yml`).
2. *¿Por qué dos listeners?* Porque el cliente dentro de `aggora-net` resuelve el
   nombre `kafka-1` y el de fuera (WSL/Windows) no; el EXTERNAL publica
   `localhost:29092`. Un solo listener obligaría a `host.docker.internal`, que no es
   portable fuera de Docker Desktop.
3. *¿Por qué la red es `external: true`?* Para que la red la cree una vez el humano y
   sobreviva a `docker compose down`; ahí está el estado (Kafka no tiene volúmenes en
   este compose).

### Fase 1 — Spring Boot: fundamentos

**(a) El problema.** Producir y consumir de verdad, con **commit manual**, y ver qué
pasa cuando entra una segunda instancia en el grupo.

**(b) Qué se construyó.** El módulo `services/spring/market-data-simulator`
(`MarketDataSimulatorApplication`, `TickProducer`, `TickEngine`, `PriceWalk`,
`Exchange`, `KafkaTopicsConfig`, `AggoraProperties`) y
`services/spring/ingestion-normalizer` (`TickConsumer`, `RebalanceLogger`,
`KafkaConsumerConfig`, `KafkaTopicsConfig`). Primero con JSON plano, sin Schema
Registry.

**(c) Las decisiones.** Proveedor Twelve Data; **sin API key** el simulador arranca
igual (semilla + ticks sintéticos) y avisa por log; Maven multi-módulo; **sin módulo
`common`** (cada servicio define su copia del contrato: ese dolor es el que justifica
el Schema Registry en la Fase 2); el topic lo crea **quien escribe**; **key = símbolo**;
productor **idempotente** (`acks=all`); JSON sin cabeceras `__TypeId__`; consumidor con
**`manual_immediate`**; horarios de mercado sin festivos.

**(d) Cómo se verificó.** `market.ticks.raw` con 6 particiones, ~53 ticks/s con
key = símbolo, productor idempotente sin fallos, consumidor con lag 0, rebalanceo
**3/3** al arrancar una segunda instancia y reprocesión de offsets tras un `kill -9`
(`CONTRIBUTING.md`, Fase 1). 17 tests unitarios en verde.

**(e) Qué se rompió.**

- **El `eventTime` viajaba como número decimal de segundos** (`1789395320.744403168`),
  que es lo que hace por defecto el serializador JSON de Spring. Se dejó a propósito:
  es exactamente el problema que resuelve un contrato (`docs/decisions.md`, "Formato del
  timestamp en el topic").
- `target/` quedó propiedad de `root` por compilar con Maven desde fuera del
  devcontainer; después, `...jar is read-only`. Regla: compilar con
  `docker exec -u vscode`.

**Preguntas de entrevistador**

1. *¿Por qué la key es el símbolo?* Porque el orden solo se garantiza **dentro de una
   partición** y los precios de un instrumento tienen que leerse en orden. Con la key
   aleatoria, la media móvil sería basura.
2. *¿Qué garantiza el commit manual?* **At-least-once**: el offset se confirma después
   de procesar, así que un fallo repite, no pierde.
3. *¿Por qué no hay módulo común?* Porque el objetivo del proyecto es *sufrir* el
   problema que resuelve el Schema Registry. Se documenta en vez de esconderlo.
4. *¿Cómo se comporta con 8 instancias?* Con 6 particiones, 6 leen y 2 miran: el
   paralelismo máximo es el número de particiones.

### Fase 1b — Datos reales con dos proveedores

**(a) El problema.** Los planes gratuitos **no cubren los tres husos horarios** del
spec.

**(b) Qué se construyó.** `ReferenceSource` (interfaz), `TwelveDataClient`,
`AlphaVantageClient` y `ReferenceFeed` con dos `@Scheduled` y presupuesto diario por
proveedor.

**(c) Las decisiones.** Dos proveedores, **cada uno donde gana**: Twelve Data para
EEUU, forex y oro (endpoint por lotes, poll cada 15 min), Alpha Vantage para Euronext y
Shanghai (una petición por símbolo, 1/s, poll cada 3 h). El crudo spot no está gratis en
ninguno: se usa el ETF `USO` **y se dice en el YAML que es el ETF, no el barril**.

**(d) Cómo se verificó.** Comprobación símbolo a símbolo contra las dos APIs
(2026-09-14/15): Twelve Data sirve `AAPL`, `JPM`, `XOM`, `EUR/USD`, `USD/CNY`,
`XAU/USD`, `USO`; Alpha Vantage sirve `MC.PAR`, `OR.PAR`, `AIR.PAR`, `600519.SHH`,
`601398.SHH`. Los documentos hablan de **12 instrumentos con precio real**; el YAML
declara **14** entradas y el repo **no documenta** la diferencia (ver 2.3).

**(e) Qué se rompió.** Tres correcciones que salieron de probar:

- `/price` de Twelve Data responde en **dos formatos**: `{"price":"333.47"}` si pides un
  símbolo y `{"AAPL":{"price":"..."}}` si pides varios. El cliente solo entendía el
  segundo, así que AAPL (único de su grupo) se perdía.
- La propia clave del endpoint es `price`, así que confundir los formatos hacía que
  "price" pareciera un símbolo.
- Alpha Vantage **no usa códigos HTTP** para decir "te pasaste de cuota": responde
  `200` con `{"Information": "..."}`. Si no se mira el cuerpo, el aviso se cuela como si
  fuera un precio. Hay test para eso (`AlphaVantageClientTest.cuota_agotada_no_es_un_precio`).

**Preguntas de entrevistador**

1. *¿Por qué dos proveedores y no uno?* Porque ningún plan gratuito cubre EEUU, Europa
   y Asia; cada uno gana en una región.
2. *¿Cómo se respeta una cuota?* Con presupuesto diario por proveedor y parando el
   scheduler al llegar: 750 créditos en Twelve Data (un crédito = un símbolo) y 18
   peticiones en Alpha Vantage.
3. *¿Qué pasa sin API key?* El simulador arranca con precios semilla y ticks 100 %
   sintéticos, y lo avisa por log.

### Fase 2 — Schema Registry, Avro y migración a Spring Boot 4.1.1

**(a) El problema.** En la Fase 1 cada servicio **adivinaba** qué era cada campo del
JSON. Hay que declarar el contrato y hacerlo cumplir.

**(b) Qué se construyó.** Los **12 esquemas** en `services/schemas/*.avsc`, el
`avro-maven-plugin` (las clases Java se **generan**, no se escriben), `AvroSerdes` en
cada servicio, y el normalizador pasó a ser consumidor **y** productor (republica el
canónico). También el topic `_schemas`.

**(c) Las decisiones.** **Avro** y no Protobuf (los `.avsc` son JSON legible mientras se
aprende y los ejercicios de compatibilidad salen limpios); migrar a **Spring Boot
4.1.1**; `schema.registry.url` en `spring.kafka.properties` (es común a productor y
consumidor); **namespace distinto** para el canónico (`com.aggora.avro.canonical`)
porque los dos esquemas definen enums con los mismos nombres; `price` como **decimal
lógico** y `eventTime` como **timestamp-millis**; el normalizador **construye un objeto
nuevo** (los dos subjects evolucionan por separado).

**(d) Cómo se verificó.** 2 subjects (`market.ticks.raw-value`,
`market.ticks.canonical-value`), mensajes binarios en el crudo, 7.000 mensajes
procesados con 0 descartes y 0 errores (`CONTRIBUTING.md`, Fase 2); tras migrar a
Boot 4: **17 tests en verde y 63.500 mensajes procesados, 0 descartes, 0 errores**
(`docs/decisions.md`).

**(e) Qué se rompió.** Tres incidentes que valen más que la teoría:

1. **La autocreación de topics es una trampa.** Apareció un `market.ticks.raw` con
   **1 partición** que nadie había declarado: un `kafka-console-consumer` olvidado pedía
   metadatos y el broker lo creaba solo. Se apagó con
   `KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"`.
2. **Con la autocreación apagada, los topics internos hay que declararlos.** El Schema
   Registry no crea su `_schemas`, se quedó atascado y **el productor de la aplicación
   se quedó colgado**: el serializador Avro se ejecuta en el mismo hilo que hace el
   `send`, así que un registry que no responde bloquea la producción entera **sin
   errores en el log**. Se añadió un servicio `kafka-init`. (Nota: el compose actual de
   3 brokers **ya no incluye `kafka-init`** y el repo **no documenta** su retirada.)
3. **Jackson 2 → Jackson 3** (paquete `tools.jackson`), la autoconfiguración de Kafka se
   movió a `spring-boot-kafka` y los serializadores JSON de Spring Kafka pasaron a
   llamarse `JacksonJson*`; los de siempre revientan al arrancar con
   `NoClassDefFoundError: com/fasterxml/jackson/databind/JavaType`.

**Preguntas de entrevistador**

1. *¿Qué gana Avro sobre JSON?* Tipos declarados (decimal en dinero, timestamp de
   verdad), esquema validado por un notario antes de desplegar, y mensajes más pequeños.
2. *¿Por qué el mismo dato tiene dos subjects?* Porque el crudo y el canónico evolucionan
   por separado; el canónico puede ganar campos sin tocar el contrato del crudo.
3. *¿Qué aprendiste del registry que no responde?* Que sus timeouts importan tanto como
   los del broker: bloquea el hilo que serializa.

### Fase 3 — Kafka Streams: métricas, arbitraje y consultas interactivas

**(a) El problema.** Hasta aquí cada mensaje se procesaba por separado; una media móvil
**necesita memoria**. Y hay que cruzar dos fuentes y poder preguntar por el estado.

**(b) Qué se construyó.** `services/spring/analytics-streams`:
`MetricsTopology`, `ArbitrageTopology`, `AnalyticsConfig`, `KafkaTopicsConfig`,
`AvroSerdes`, `AggoraProperties`, `AnalyticsQueryController` y el topic
`market.analytics` + `market.arbitrage`.

**(c) Las decisiones.** **API de Kafka Streams a pelo** (no el binder de Spring Cloud
Stream: así la comparación con Quarkus es justa); **ASML en sus dos cotizaciones**
reales (`ASML` y `ASML.AMS`); el acumulador es un **registro Avro** (necesita serde para
el state store y el changelog); importes en `double` dentro del acumulador y **decimal**
en el contrato; ventanas cortas y configurables (30 s fijas, 60/15 s móviles);
`statestore.cache.max.bytes: 0` para ver la ventana llenarse; los tests de topología usan
`TopologyTestDriver` y una URL **`mock://`** del registry; el **tipo de cambio va a un
topic compactado** y se lee como GlobalKTable; la conversión de divisa va **antes** del
cruce; el join stream-stream lleva ventana de 5 s.

**(d) Cómo se verificó.** **6 tests de topología** (VWAP/media/volatilidad, solape de
ventanas, spread y sus dos casos "no hay") sin broker; en vivo, métricas y
`curl 'localhost:8085/analytics?symbol=EUR/USD&minutes=3'` devolviendo las últimas
ventanas. El spread real solo aparece en el solape NASDAQ+Euronext (13:30–15:30 UTC).

**(e) Qué se rompió.**

- Faltaban los **serdes en `StreamJoined.with(...)`**: el join guarda la pata izquierda
  en un state store con ventana y sin decirle los serdes Streams falla al arrancar.
  Costó un test en rojo.
- El endpoint devolvía `HttpMessageNotWritableException` porque Jackson intentaba
  serializar también el `getSchema()` del registro Avro: de ahí el **DTO propio**.
- `server.port` se puso **dentro de `spring:`** y Tomcat se fue al 8080 (va en la raíz
  del YAML).
- El símbolo no puede ir en la ruta porque los pares llevan barra (`EUR/USD` partía la
  URL): va como **parámetro de consulta**.

**Preguntas de entrevistador**

1. *¿Diferencia entre KStream y KTable?* El KStream es la película (cada tick es un
   hecho); la KTable es la foto (el último valor por clave).
2. *¿Por qué una GlobalKTable para el tipo de cambio?* Porque es una tabla pequeña (dos
   pares) que se copia entera en cada instancia, así que la clave de búsqueda puede
   calcularse del propio registro y no hace falta co-particionar.
3. *¿Por qué el join lleva ventana?* Porque los dos precios no llegan en el mismo
   milisegundo; sin ventana no hay nada que emparejar.
4. *¿Qué límite tienen las consultas interactivas?* Cada instancia solo conoce **sus**
   particiones; con varios despliegues hay que preguntar a la que tiene la clave.

### Fase 4 — Motor de matching con exactly-once

**(a) El problema.** El motor tiene que **publicar la ejecución** y **confirmar el
offset**: son dos pasos y con dos pasos no hay forma de acertar siempre (si publica y se
cae antes de confirmar, duplica; si confirma antes de publicar, pierde).

**(b) Qué se construyó.** `services/spring/order-matching-engine`: `OrderBook`,
`OrderBooks`, `OrderMatcher`, `KafkaTransactionConfig`, `KafkaTopicsConfig` y
`orders.incoming` + `orders.executions`. Las órdenes las genera `OrderFlowGenerator`
en el simulador.

**(c) Las decisiones.** La key de `orders.incoming` es el **símbolo, no la cuenta**
(**desviación consciente del spec**: el libro es por instrumento y sus órdenes tienen
que caer juntas); **exactly-once** con `transaction-id-prefix`, gestor de transacciones
en el contenedor y `read_committed`; el manejador de errores **aborta en vez de
descartar**; el libro vive **en memoria**; inyección de fallo **configurable**
(`fail-every-n-orders`).

**(d) Cómo se verificó.** **4 tests** del libro (cruce al precio pasivo, ejecución
parcial, sin cruce, prioridad por tiempo) y el experimento en vivo: con fallos
inyectados cada 10 órdenes, `read_committed` vio **121** mensajes y `read_uncommitted`
**168** — los **47 abortados** existen en el log y no los ve nadie.

**(e) Qué se rompió.**

- El `DefaultErrorHandler` por defecto, tras agotar reintentos, **descarta** el mensaje
  (confirma el offset). Con transacciones eso es **perder la orden en silencio**. Se
  configuró para relanzar y que la transacción se deshaga.
- No descartar en silencio tuvo su propio precio: un mensaje imposible **bloqueaba su
  partición para siempre**. Ese es exactamente el problema que resuelve el DLT (Fase 6).
- El fallo inyectado por **contador** nunca llegaba al DLT (al reintentar, el contador
  avanza y el mensaje se procesa bien). Se decidió por el `orderId` (ver 7.4).

**Preguntas de entrevistador**

1. *¿Por qué la clave no es la cuenta?* Porque el libro de órdenes es por instrumento:
   con la cuenta como key, cada partición tendría un libro incompleto.
2. *¿Cuáles son las tres piezas del exactly-once?* Productor transaccional,
   transacción en el contenedor de escucha (publicar y confirmar en el mismo commit) y
   consumidor `read_committed`.
3. *¿At-least-once o exactly-once?* Los dos, y sé lo que cuesta cada uno: el
   normalizador es at-least-once (idempotente o tolerable a repetidos) y el motor es
   exactly-once porque una ejecución repetida es una posición contada dos veces.

### Fase 5 — Cartera, alertas y auditoría

**(a) El problema.** Convertir ejecuciones en posiciones y P&L, detectar anomalías
(incluida la **ausencia** de datos) y guardar un rastro inmutable sin que la escritura
en base de datos y la publicación puedan desincronizarse.

**(b) Qué se construyó.** `services/spring/portfolio-risk` (`PortfolioTopology`,
`PortfolioConfig`, `AvroSerdes`), `services/spring/alerting-service`
(`AlertingTopology`, `SpikeDetector`, `StaleFeedDetector`), `services/spring/audit-log`
(`AuditStore`, `OutboxRelay`, `AuditConsumer`, `OutboxMessage`, `schema.sql`) y el
regreso de **Postgres** a la infraestructura.

**(c) Las decisiones.** Se añade **`currency` con valor por defecto** a `Order` y
`Execution` (primer cambio de esquema real, **COMPATIBLE**); el estado de cartera se
clava por **`cuenta|símbolo`** y en el topic por **cuenta** (re-clavado → repartición);
`isolation.level=read_committed` también en el Streams de cartera; el coste medio solo
cambia al abrir o aumentar; límite de margen **global** (500.000) configurable; el feed
parado se detecta con un **punctuator**; picos **una vez por episodio** + **histéresis**
(40 bps dispara, 20 rearma); `audit.events` **compactado** con key = entidad; el evento
se guarda en **JSON** (con el codificador de Avro) para que una auditoría se lea; índice
**único** por `(topic, partición, offset)` + `on conflict do nothing`.

**(d) Cómo se verificó.** 3 tests de cartera (coste medio ponderado, cierre con P&L,
vuelta de posición); en vivo, posiciones con divisa; en vivo, **122.000 eventos
auditados** y la bandeja de salida con `pendientes = 0` (`CONTRIBUTING.md`, Fase 5). El
ruido de alertas bajó de **~36.000 avisos a 686** en el mismo tiempo (`~65×`).

**(e) Qué se rompió.**

- **Una alerta que se repite no es una alerta:** la primera versión avisaba en cada
  actualización de ventana que superara el umbral (36.000 en dos minutos). Se arregló
  con "una vez por episodio" + histéresis.
- El driver de Postgres **no sabe convertir un `Instant`**: costó **327 errores** en el
  log descubrirlo. Se pasa `OffsetDateTime`.
- El camino con Postgres se verificaba **en vivo**, sin test de integración (la deuda se
  cerró después, con Testcontainers: ver Fase 10).

**Preguntas de entrevistador**

1. *¿Por qué el evento auditado y el recado van en la misma transacción?* Porque escribir
   en Postgres y publicar en Kafka son dos sistemas: si se cae entre las dos, o se pierde
   el evento o no queda constancia. El recado en la misma transacción de BD sí es atómico.
2. *¿Por qué el topic de auditoría es compactado?* Porque el publicador puede repetir
   (at-least-once) y, con clave por entidad, el duplicado se queda como el mismo último
   estado.
3. *¿Cómo se detecta que algo NO llega?* Con un punctuator, que se ejecuta por reloj
   aunque no entre ni un mensaje.
4. *¿Qué es la histéresis y por qué hizo falta?* Avisar a 40 bps y no rearmar hasta bajar
   de 20; con un solo umbral, un precio que oscila alrededor avisa sin parar.

### Fase 6 — Resiliencia y operación

**(a) El problema.** ¿Qué se hace con lo que falla? ¿Y cómo se sobrevive a la caída de un
broker? ¿Y cómo se ve lo que pasa?

**(b) Qué se construyó.** DLT y reintentos en tres consumidores (normalizador, motor y
auditoría), el **clúster de 3 brokers**, la observabilidad (kafka-exporter + Prometheus +
Grafana provisionados desde el repo), la **sonda de salud** `StreamsHealth` y los scripts
`scripts/start-services.sh` / `stop-services.sh`.

**(c) Las decisiones.** **Tres mecanismos** según el fallo (reintento en el sitio, topic
de reintento, DLT); el veneno se decide **por el `orderId`**; el DLT se **declara** en el
`KafkaTopicConfig`; el motivo viaja en la cabecera **`x-dlt-reason`**; reintentos
**largos en la auditoría y cortos en el resto** (decisión de negocio); **3 nodos** KRaft
con `KAFKA_DEFAULT_REPLICATION_FACTOR: 3` y `KAFKA_MIN_INSYNC_REPLICAS: 2`; los topics se
declaran **sin réplicas explícitas** (manda el default del broker); `replication.factor:
3` en los topics internos de Streams; panel y datasource **provisionados**; Grafana
anónimo en dev; `restart: unless-stopped` en la infraestructura.

**(d) Cómo se verificó.**

- 7 ticks inválidos descartados con su motivo; 24 órdenes venenosas descartadas **sin
  bloquear la partición**; la auditoría se recuperó sola tras parar Postgres.
- Con **1 broker caído**: `Isr: 3,1,2 → 3,1`, el líder se elige solo, offsets 639→705.
  Con **2 caídos**: offsets **751→751** y `NOT_ENOUGH_REPLICAS`. Al volver:
  `Isr: 1 → 1,3,2` y el offset salta a **1310** con lo que estaba en el buffer.
- Tras reiniciar el entorno entero: **29 topics, 22 esquemas y 131.932 eventos
  auditados** seguían ahí.
- El panel mostró lag de los 6 grupos y throughput (`market.analytics` ~308 msg/s,
  `market.ticks.raw` ~62 msg/s).

**(e) Qué se rompió.** (Todas están contadas en detalle en la sección 7.)

- **Carrera de arranque**: `MissingSourceTopicException` si el topic de origen no existe
  todavía → arranque en orden y esperando en `scripts/start-services.sh`.
- **GlobalKTable + topic compactado**: `OffsetOutOfRangeException` cuando la
  compactación adelanta el log por delante del checkpoint → reiniciar; queda anotado que
  un compactado **no es un histórico**.
- **El fallo silencioso**: Streams en ERROR con el proceso vivo; `REPLACE_THREAD` primero;
  `SHUTDOWN_APPLICATION` **peor** (**429 MB de log en 28 segundos** y el proceso tampoco
  muere); la respuesta buena es la **sonda de salud**.
- **500 en vez de 503** mientras se reconstruye el state store.
- El fichero suelto montado en Docker (`prometheus.yml`) se rompe al recrear el
  contenedor; los permisos `600` de los ficheros montados dejaban Prometheus (uid 65534)
  y Grafana (472) en bucle de reinicio → `chmod 644`.
- `--kafka.server` del exporter **no acepta una lista con comas**: hay que repetirlo por
  broker (`too many colons in address`).

**Preguntas de entrevistador**

1. *¿Qué pasa si se cae un broker?* Con 3 réplicas y `min.insync.replicas=2`, nada: el
   líder se elige solo y se sigue escribiendo. Con dos caídos, **se deja de escribir** a
   propósito.
2. *¿Por qué `NOT_ENOUGH_REPLICAS` no es un fallo?* Porque es la garantía funcionando:
   mejor parar que aceptar un mensaje que solo tiene una copia.
3. *¿Cómo distingues un error mortal de uno auto-reparable?*
   `TaskCorruptedException ... need to be re-initialized` puso ERROR y **se arregló solo**
   (Streams reconstruyó la tarea desde su changelog). Un motor en ERROR que no se
   recupera necesita reinicio.
4. *¿Por qué la sonda y no el manejador?* Porque un servicio **no debería decidir
   suicidarse**: quien levanta un proceso caído es el supervisor (systemd, Docker,
   Kubernetes) y quien le dice que está roto es la sonda.

### Fase 7 — Laboratorio de evolución de esquemas

**(a) El problema.** Cambiar un contrato que **ya está en marcha** y enterarse **antes**
de romperlo.

**(b) Qué se construyó.** `scripts/schema-evolution-lab.sh` (pregunta al registry por
seis cambios en las dos direcciones, enseña el `409` real, registra el bueno y limpia) y
`SchemaEvolutionTest` con `canonical-v2.avsc`.

**(c) Las decisiones.** El laboratorio **no produce mensajes** con los esquemas de
prueba (un mensaje apunta al ID de su esquema: si lo borras, queda ilegible para
siempre); **deja el registro como estaba** y se limpia solo con un `trap` si se corta; el
esquema propuesto vive en `src/test/resources` (en `services/schemas` rompería la
generación al duplicar `CanonicalTick`); el "consumidor antiguo" del test es la clase
`CanonicalTick` **ya compilada**; el nivel se queda en **BACKWARD**.

**(d) Cómo se verificó.** La tabla de veredictos, medida contra el registro de verdad
(`docs/schema-evolution-lab.md`):

| Cambio | BACKWARD | FORWARD |
|---|---|---|
| Añadir un campo **con** valor por defecto | COMPATIBLE | COMPATIBLE |
| Añadir un campo **sin** valor por defecto | RECHAZADO | COMPATIBLE |
| Renombrar un campo sin alias | RECHAZADO | RECHAZADO |
| Renombrar un campo con alias | COMPATIBLE | RECHAZADO |
| **Borrar un campo** | **COMPATIBLE** | RECHAZADO |
| Campo opcional (`["null","long"]`, default `null`) | COMPATIBLE | RECHAZADO |

El `409` real: `{"error_code":40901,"message":"Schema being registered is incompatible
with an earlier schema for subject \"market.ticks.canonical-value\""}`.

**(e) Qué se rompió.** Nada material: el laboratorio se diseñó para no dejar minas. Lo
que sí se aprendió es la **trampa** de la tabla: borrar un campo **pasa** el filtro
BACKWARD (el lector nuevo ignora lo que no conoce) y **rompe** a los consumidores ya
desplegados. "Compatible" no significa nada sin decir **en qué dirección**.

**Preguntas de entrevistador**

1. *¿Qué cambio es seguro en las dos direcciones?* Añadir un campo **con valor por
   defecto**. Todo lo demás obliga a elegir quién se despliega primero.
2. *¿Por qué no produces mensajes en el laboratorio?* Porque cada mensaje apunta al ID de
   su esquema; borrarlo deja esos mensajes ilegibles para siempre.
3. *¿Cómo se cambia un contrato de verdad?* Se despliega el campo como opcional, se
   espera a que nadie lo escriba y se quita; o **topic nuevo** (`market.ticks.canonical.v2`),
   que es un subject nuevo sin historial con el que ser incompatible.
4. *¿Por qué BACKWARD?* Porque es el proyecto donde los consumidores se despliegan
   **después** que los productores.

### Fase 8 — Port a Quarkus, nativo y los dos stacks

**(a) El problema.** Implementar **lo mismo** en un segundo framework, sin cambiar la
funcionalidad, para comparar de verdad (y no dos repos paralelos).

**(b) Qué se construyó.** `services/quarkus/` con los **ocho** servicios portados,
`scripts/start-quarkus-stack.sh` (los dos stacks a la vez) y el nativo de GraalVM para
`ingestion-normalizer` y `market-data-simulator` (`scripts/build-native.sh`).

**(c) Las decisiones.** `services/pom.xml` es un **agregador que no hereda de nadie** y
cada implementación tiene su padre; `services/schemas/` **no se movió** (los contratos son
de las dos); cada implementación lleva su sufijo en el `artifactId`
(`ingestion-normalizer-spring` / `-quarkus`); los mismos topics, el mismo `group.id` y los
mismos serializadores (intercambiables); sin health ni métricas donde la versión Spring
tampoco las tiene (añadirlas falsearía la comparación); en los dos stacks a la vez, grupo
propio, topics de salida con sufijo `.q` y `application-id` y directorio de estado
propios.

**(d) Cómo se verificó.**

- **Intercambiabilidad**: parando el de Spring y arrancando el de Quarkus con el mismo
  `group.id`, el pipeline siguió (offsets subiendo, lag 0-2, `analytics-streams` de
  Spring respondiendo 200) y los inválidos fueron al DLT con **la misma cabecera y el
  mismo texto**.
- **Exactly-once cruzando implementaciones**: el `portfolio-risk` de Quarkus consumió
  `orders.executions` publicado por el motor **transaccional de Spring**.
- **Nativo**: **0,022 s** de arranque, RSS **114-124 MB**, 15 hilos, binario de **91,7 MB**.
- **Los dos stacks a la vez**: `market.analytics` 3.681.665 frente a `market.analytics.q`
  2.899.871; `portfolio.updates` 42.130 frente a 1.920; los de Quarkus arrancaron entre
  1,1 s y 4,2 s (más lentos que en solitario: compiten por CPU).

**(e) Qué se rompió.** Los tropiezos de esta fase son oro para una entrevista:

- El **scheduler de Quarkus no baja de un segundo** (`An every() value less than 1000 ms
  is not supported`); se emiten `1000 / tick-interval-ms` ticks por vuelta (mismo ritmo,
  pero en ráfagas).
- `SRMSG00019: Unable to connect an emitter with the channel ticks-raw` cuando el job
  dispara antes de que los canales estén conectados → `skipExecutionIf =
  Scheduled.ApplicationNotRunning.class`. (El `SRMSG00051` que menciona la consigna **no
  aparece en el repo**: no está documentado; el que está es el `00019`.)
- Un `String` obligatorio con valor vacío **no arranca** (`defined as the empty String
  which the Converter considered to be null`): la api key es `Optional<String>`.
- Las claves con puntos puestas en **YAML** salen entre comillas y Quarkus no las
  reconoce: las propiedades planas van a `application.properties`.
- **Avro 1.12.2** (el del BOM de Quarkus) **enciende el validador de clases** y los serdes
  de Streams revientan con `Forbidden com.aggora.avro.canonical.CanonicalTick`; Spring usa
  1.12.1 y no le pasa (el aviso queda escrito: el día que suba, le pasará).
- El nativo: **BouncyCastle** (TLS), **Brotli** (compresión), **commons-compress + xz** y
  la **reflexión** de los serializadores. La frase que resume los cuatro: **en una imagen
  nativa no existe lo "opcional en tiempo de ejecución"**. Se resolvió la reflexión
  *generando el fichero desde los propios jars* (**169 clases**), y con eso el segundo
  binario salió a la primera.

**Preguntas de entrevistador**

1. *¿Qué cambió entre frameworks en Kafka Streams?* **Nada**: las tres topologías se
   copiaron tal cual, incluido el punctuator. Lo que cambia es quién envuelve el motor
   (`@EnableKafkaStreams` frente a `@Produces Topology`) y de dónde sale la configuración.
2. *¿Dónde ayudó el modelo reactivo?* La transacción se pide en el código
   (`withTransactionAndAck`), los canales son tipados y `/q/health` trae la sonda del
   motor puesta.
3. *¿Dónde estorbó?* Un `Emitter` no tiene `RecordMetadata` (el `KafkaTemplate` sí), el
   scheduler no baja de 1 s y la configuración es estricta (una variable vacía es
   "ausente").
4. *¿Por qué el nativo no se hizo con `-Dnative`?* Porque los poms escritos a mano no
   tienen el perfil `native`: `-Dnative` daba BUILD SUCCESS y **no generaba binario**. Hay
   que usar `-Dquarkus.package.type=native`.

### Fase 9 — Informe comparativo y `gateway-ws`

**(a) El problema.** Comparar con números (no con opiniones) y, de paso, tener forma de
**ver** la plataforma funcionando.

**(b) Qué se construyó.** `SPRING_VS_QUARKUS.md` y el octavo servicio `gateway-ws` en las
dos implementaciones (`services/spring/gateway-ws`, `services/quarkus/gateway-ws`),
más `scripts/GatewayLiveCheck.java`.

**(c) Las decisiones.** El gateway usa **`group.id` propio** (`gateway-ws` y
`gateway-ws-quarkus`): con grupo propio Kafka le da **su copia de cada registro**; si
compartiera grupo, le quitaría particiones a la analítica. **Commit automático** (en un
fan-out, si se pierde un tick el siguiente trae el precio nuevo), **envío asíncrono**
(una sesión lenta no puede frenar al hilo que consume) y **sin estado** (al arrancar, la
pantalla se rellena con los siguientes eventos). Los dos gateways corren a la vez a
propósito.

**(d) Cómo se verificó.** `java scripts/GatewayLiveCheck.java localhost 8089 12`, con los
dos stacks en marcha: handshake `101 Switching Protocols` y, en 12 s, **180 ticks,
10 posiciones y 30 alertas** (Spring) / **11 alertas** (Quarkus). La comparación de
arranque y memoria de los siete servicios, en `SPRING_VS_QUARKUS.md` (Quarkus gana el
arranque en los siete, entre un 24 % y un 47 %, y la memoria en seis de siete).

**(e) Qué se rompió.** Dos detalles de API, ninguno grave: en Spring hay que elegir el
envío asíncrono a mano (`getAsyncRemote()`); si usas `getBasicRemote()`, el hilo de Kafka
se queda esperando al cliente más lento y **el pipeline entero se frena** (y el
compilador no avisa). Y "Quarkus usa Jackson" no significa "el mismo Jackson": el BOM de
Quarkus 3.39 fija **Jackson 2** (excepción *checked*: `try/catch` obligatorio) y Spring
Boot 4 va con **Jackson 3** (*unchecked*).

**Preguntas de entrevistador**

1. *¿Spring o Quarkus?* Con números: Quarkus gana arranque y memoria; pero **no migraría
   siete servicios Spring sanos por un segundo de arranque**. Quarkus se gana el sitio
   cuando el arranque o la escala a cero son el problema.
2. *¿Por qué el gateway no es un servicio de trabajo?* Porque no procesa: **reparte**.
   Necesita todos los mensajes, no un reparto.
3. *¿Por qué los dos gateways pueden correr a la vez sobre los mismos topics?* Porque
   tienen grupos distintos: los grupos de consumo **no son canales exclusivos**.

### Fase 10 — Despliegue, Lambda y el test de estrés

**(a) El problema.** Llevar la plataforma fuera del portátil y medir de verdad el
throughput de los dos stacks.

**(b) Qué se construyó.** `deploy/` (runbook, Terraform, unidad de systemd, compose de la
VM, script de artefactos), el tercer árbol `services/lambda/ingestion-normalizer-lambda`
y el laboratorio de throughput (`scripts/AvroLoadGenerator.java` +
`scripts/throughput-test.sh`).

**(c) Las decisiones.** El broker y los topics se crean **a mano** (Terraform no debe
poder crear lo que factura por hora y guarda estado); lo que tiene estado va a **una VM
siempre encendida** con systemd y `Restart=always`; solo el normalizador va a **Lambda**
con *event source mapping*; los secretos entran por **SSM `SecureString`**; **no EKS**
(el plano de control se cobra por hora; si algún día apetece Kubernetes, un k3s). Y en
Lambda no es "la misma app con otro envoltorio": **no hay bucle de consumo**, el valor
llega en **base64** y **los offsets los confirma el servicio**; por eso un mensaje
venenoso **no lanza excepción** (lanzar haría que Lambda reintentara el lote entero) y
solo se propaga el fallo de infraestructura, que acaba en la *on-failure destination*
(SQS).

**(d) Cómo se verificó.** Todo lo verificable **sin cuenta de AWS**: los tres árboles
compilan y sus tests pasan (`mvn test` en `services/`), el handler de Lambda tiene **5
tests** sin broker ni Docker, la infraestructura pasa `terraform fmt -check -recursive` +
`init -backend=false` + `validate` (`Success! The configuration is valid.`), el compose
de la VM valida (`docker compose config -q`, exit 0) y `systemd-analyze verify` cazó un
bug real (`Environment=JAVA_OPTS=-Xms128m -Xmx320m` sin comillas hacía que systemd
ignorase `-Xmx320m`). Y el test de estrés, con los dos stacks a la vez:

| Ráfaga | Entrada | Canónico S/Q | Analítica S/Q | CPU/msg S | CPU/msg Q |
|---|---|---|---|---|---|
| 1.500 msg/s (20 s) | 29.998 | 29.998 / 29.998 | 149.990 / 149.990 | 0,61 ms | 1,27 ms |
| 4.000 msg/s (20 s) | 79.996 | 79.996 / 79.996 | 399.980 / 399.980 | 0,47 ms | 0,81 ms |
| 1.000 msg/s (120 s) | 119.999 | 119.999 / 119.999 | 599.995 / 599.995 | 0,21 ms | 0,74 ms |

Conclusiones medidas: **empatan en throughput** (47× la tasa del simulador, sin perder un
mensaje); el cuello es el **normalizer**, no el motor de Streams; el coste **no** empata
(Quarkus ~1,7× más CPU, y hasta 3,6× en el normalizer en la ráfaga larga, pero acumula la
mitad de atraso); y la tasa sostenible estimada baja al entorno de **600-800 msg/s por
stack** en este portátil.

**(e) Qué se rompió.**

- **Los nombres de las variables de entorno no coincidían** entre el handler y Terraform
  (`KAFKA_BOOTSTRAP_SERVERS` frente a `BOOTSTRAP_SERVERS`, etc.): compilaba y en AWS
  habría arrancado apuntando a `kafka-1:9092`. Se alineó el handler con el contrato de
  despliegue.
- **Faltaba la referencia de divisas**: la Lambda solo publicaba el canónico y el DLT, así
  que el pipeline desplegado se habría quedado sin tipos de cambio y el join habría
  fallado. Se añadió el tercer productor (`market.fx.reference`) y su test.
- **El jar que se subía era el fino** (121 KB, sin dependencias) en vez del gordo del
  `shade` (`...-shaded.jar`, 34 MB): Lambda habría fallado con `ClassNotFoundException` en
  la primera invocación. Por eso el CI construye el paquete y lo sube como artefacto.
- **`send()` asíncrono y `abortTransaction()`**: el IT de exactly-once falló en CI dos
  veces y el primer diagnóstico ("es una carrera al leer") era **falso**. La secuencia
  "`send` y abortar inmediatamente" falla **8 de 8**; la correcta (esperar a **ver** el
  registro con `read_uncommitted` y solo entonces abortar) acierta **8 de 8**. Ver 7.15.
- **El test de estrés no midió nada la primera vez**, y fue el hallazgo: **13 JVM sobre
  23 GB sin límite de heap** (75.055 GC completos, Tomcat sin contestar, productor colgado
  con **lag 0** y ~18.000 mensajes perdidos en silencio). El arreglo ya estaba en la
  unidad de systemd (`-Xmx320m`); ahora también en los scripts locales. Ver 7.13.

**Preguntas de entrevistador**

1. *¿Qué se puede serverless y qué no?* Solo lo que no tiene estado: en Aggora, el
   normalizador. Los tres de Streams necesitan state store en disco, rebalanceo y
   transacciones abiertas.
2. *¿Por qué no EKS?* Porque el plano de control se cobra aunque no tengas nodos y lo
   único que aporta aquí (siempre encendido + consumidores que escalan a cero) se
   consigue con KEDA sobre una VM o ECS.
3. *¿Qué es lo primero que dolerá al desplegar de verdad?* La falta de cuenta: **no se ha
   desplegado nada**. Lo que sí está validado es Terraform, el compose, la unidad y los
   tests. Y de lo que dolerá: credenciales (`Access denied` de SASL no dice qué falta),
   topics con las mismas particiones y el reloj/certificados que caducan.
4. *¿Por qué el test de estrés no vale `kafka-producer-perf-test`?* Porque manda bytes
   arbitrarios y detrás hay un esquema Avro y un validador: mediría el camino del DLT. Por
   eso el generador usa **el mismo serializador de Confluent** y produce ticks válidos.

---

## 5. Recorrido fichero a fichero

El corazón del curso. Agrupado por servicio y por tipo. Cada fichero con **una o dos
líneas** de qué hace y por qué existe. Cuando el "por qué" no está en un comentario ni en
un doc del repo, se dice **sin comentario** en vez de inventarlo. Los tests van agrupados
por módulo.

### 5.1 Raíces, agregadores y documentos

| Fichero | Qué es y por qué |
|---|---|
| `services/pom.xml` | Agregador de los tres árboles (Spring, Quarkus, Lambda). **No hereda de nadie** a propósito: si heredara de Spring, su `dependencyManagement` decidiría las versiones de `kafka-clients`, Jackson o JUnit en vez del BOM de Quarkus. |
| `services/spring/pom.xml` | Padre de los 8 módulos Spring: `spring-boot-starter-parent` 4.1.1, Java 21, Confluent 8.3.1, Avro 1.12.1, Testcontainers 1.21.3, repo de Confluent y failsafe para los `*IT`. |
| `services/quarkus/pom.xml` | Padre del árbol Quarkus: importa el **BOM 3.39.3** (que decide *todas* las versiones). Trae **BouncyCastle, Brotli, commons-compress y xz** aunque el código no los use: son obligatorios para compilar el nativo. El failsafe va en `<plugins>` y no en `<pluginManagement>`: *pluginManagement solo configura, no activa*, y por eso los `*IT` de Quarkus no se ejecutaban nunca. |
| `services/lambda/pom.xml` | Agregador serverless. No hereda de nadie porque una Lambda no es una aplicación con bucle de consumo. |
| `README.md` | Portada: qué es, cómo arrancarlo, mapa "concepto Kafka → dónde vive". |
| `SPEC.md` | La especificación original, **inmutable**. Útil para ver qué se desvió y por qué. |
| `CONTRIBUTING.md` | Reglas de trabajo y el **estado de cada fase** con sus verificaciones. La fuente más al día junto con `docs/decisions.md`. |
| `SPRING_VS_QUARKUS.md` | Informe comparativo con números y recomendación honesta. |
| `AGENTS.md` | Modo "Ponytail" (senior perezoso): la escalera de 7 peldaños antes de escribir código y la regla de dejar **una comprobación ejecutable** por lógica no trivial. |
| `.gitignore` | Ignora `target/`, `*.class`, `*.log`, `.secrets/`, `*apikey*`, `*.key`. Comentario explícito: "Secretos: NUNCA versionar". |

### 5.2 Los 12 contratos Avro (`services/schemas/`)

Una sola copia para las dos implementaciones: de estos `.avsc` se **generan** las clases
Java con el `avro-maven-plugin` (no se escriben a mano).

| Esquema | Namespace | Campos clave |
|---|---|---|
| `tick.avsc` | `com.aggora.avro` | 10 campos; `price` decimal(18,4), `eventTime` timestamp-millis, `source` REFERENCE/SYNTHETIC. Subject `market.ticks.raw-value`. |
| `canonical.avsc` | `com.aggora.avro.canonical` | 13 campos; añade `normalizedAt` y `originPartition`/`originOffset` (trazabilidad). Namespace distinto para que no choquen los enums homónimos. |
| `order.avsc` | `com.aggora.avro.orders` | 8 campos; `currency` se añadió **con default** (primer cambio COMPATIBLE del proyecto). Solo órdenes limitadas. |
| `execution.avsc` | `com.aggora.avro.orders` | 10 campos; se publica **dentro de una transacción** junto al offset. |
| `alert.avsc` | `com.aggora.avro.alerts` | 8 campos; `type` PRICE_SPIKE/STALE_FEED/MARGIN_BREACH, `severity` INFO/WARNING/CRITICAL. |
| `metrics.avsc` | `com.aggora.avro.analytics` | 11 campos; `windowKind` TUMBLING/HOPPING, VWAP/media/volatilidad en decimal. |
| `metrics-accumulator.avsc` | `com.aggora.avro.analytics` | 7 campos en `double`: es el **estado interno** del state store, no contrato público (puede cambiar sin avisar). |
| `arbitrage-spread.avsc` | `com.aggora.avro.analytics` | 10 campos; todo en USD y `spreadBps` para comparar instrumentos de precios distintos. |
| `portfolio-position.avsc` | `com.aggora.avro.portfolio` | 11 campos; `quantity` long (± = largo/corto), `marginBreach` booleano. Cada update es el estado completo. |
| `position-delta.avsc` | `com.aggora.avro.portfolio` | 7 campos; cada ejecución genera **dos** deltas (comprador +, vendedor −); contrato propio para el topic de repartición. |
| `audit-event.avsc` | `com.aggora.avro.audit` | 9 campos; es un **sobre**: `payload` es JSON en texto y `entityId` es la clave del compactado. |
| `fx-rate.avsc` | `com.aggora.avro.reference` | 3 campos; `market.fx.reference` es **compactado**: solo interesa el último valor por par. |

### 5.3 Implementación Spring (`services/spring/`)

**`market-data-simulator`** — produce ticks reales+sintéticos y órdenes simuladas.

| Fichero | Qué hace · por qué |
|---|---|
| `MarketDataSimulatorApplication.java` | Arranque con `@EnableScheduling`; produce a `market.ticks.raw`. Dos fuentes: `REFERENCE` real y `SYNTHETIC` interpolada. |
| `config/AggoraProperties.java` | Record de `aggora.*` (topics, simulación, proveedores, instrumentos). Los placeholders de `@Scheduled` se leen del `Environment`, no de aquí. |
| `config/KafkaTopicsConfig.java` | Declara `market.ticks.raw` y `orders.incoming` con 6 particiones: versionar las particiones en el repo y que un libro por instrumento quepa en una partición. |
| `domain/Exchange.java` | Horario real y zona de cada mercado; `isOpen()`. Callarse con el mercado cerrado (spec 3.3.5). `ponytail:` sin festivos ni medias sesiones. |
| `pricing/PriceWalk.java` | Paseo gaussiano con reversión a la media y banda dura ±1,5 %. Es la pieza con lógica real, por eso tiene test. |
| `producer/TickProducer.java` | Envío asíncrono con callback y contadores. Async para no limitar el caudal a un *round-trip*; idempotente + `acks=all`. |
| `reference/ReferenceSource.java` | Interfaz de fuente de precios reales: la abstracción se justifica porque hay **dos** proveedores con formato y cuota distintos. |
| `reference/TwelveDataClient.java` | Proveedor principal: agrupa por exchange y traduce `/price` (los dos formatos). Un crédito = un símbolo. |
| `reference/AlphaVantageClient.java` | Proveedor secundario (Euronext, Shanghai), 1,2 s entre llamadas. La cuota agotada llega como **HTTP 200 con `Information`**. |
| `reference/ReferenceFeed.java` | Dos `@Scheduled` con presupuesto diario por proveedor y solo mercados abiertos. Scheduler, no bucle ocupado. |
| `simulation/TickEngine.java` | Un `PriceWalk` por instrumento, ticks sintéticos, `applyReference` y el gancho de tick inválido. |
| `simulation/OrderFlowGenerator.java` | Órdenes simuladas a `orders.incoming` cada segundo, precio ±0,5 %. Vive aquí porque el simulador ya es el generador de tráfico. |
| `resources/application.yml` | Ver 5.7. |
| *tests* | `ExchangeTest` (NYSE/NY, Euronext con cambio de hora, mediodía de Shanghai, fin de semana); `PriceWalkTest` (dentro de banda, reajuste, sin deriva); `AlphaVantageClientTest` (5 casos de formato y cuota); `TwelveDataClientTest` (5 casos, incluido el `meta`). |

**`ingestion-normalizer`** — valida, normaliza y republica.

| Fichero | Qué hace · por qué |
|---|---|
| `IngestionNormalizerApplication.java` | Arranque; consume `market.ticks.raw`. |
| `config/AggoraProperties.java` | Record `aggora`: topics + `processingDelayMs`. Sin comentario. |
| `config/KafkaConsumerConfig.java` | Fábrica propia de contenedores para colgarle el `RebalanceLogger`: un callback no se configura desde YAML. |
| `config/KafkaTopicsConfig.java` | Declara canónico (6), `fx-reference` (1, **compactado**) y `market.ticks.raw.DLT` (1). Cada topic lo declara quien escribe. |
| `consumer/RebalanceLogger.java` | Loguea particiones asignadas, revocadas y perdidas: hace **visible** el reparto con dos instancias. |
| `consumer/TickConsumer.java` | `@KafkaListener` con commit manual; valida (7 reglas), publica canónico + `FxRate`, manda los inválidos al DLT con `x-dlt-reason`. At-least-once. |
| `resources/application.yml` | Ver 5.7. |
| *tests* | `SchemaEvolutionTest` (5 tests: consumidor antiguo/nuevo, default, alias, borrado). |

**`analytics-streams`** — ventanas, joins y consultas interactivas.

| Fichero | Qué hace · por qué |
|---|---|
| `AnalyticsStreamsApplication.java` | `@EnableKafkaStreams`; Streams "a pelo" para comparar justo con Quarkus. |
| `config/AggoraProperties.java` | Record: registry, topics, ventanas, arbitraje. Ventanas cortas en dev para ver resultados. |
| `config/AnalyticsConfig.java` | Engancha las topologías, los `AvroSerdes` y el `StreamsUncaughtExceptionHandler` (`REPLACE_THREAD`). Un servicio no debe decidir suicidarse. |
| `config/AvroSerdes.java` | `SpecificAvroSerde` de canónico, acumulador, métricas, FX y spread. Clase normal (no `@Component`) para poder pasar `mock://` en los tests. |
| `config/KafkaTopicsConfig.java` | Declara `market.analytics` y `market.arbitrage` (6). Los topics internos los crea Streams. |
| `health/StreamsHealth.java` | `HealthIndicator` que baja a **DOWN** si el motor no procesa + gauge `aggora_kafka_streams_running`. Proceso vivo ≠ servicio sano. |
| `query/AnalyticsQueryController.java` | `GET /analytics?symbol=&minutes=`: lee el window store y devuelve 503 si el motor está en ERROR o el store se reconstruye. Símbolo por *query* (la barra de `EUR/USD` parte la ruta) y DTO propio (Jackson revienta con `getSchema()`). |
| `topology/MetricsTopology.java` | Ventana fija (30 s) y móvil (60/15 s) → VWAP, media, volatilidad en dos state stores. Sin `suppress`: una métrica por tick, a propósito. |
| `topology/ArbitrageTopology.java` | Join stream-GlobalKTable con `market.fx.reference` (conversión a USD) + join stream-stream de `ASML`/`ASML.AMS` por símbolo raíz con ventana de 5 s. |
| `resources/application.yml` | Ver 5.7. |
| *tests* | `MetricsTopologyTest` (3), `ArbitrageTopologyTest` (3), con `TopologyTestDriver` y registry `mock://`. |

**`order-matching-engine`** — libro de órdenes con exactly-once.

| Fichero | Qué hace · por qué |
|---|---|
| `OrderMatchingEngineApplication.java` | Arranque; el exactly-once se monta en `KafkaTransactionConfig`. |
| `book/OrderBook.java` | Libro de **un** instrumento con prioridad precio-tiempo; solo órdenes limitadas; estado en memoria (se pierde al reiniciar). |
| `book/OrderBooks.java` | Un libro por símbolo, creado a demanda en un `ConcurrentHashMap`. Concurrente porque la concurrencia del listener puede subir. |
| `config/AggoraProperties.java` | Record: registry, topics, `failEveryNOrders`. Sin comentario. |
| `config/KafkaTopicsConfig.java` | Declara `orders.executions` (6) y `orders.incoming.DLT` (1). Con la autocreación apagada, el publicador del DLT no puede crear el topic. |
| `config/KafkaTransactionConfig.java` | `KafkaTransactionManager` + fábrica transaccional + `DefaultErrorHandler` con DLT (2 reintentos de 200 ms). Publicar y confirmar no son dos pasos. |
| `consumer/OrderMatcher.java` | `@KafkaListener` que cruza y publica; el veneno se decide **por el `orderId`**. Sin ack manual: lo cubre la transacción. |
| `resources/application.yml` | Ver 5.7. |
| *tests* | `OrderBookTest` (4: precio pasivo, parcial, sin cruce, prioridad por tiempo); `ExactlyOnceKafkaIT` (Testcontainers Kafka + Schema Registry; se ejecuta con `mvn verify`). |

**`portfolio-risk`** — posiciones, P&L y margen.

| Fichero | Qué hace · por qué |
|---|---|
| `PortfolioRiskApplication.java` | Arranque; una ejecución son **dos** movimientos, por eso se abre en deltas. |
| `config/AggoraProperties.java` | Record: registry, topics, `marginLimit`. Sin comentario. |
| `config/AvroSerdes.java` | Serdes de `Execution`, `PositionDelta` y `PortfolioPosition`; clase normal para `mock://`. |
| `config/PortfolioConfig.java` | Handler `REPLACE_THREAD`, serdes, topología y topic `portfolio.updates` (6). |
| `topology/PortfolioTopology.java` | `flatMap` (compra +, venta −) → KTable con coste medio, P&L realizado, exposición y `marginBreach`. El re-clavado es obligatorio y solo abrir/aumentar cambia el coste medio. |
| `resources/application.yml` | Ver 5.7. |
| *tests* | `PortfolioTopologyTest` (3: coste medio ponderado, cierre parcial con P&L, vuelta de posición). |

**`alerting-service`** — tres reglas.

| Fichero | Qué hace · por qué |
|---|---|
| `AlertingServiceApplication.java` | Arranque; la tercera regla detecta la **ausencia** de datos. |
| `config/AggoraProperties.java` | Record: registry, topics, umbrales de pico, timeout/intervalo de feed parado. Sin comentario. |
| `config/AlertingConfig.java` | Handler `REPLACE_THREAD`, serdes, topología y topic `alerts.raised` (3). |
| `config/AvroSerdes.java` | Serdes de `SymbolMetrics`, `PortfolioPosition` y `Alert`. |
| `topology/AlertingTopology.java` | Filtra solo la ventana HOPPING, procesa `SpikeDetector` + `StaleFeedDetector` y engancha el `marginBreach` de la cartera. |
| `topology/SpikeDetector.java` | `Processor` con estado `inSpike` y umbral de rearme (histéresis). Un filtro produjo 36.000 alertas por el mismo pico. |
| `topology/StaleFeedDetector.java` | Punctuator `WALL_CLOCK_TIME`; avisa una vez por episodio. Detectar lo que NO llega no se puede reaccionar. |
| `resources/application.yml` | Ver 5.7. |
| *tests* | `AlertingTopologyTest` (3: pico, margen, feed parado con `advanceWallClockTime`). |

**`audit-log`** — transactional outbox.

| Fichero | Qué hace · por qué |
|---|---|
| `AuditLogApplication.java` | Arranque con `@EnableScheduling`; dos escrituras en dos sistemas no son atómicas. |
| `config/AggoraProperties.java` | Record: registry, topics, `relayIntervalMs`, `relayBatchSize`. Sin comentario. |
| `config/KafkaTopicsConfig.java` | `audit.events` **compactado**, 3 particiones. La clave es la entidad: un duplicado es inofensivo. |
| `consumer/AuditConsumer.java` | `@KafkaListener` de tres topics + `@RetryableTopic` (3 intentos, backoff ×2) y `@DltHandler`. Valor como `Object` porque son tres tipos Avro. |
| `relay/OutboxRelay.java` | `@Scheduled` que lee pendientes, publica a `audit.events` y marca `published_at`. La BD es la fuente de verdad. |
| `store/AuditStore.java` | `JdbcTemplate` con `@Transactional`: evento + recado, `on conflict do nothing`, `OffsetDateTime`. El índice único da la idempotencia. |
| `store/OutboxMessage.java` | Record de una fila pendiente. Trivial. |
| `resources/application.yml` | Ver 5.7. |
| `resources/schema.sql` | `audit_events` (índice único por topic/partición/offset) y `audit_outbox` (índice parcial de pendientes). En un proyecto mayor sería Flyway. |
| *tests* | `AuditStoreTest` (3, sin BD: entidad, JSON); `OutboxPostgresIT` (Testcontainers Postgres: idempotencia, eventos distintos). |

**`gateway-ws`** — fan-out a navegadores.

| Fichero | Qué hace · por qué |
|---|---|
| `GatewayWsApplication.java` | Arranque; de Kafka al navegador por WebSocket. Sin comentario. |
| `config/AggoraProperties.java` | Record: registry + topics (canónico, cartera, alertas). Sin comentario. |
| `consumer/LiveFeedConsumer.java` | Tres `@KafkaListener` que traducen Avro a un JSON de pantalla y reparten. Sin commit manual ni transacciones: el siguiente evento corrige. |
| `ws/LiveFeedHandler.java` | `TextWebSocketHandler` con el conjunto de sesiones y `broadcast`. Sesiones en memoria, una sola instancia. |
| `ws/WebSocketConfig.java` | `@EnableWebSocket`, endpoint `/ws`. A pelo, sin STOMP; `allowed-origin "*"` es de desarrollo. |
| `resources/application.yml` | Ver 5.7. |
| `resources/static/index.html` | Página de demo: precios, cartera y alertas por WebSocket. La consulta `ws://host/ws`. |
| *tests* | Ninguno en el módulo. |

### 5.4 Implementación Quarkus (`services/quarkus/`)

En vez de repetir cada fichero, aquí va **qué cambia** y qué se copia tal cual. La
conclusión de la fase está en las tres primeras filas: la API de Kafka Streams **no
cambia**, cambia quién la envuelve.

| Fichero | Qué hace · por qué |
|---|---|
| `ingestion-normalizer/src/main/java/**/TickNormalizer.java` | `@Incoming` + `record.ack()`; publica canónico, `FxRate` y DLT. Mismo comportamiento que Spring; el envío al canónico es asíncrono y no se espera (se deja así para portar igual, y se dice). |
| `.../TickValidator.java` | Las **mismas 7 reglas**, mismo texto y mismo orden que Spring, separadas del consumo para probarlas sin Kafka. |
| `.../RebalanceLog.java` | `KafkaConsumerRebalanceListener` de SmallRye, `@Named("rebalance-log")` para engancharlo por nombre desde `consumer-rebalance-listener.name`. |
| `.../TopicCreator.java` | Crea topics con el `AdminClient` en `@Observes StartupEvent` (en Quarkus no hay autoconfiguración de topics). Sin réplicas explícitas: manda el default del broker. |
| `.../config/ReflexionNativa.java` | `@RegisterForReflection` de las cuatro estrategias de subject de Confluent: en nativo, `Utils.newInstance` falla si no se declaran. |
| `.../META-INF/native-image/.../reflect-config.json` | Las **169 clases** de Confluent registradas para reflexión (generadas desde los jars). |
| `market-data-simulator/**/AggoraConfig.java` | `@ConfigMapping(prefix="aggora")` (interfaz, no record: Quarkus la implementa en el build). `apiKey` y `feedExchange` son `Optional` porque una propiedad vacía **no está**, y un `String` obligatorio vacío rompe el arranque. |
| `.../config/MetricsTags.java` | `MeterFilter` con `stack=quarkus`, `service=...`: Quarkus no tiene una propiedad global de etiquetas como Spring. |
| `.../reference/TwelveDataApi.java`, `AlphaVantageApi.java` | Clientes REST **declarativos** (`@RegisterRestClient`): la diferencia de filosofía más grande del port. |
| `.../reference/TwelveDataClient.java`, `AlphaVantageClient.java` | Misma traducción de respuestas, con **Jackson 2** (`asText`/`fields`) en vez del Jackson 3 de Spring (`asString`/`properties`). |
| `.../simulation/TickEngine.java` | El scheduler de Quarkus **no baja de 1 s**, así que emite `1000/tick-interval-ms` ticks por vuelta (mismo ritmo, en ráfagas de 5). `skipExecutionIf` evita el `SRMSG00019`. |
| `analytics-streams/**/config/TopologyProducer.java` | `@Produces Topology`: **toda** la diferencia de montaje con `@EnableKafkaStreams`. |
| `analytics-streams/**/topology/*.java` | `MetricsTopology` y `ArbitrageTopology` **copiadas tal cual** (solo cambia el tipo de la configuración). |
| `analytics-streams/**/query/AnalyticsResource.java` | Recurso JAX-RS con la misma lógica de 503 y el mismo JSON (fechas ISO) que Spring. |
| `portfolio-risk/**/topology/PortfolioTopology.java` | Copiada tal cual (180 líneas de aritmética de cartera), con `isolation.level=read_committed`. |
| `order-matching-engine/**/consumer/OrderMatcher.java` | El exactly-once se pide **en el código** (`@Channel("executions") KafkaTransactions<Execution>` + `withTransactionAndAck`), al revés que en Spring. |
| `order-matching-engine/**/book/*.java` | `OrderBook` y `OrderBooks` copiados tal cual. |
| `alerting-service/**/topology/*.java` | Los tres ficheros de topología copiados enteros, **incluido el punctuator**. |
| `audit-log/**/store/AuditStore.java` | JDBC a pelo sobre el `DataSource` de Agroal y `@Transactional` de Jakarta (Narayana), con el **mismo `schema.sql`**. |
| `audit-log/**/config/SchemaInit.java` | Quarkus no ejecuta el schema solo: se lee el mismo `schema.sql` y se ejecuta al arrancar (idempotente). |
| `audit-log/**/consumer/AuditConsumer.java` | **Tres canales tipados** en vez de un `@KafkaListener` con `Object`. |
| `audit-log/**/relay/OutboxRelay.java` | `@Scheduled(every=...)` + `SKIP` + `ApplicationNotRunning`; el contador de pendientes se lee **antes** de publicar. |
| `gateway-ws/**/LiveFeedBroadcaster.java` | Tres `@Incoming` que traducen a JSON y reparten; `failure-strategy=ignore`. |
| `gateway-ws/**/LiveFeedSocket.java` | WebSockets Next (`@WebSocket`), donde `sendText` ya devuelve `Uni<Void>`. |
| `gateway-ws/**/META-INF/resources/index.html` | El **mismo** `index.html` copiado desde Spring. |
| `*/src/main/resources/application.properties` | Las propiedades **planas** (canales, `kafka-streams.*`, REST client, `quarkus.avro.trusted-packages`). En YAML salen entre comillas y Quarkus no las reconoce. |
| `*/src/main/resources/application.yaml` | El bloque `aggora:` **copiado entero** de Spring, para no comparar dos cosas a la vez. |
| `*/pom.xml` | `quarkus-confluent-registry-avro` (Quarkus **falla el build** si detecta clases Avro de Confluent sin su extensión), failsafe en `<plugins>` y `SERIALIZABLE_PACKAGES` en surefire para los tests que no levantan Quarkus. |

### 5.5 La tercera implementación (`services/lambda/`)

| Fichero | Qué hace · por qué |
|---|---|
| `ingestion-normalizer-lambda/pom.xml` | El artefacto de despliegue es el jar **gordo** `...-shaded.jar`. `maven-shade-plugin` con classifier `shaded`, excluyendo firmas `META-INF/*.SF|DSA|RSA` y `module-info` (en un jar gordo la JVM lo rechaza) y fusionando `META-INF/services`. |
| `.../AggoraNormalizerHandler.java` | `RequestHandler<KafkaEvent,String>`: recorre el lote, decodifica **base64**, deserializa Avro, valida y publica canónico/FX/DLT. **No hay bucle de consumo** (lo hace el *event source mapping*); un veneno va al DLT **sin lanzar** (lanzar haría reintentar el lote entero); solo se propaga el fallo de infraestructura. El `TickSink` es una interfaz inyectada por constructor: **inversión de dependencias** para poder probarlo sin Kafka. |
| `.../AggoraNormalizerHandlerTest.java` | 5 tests sin broker ni Docker: válido al canónico, FX al compactado, inválido al DLT, base64/Avro ilegible al DLT, lote mixto. |

### 5.6 Scripts (`scripts/`)

| Fichero | Qué hace · decisiones |
|---|---|
| `start-services.sh` | Arranca los 7 Spring **en orden y esperando** a que cada uno confirme arranque (`esperar_listo`: 45 intentos de 1 s buscando `Started XApplication`). Es la respuesta a la carrera de `MissingSourceTopicException`. `SPRING_JAVA_OPTS` por defecto `-Xms128m -Xmx384m`. |
| `stop-services.sh` | `pkill -TERM` (no `-9`) para que los consumidores se despidan del grupo y no haya que esperar al *timeout* de sesión. No para la infraestructura: ahí vive el estado. |
| `start-quarkus-stack.sh` | Levanta el stack Quarkus **en paralelo** al de Spring: grupo `-q`, topics `.q`, `application-id` y `state.dir` propios. Pasa todo como `-D` y no como variables de entorno porque los nombres de canal llevan guiones. `QUARKUS_JAVA_OPTS` por defecto `-Xms128m -Xmx320m`. |
| `stop-quarkus-stack.sh` | Para los 6 servicios Quarkus con `pkill -TERM`. |
| `measure-service.sh` | Mide arranque (contexto y **proceso hasta listo**), RSS de `ps`, hilos de `nlwp` y descriptores de `/proc/<pid>/fd`. El RSS es la cifra que existe también en un binario nativo. |
| `build-native.sh` | Compila un servicio Quarkus a binario con `-Dquarkus.package.type=native` (no `-Dnative`: sin perfil `native` en el pom **no activa nada**), `container-build=true` y sin fijar el tag del builder. |
| `schema-evolution-lab.sh` | Ver 5.6 y sección 8. `trap` de limpieza y comprobación de que el subject queda como estaba. |
| `throughput-test.sh` | Test de estrés: para el simulador, lanza el generador, mide offsets de salida, **pico de lag por grupo** y **CPU por proceso desde `/proc`**, y marca el resultado como **NO VÁLIDO** si el DLT crece o los canónicos no cuadran. Dos bugs de medición corregidos y documentados (el envoltorio `bash -lc` duplicaba filas; la variable del `awk` se pisaba). |
| `AvroLoadGenerator.java` | Generador de ticks **válidos** con el serializador de Confluent (porque `kafka-producer-perf-test` mandaría bytes al DLT y mediría el descarte). |
| `ExactlyOnceRaceCheck.java` | Mide las dos secuencias de transacción contra el cluster real (sin Testcontainers). Sale con código 1 si la secuencia nueva falla. |
| `GatewayLiveCheck.java` | Habla WebSocket con la librería del JDK (`java.net.http.WebSocket`), sin dependencias, y falla (código 1) si no llega ningún tick. |

### 5.7 Los bloques raros de configuración, explicados

Los `application.yml`/`application.properties` tienen comentarios que son decisiones.
Estos son los que más valen:

| Bloque | Qué decisión hay detrás |
|---|---|
| `KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"` (`infra/docker-compose.yml`) | Un error tipográfico en un nombre de topic debe **fallar**, no crear un topic fantasma de 1 partición. Pasó, y está en `docs/decisions.md`. |
| `KAFKA_DEFAULT_REPLICATION_FACTOR: 3` + topics declarados sin réplicas explícitas | El código deja de fijar `replicas(1)`: manda el default del broker, así el mismo código sirve para uno o para tres. |
| `KAFKA_MIN_INSYNC_REPLICAS: 2` + `acks=all` | Convierte "tengo copias" en "no pierdo datos": con dos brokers caídos el productor **deja de escribir** en vez de arriesgar. |
| `KAFKA_AUTO_CREATE_TOPICS_ENABLE` + `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3`, `TRANSACTION_STATE_LOG_*` | Los topics internos de offsets y transacciones también tienen que sobrevivir a una caída. |
| `listener.ack-mode: manual_immediate` | El offset lo confirma el código, no el framework. Es la mitad de at-least-once. |
| `missing-topics-fatal: false` | Si el normalizador arranca antes que el simulador, el topic aún no existe: avisa y sigue en vez de morir. |
| `spring.kafka.consumer.properties.max.poll.interval.ms: 300000` | Cuánto puede estar el proceso sin llamar a `poll()` antes de que el broker lo expulse del grupo (y dispare un rebalanceo). |
| `producer.properties.enable.idempotence: true` | Un reintento por fallo transitorio no duplica el mensaje. |
| `transaction-id-prefix` + `isolation.level: read_committed` | Las dos piezas del exactly-once que se ven en configuración (la tercera es el gestor en el contenedor). |
| `statestore.cache.max.bytes: 0` | Sin caché: cada tick emite una métrica actualizada y **se ve la ventana llenarse**. Con caché hay menos escrituras pero resultados a plazos. |
| `replication.factor: 3` en `kafka-streams.properties` | El estado de las ventanas (changelog) también tiene que sobrevivir a un broker caído. |
| `server.port: 8085` **en la raíz** del YAML | El 8080 lo ocupa el simulador; dentro de `spring:` se ignora y Tomcat se va al 8080 (pasó). |
| `management.metrics.tags.stack: spring` / `MeterFilter` en Quarkus | Las etiquetas van en la **aplicación**, no en el scrape de Prometheus: si se ponen en el scrape, Prometheus renombra las de dentro a `exported_stack` y se lía. |
| `management.endpoint.health.show-details: always` | Cuando el motor está DOWN, lo primero es preguntarle por qué: `estado: ERROR` ahorra media hora. En producción iría `never` o detrás de autenticación. |
| `quarkus.kafka-streams.topics=...` | La extensión **espera a que existan** los topics de origen antes de arrancar: es la carrera de la Fase 6 resuelta por el framework. |
| `quarkus.avro.trusted-packages=com.aggora.avro` | Avro 1.12.2 enciende el `ClassSecurityValidator`; los serdes de Streams resuelven la clase desde el esquema y sin esto revientan. |
| `quarkus.config.mapping.validate-unknown=false` | Hay propiedades del prefijo `aggora` que no son miembros del `@ConfigMapping` (las leen las anotaciones `@Scheduled`) y Quarkus **falla la validación** por defecto. |
| `kafka-streams.state.dir` distinto en Quarkus (`/tmp/aggora-streams-state-quarkus`) | Para no mezclar el estado de las dos implementaciones mientras se comparan. |
| `deploy/systemd/aggora@.service`: `Environment="JAVA_OPTS=..."` con comillas y `$JAVA_OPTS` sin llaves | systemd parte por espacios: sin comillas solo asigna `-Xms128m`, y con `${JAVA_OPTS}` la JVM recibe un único argumento y no arranca. `systemd-analyze verify` cazó el primero. |
| `Restart=always` + `RestartSec=15` | Sustituye al script de arranque con esperas: el que arranque antes de que exista su topic falla y a los 15 s ya existe. |

### 5.8 Infraestructura (`infra/`)

| Fichero | Qué hace · decisiones |
|---|---|
| `docker-compose.yml` | 3 brokers KRaft + Schema Registry + Postgres + kafka-exporter + Prometheus + Grafana, red `aggora-net` externa, listeners INTERNAL/EXTERNAL y `restart: unless-stopped` en todos. Ver 5.7 para las variables. |
| `prometheus/prometheus.yml` | Scrape cada 10 s: exporter en `:9308`, apps Spring en `host.docker.internal:8080/8085/8089` con `/actuator/prometheus`, apps Quarkus en `:8185/:8189` con `/q/metrics` (jobs separados: no se mezclan rutas). |
| `grafana/provisioning/datasources/prometheus.yml` | Datasource `Prometheus` → `http://prometheus:9090`, `uid: prometheus`. No hay que añadirla a mano. |
| `grafana/provisioning/dashboards/dashboards.yml` | Provider de ficheros que lee `/etc/grafana/dashboards`: el panel está versionado en el repo. |
| `grafana/dashboards/aggora-kafka.json` | Panel "Aggora — Kafka" (`uid aggora-kafka`): lag por grupo y por topic, throughput, **DLT**, brokers vivos, `aggora_kafka_streams_running` (el que delata el fallo silencioso), arranque, hilos, memoria y CPU. |

### 5.9 Despliegue (`deploy/`)

| Fichero | Qué hace · decisiones |
|---|---|
| `README.md` | Runbook (inglés): reparto broker gestionado / VM / Lambda, topics a mano, SASL, costes estimados, pasos de verificación y "What NOT to do". |
| `user-data.sh` | Arranque de la VM (AL2023 ARM64): instala Java 21, Docker y el plugin de compose; lee secretos de SSM; escribe `common.env` y un `.env` por servicio; arranca systemd y el compose. El state store va a **disco**, no a `/tmp` (que puede ser tmpfs). |
| `systemd/aggora@.service` | **Una** unidad plantilla para los 7 servicios (`%i` resuelve el jar y el env). Ver 5.7. |
| `docker-compose.vm.yml` | Postgres + observabilidad en la VM (sin Kafka ni SR: gestionados). Puertos solo en `127.0.0.1` y `extra_hosts: host.docker.internal:host-gateway` para que Prometheus vea las apps de systemd. |
| `collect-artifacts.sh` | Compila y copia los 7 jars Spring a `deploy/artifacts/` con el nombre exacto que espera `user-data.sh`. Detecta el devcontainer por imagen. |
| `terraform/main.tf` | S3 de artefactos, secretos en SSM, VM con systemd (sin ingress), Lambda con *event source mapping*, SQS *on-failure* y tres alarmas. **No** crea el broker ni los topics: eso se hace a mano. |
| `terraform/variables.tf` | Región, tipo de instancia (`t4g.small`), mecanismo SASL (validado a PLAIN/SCRAM), topics de la Lambda, rutas. Los secretos son `sensitive` y llegan por `TF_VAR_`. |
| `terraform/outputs.tf` | IP, comando de sesión SSM, túnel al gateway, nombre de la Lambda, cola SQS y bucket. |
| `terraform/.gitignore` | El **state es un secreto** (lleva credenciales): fuera de git. |
| `artifacts/.gitignore` | Ignora todo salvo el propio `.gitignore`, para que Terraform vea la carpeta sin arrastrar los `.jar`. |

### 5.10 CI y devcontainer

| Fichero | Qué hace · decisiones |
|---|---|
| `.github/workflows/ci.yml` | Cuatro jobs: `unit` (`mvn test` de los tres árboles, sin contenedores), `artifacts` (construye el jar gordo de Lambda y lo sube: un shade mal configurado no se detecta hasta el despliegue), `infra` (`terraform fmt -check` + `init -backend=false` + `validate`) e `integration` (`mvn verify` de los tres módulos con `*IT`). **Corre en push, PR y manual**: condicionarlo a los PR lo dejaba "skipped" siempre en un repo que empuja a `main`. |
| `.devcontainer/devcontainer.json` | Java 21 + Maven + Docker-outside-of-docker (socket y CLI). `forwardPorts: [8089, 8189]` **declarados** (no auto-descubrimiento) para que Prometheus los alcance por `host.docker.internal`. Segundo montaje del repo en la ruta del host para el compilador nativo. |

---

## 6. SOLID y patrones, con ejemplos reales

### 6.1 Los principios SOLID, fichero a fichero

| Principio | Dónde se ve | Por qué es ese y no otro |
|---|---|---|
| **S — responsabilidad única** | Cada servicio hace **una** cosa (sección 1.2). Dentro: `TickValidator.java` solo valida; `AuditStore.java` solo persiste; `OrderBook.java` solo cruza órdenes; `OutboxRelay.java` solo publica lo pendiente. | Es lo que permite probar el validador **sin Kafka** y la aritmética de cartera **sin broker**: la lógica no trivial está separada de la fontanería. |
| **O — abierto/cerrado** | Las reglas de alerta: `SpikeDetector` y `StaleFeedDetector` son `ProcessorSupplier` independientes y `AlertingTopology` los **fusiona**. Añadir una regla nueva es añadir una clase y un `.merge()`, sin tocar las existentes. En el simulador, `ReferenceSource` permite añadir un proveedor sin tocar `ReferenceFeed`. | Límite honesto: añadir un **tipo** de alerta sí obliga a tocar el enum `AlertType` del esquema Avro. Abierto/cerrado no significa "no tocar nunca nada". |
| **L — sustitución de Liskov** | Las dos implementaciones de `ReferenceSource` son intercambiables: el feed no sabe cuál tiene delante. Y los dos stacks (Spring y Quarkus) son intercambiables **en el topic**: se para uno y arranca el otro con el mismo `group.id` y el pipeline ni se entera (verificado en vivo). | Es el principio llevado al nivel de despliegue, no de clase. |
| **I — segregación de interfaces** | Los emisores están **segregados por tipo**: en Spring, `TickConsumer` recibe tres `KafkaTemplate` distintos (`CanonicalTick`, `FxRate`, `Tick` para el DLT) y en Quarkus hay un `Emitter` por canal. Ninguna clase depende de un emisor que no usa. | Se evita el "emisor genérico de `Object`", que compila igual y falla en ejecución. |
| **D — inversión de dependencias** | `AggoraNormalizerHandler` (Lambda) depende de la interfaz **`TickSink`**, no de Kafka: el constructor sin argumentos monta el sink real y el constructor de paquete inyecta uno de mentira para los tests. Todo por **inyección por constructor**. | Es lo que hace que los 5 tests de la Lambda corran sin broker, sin Docker y en milisegundos. |

### 6.2 Patrones de arquitectura que sí se usan

| Patrón | Dónde | Qué problema resuelve aquí |
|---|---|---|
| **Arquitectura orientada a eventos / pub-sub** | Todo el proyecto: los servicios no se llaman entre sí, leen y escriben topics. | Desacopla emisor y receptor: el simulador no sabe quién le lee, y añadir un consumidor nuevo (el gateway) no toca a nadie. |
| **Transactional outbox** | `services/*/audit-log` (`AuditStore` + `OutboxRelay` + `schema.sql`). | Guardar en Postgres y publicar en Kafka son dos sistemas: no se pueden hacer "a la vez". El recado en la misma transacción sí. |
| **Dead letter topic + reintentos** | `.DLT` en el normalizador y el motor; `@RetryableTopic` en la auditoría. | Un mensaje venenoso no puede bloquear su partición para siempre; uno transitorio no debe perderse. |
| **Retry topics** | `@RetryableTopic` (auditoría): `.retry-*` con espera creciente y backoff ×2. | Fallos que tardan en resolverse (Postgres caído un rato) sin bloquear la partición y sin descartar. |
| **Health probe + supervisor** | `StreamsHealth` (Spring) y la sonda de la extensión (Quarkus); `Restart=always` en systemd, `restart: unless-stopped` en Docker. | Convierte un fallo silencioso (motor en ERROR, proceso vivo) en un reinicio de verdad. |
| **Fan-out por grupo de consumo** | `gateway-ws` con `group.id` propio. | Ver todos los mensajes en la pantalla sin quitarle particiones a la analítica. |
| **Port and adapter (mismo contrato, dos runtimes)** | `services/schemas/*.avsc` compartidos; Spring y Quarkus generan sus clases de los mismos esquemas. | El mismo contrato Avro con dos implementaciones: los mensajes son intercambiables byte a byte. |
| **"Mismo contrato, dos implementaciones" como experimento** | `SPRING_VS_QUARKUS.md`, `scripts/start-quarkus-stack.sh`. | Comparar frameworks con la misma entrada, los mismos topics y las mismas versiones, midiendo en vez de opinar. |
| **CQRS-lite con consultas interactivas** | `GET /analytics` lee el state store en marcha. | Separar el camino de escritura (el stream) del de lectura (la consulta), sin montar una base de datos aparte. |
| **Idempotencia por clave única** | Índice único `(source_topic, source_partition, source_offset)` en `schema.sql`. | Con at-least-once, la reentrega no puede duplicar la auditoría: lo garantiza la **tabla**, no el código. |

### 6.3 Patrones que **no** se usan (y por qué es igual de didáctico)

| Patrón | Por qué no | Qué se hace en su lugar |
|---|---|---|
| **Saga** (transacción distribuida con compensaciones) | Aquí no hay una operación de negocio que abarque varios servicios y haya que deshacer. El único flujo con dos sistemas (auditoría) se resuelve con **outbox**, que es más simple y no necesita compensar nada. | Transactional outbox + idempotencia. |
| **Circuit breaker** | Los clientes de Kafka ya reintentan con buffer y `delivery.timeout.ms`; y el proyecto no llama a servicios externos en caliente salvo los proveedores de precios, que se consultan cada 15 min o cada 3 h. | Timeouts y reintentos del propio cliente; `on-failure destination` en Lambda. |
| **CQRS completo** (modelo de lectura separado, proyecciones, event sourcing) | Sería otro sistema que mantener y el objetivo es Kafka. Las consultas interactivas dan la parte útil ("pregúntale al estado en marcha") sin el resto. | CQRS-lite con state store. |
| **EKS / Kubernetes** | El plano de control se cobra por hora aunque no tengas nodos y lo único que aporta aquí (siempre encendido + escalar a cero) se consigue con KEDA sobre una VM o ECS. | Una VM con systemd y una Lambda. |

**El patrón que se usa "al revés" a propósito**: el commit **automático** en `gateway-ws`.
En los servicios de trabajo el commit es manual porque un mensaje perdido es un dato
perdido; en un fan-out, el siguiente evento trae el dato nuevo y la pantalla se corrige
sola. El mismo mecanismo, configurado al revés según lo que procesa.

---

## 7. Los problemas que enfrentamos

Contados en segunda persona y en el orden en que aparecieron. Cada uno con **síntoma →
diagnóstico → causa raíz → arreglo → lección**. Todos son reales y están en
`docs/decisions.md` o en `CONTRIBUTING.md`.

### 7.1 `MissingSourceTopicException` al arrancar Streams

- **Síntoma.** Arrancas los siete servicios y la analítica (o las alertas) **se cae** al arrancar: `MissingSourceTopicException`.
- **Diagnóstico.** Miras el log y falta el topic de origen. Compruebas con `kafka-topics.sh --list` que no existe.
- **Causa raíz.** Kafka Streams **falla si su topic de origen no existe todavía**, y los topics los crea **quien escribe** en ellos. Si arrancas el consumidor antes que el productor, no hay topic.
- **Arreglo.** `scripts/start-services.sh` arranca **en orden y esperando** a que cada servicio confirme arranque (`Started XApplication`) antes de lanzar el siguiente. En Quarkus lo resuelve `quarkus.kafka-streams.topics`; en la VM, `Restart=always` + `RestartSec=15`.
- **Lección.** El orden de arranque es parte del diseño cuando el topic lo crea el productor.

### 7.2 La compactación adelanta el log: `OffsetOutOfRangeException` en una GlobalKTable

- **Síntoma.** El hilo **global** muere con `OffsetOutOfRangeException`; el servicio sigue vivo pero no calcula el arbitraje.
- **Diagnóstico.** El checkpoint local del state store global apunta a offsets que ya no existen en el topic.
- **Causa raíz.** El topic `market.fx.reference` es **compactado** y la compactación **avanza el principio del log**, por delante del checkpoint de Streams.
- **Arreglo.** Reiniciar la aplicación (Streams limpia el estado local y relee desde el principio disponible). Con el manejador de excepciones, el hilo se sustituye y el estado global se relee.
- **Lección.** Un topic compactado **no es un histórico fiable**: es el último estado por clave. El checkpoint de una GlobalKTable sobre un compactado puede quedar colgado en offsets que ya no existen.

### 7.3 El fallo silencioso de Streams (y `REPLACE_THREAD` / `SHUTDOWN_APPLICATION`)

- **Síntoma.** El endpoint HTTP responde y el servicio parece sano, pero **no procesa nada**. `/analytics` devuelve 503 y no hay errores visibles.
- **Diagnóstico.** `streams.state()` está en **ERROR** y `aggora_kafka_streams_running` vale 0.
- **Causa raíz.** Ante un error no recuperable, Kafka Streams **para el cliente** pero **el proceso Java sigue vivo**.
- **Arreglo (y los dos intentos fallidos).** `REPLACE_THREAD` **no arregla** un estado global inconsistente: el cliente se para igual. `SHUTDOWN_APPLICATION` es **peor**: dentro de Spring el cierre se enreda, el consumidor entra en un bucle `Request joining group due to: Shutdown requested` que escribió **429 MB de log en 28 segundos**, y el proceso **tampoco muere**. La respuesta buena es la **sonda de salud**: `/actuator/health` a DOWN, `aggora_kafka_streams_running` a 0 y que reinicie el **supervisor**.
- **Lección.** Un servicio no debería decidir suicidarse. Quien levanta un proceso caído es el supervisor; quien le dice que está roto, la sonda. Verificado rompiendo el checkpoint a propósito.

### 7.4 El veneno decidido por contador nunca llega al DLT

- **Síntoma.** Inyectas el fallo "cada 20 órdenes" y el mensaje **nunca** aparece en `orders.incoming.DLT`.
- **Diagnóstico.** Al reintentar, el contador avanza, la condición de fallo deja de cumplirse y el mensaje se procesa bien.
- **Causa raíz.** Un **DLT es para mensajes venenosos** (el fallo pertenece al mensaje), no para fallos de un momento. Un contador describe el momento, no el mensaje.
- **Arreglo.** El fallo se decide por el **`orderId`**: o esa orden es venenosa siempre, o no lo es nunca. Verificado: 24 órdenes venenosas descartadas **sin bloquear la partición**.
- **Lección.** Si el criterio de fallo no es una propiedad del mensaje, el reintento lo hace desaparecer.

### 7.5 500 en vez de 503 mientras se reconstruye el state store

- **Síntoma.** En cada arranque, `/analytics` devuelve **500** durante unos instantes.
- **Diagnóstico.** `InvalidStateStoreException: the stream thread is STARTING, not RUNNING`. El `try` envolvía solo el momento de **abrir** el store; abrirlo funcionaba y el fallo llegaba después, en el `fetch`.
- **Causa raíz.** Estado transitorio tratado como error de servidor. Un 500 hace que un balanceador saque la instancia de rotación como si estuviera rota.
- **Arreglo.** El `try` envuelve la consulta **entera** y se responde **503 con el motivo**. Verificado muestreando el endpoint cada medio segundo en un arranque real: 503, 503, 200 y **ningún 500**.
- **Lección.** "Todavía no estoy listo" (503) no es "estoy roto" (500).

### 7.6 El fichero suelto montado en Docker

- **Síntoma.** Editas `prometheus.yml`, recreas el contenedor y Docker Desktop falla con "no such file or directory".
- **Diagnóstico.** El montaje apuntaba al **inodo viejo**: al reemplazar el fichero, el punto de montaje queda atado al original.
- **Causa raíz.** Montar **un fichero** en vez de una carpeta.
- **Arreglo.** `infra/docker-compose.yml` monta la **carpeta** `./prometheus:/etc/prometheus:ro`. Con la carpeta montada, editar el fichero no rompe nada.
- **Lección.** En Docker, monta directorios para configuración que vas a editar.

### 7.7 Los errores de SmallRye, la configuración estricta y las claves con puntos

- **Síntoma.** `SRMSG00019: Unable to connect an emitter with the channel ticks-raw` en la primera ejecución del job; un `String` obligatorio con valor vacío **no arranca** (`defined as the empty String which the Converter considered to be null`); las claves con puntos puestas en YAML salen entre comillas y Quarkus no las reconoce (`Unrecognized configuration key`).
- **Diagnóstico.** Se lee el mensaje: el job disparaba antes de que los canales estuvieran conectados; la api key llega como cadena vacía; el parser del YAML entrecomilla.
- **Causa raíz.** El scheduler de Quarkus arranca antes que los canales; en SmallRye una propiedad vacía **no está**; el YAML no es el sitio para claves planas con puntos.
- **Arreglo.** `skipExecutionIf = Scheduled.ApplicationNotRunning.class`; `Optional<String>` + `filter(...).isPresent()` (y ojo: `@WithDefault("")` **no** vale, mete una cadena vacía que el conversor también rechaza); las propiedades planas a `application.properties`.
- **Lección.** Quarkus **falla al arrancar** en vez de tragar: es mejor a la larga, pero se paga al portar. (El `SRMSG00051` que menciona la consigna **no está documentado en el repo**; el que se documenta es el `00019`.)

### 7.8 El suelo de 1 segundo del scheduler de Quarkus

- **Síntoma.** `An every() value less than 1000 ms is not supported`. Spring admitía `fixedRate = 200ms`.
- **Diagnóstico.** El scheduler simple de Quarkus no baja de 1 s.
- **Causa raíz.** Limitación de la anotación `@Scheduled(every=...)`.
- **Arreglo.** Se programa **una vuelta por segundo** y en cada vuelta se emiten `1000 / tick-interval-ms` ticks (5). El ritmo es el mismo (medido: 44 msg/s, como Spring) pero llega **en ráfagas de 5**, y eso cambia un poco lo que ven las ventanas de la analítica. Queda dicho.
- **Lección.** Un port fiel en el caudal puede no serlo en la **forma** de ese caudal, y eso importa a quien consume ventanas.

### 7.9 Avro 1.12.2 y el validador de clases

- **Síntoma.** `SecurityException: Forbidden com.aggora.avro.canonical.CanonicalTick! This class is not trusted to be included in Avro schemas`.
- **Diagnóstico.** Avro **1.12.2 enciende el `ClassSecurityValidator`**; el BOM de Quarkus trae 1.12.2 y la implementación Spring usa **1.12.1**, donde venía apagado.
- **Causa raíz.** Los serdes de Kafka Streams resuelven la clase **a partir del esquema** (`ClassUtils.forName`) y ahí está la validación. El productor Avro normal no lo pisa porque ya tiene el objeto.
- **Arreglo.** En la aplicación, `quarkus.avro.trusted-packages=com.aggora.avro`; en los tests de topología (que no levantan Quarkus), `org.apache.avro.SERIALIZABLE_PACKAGES=com.aggora.avro` en el surefire.
- **Lección.** No era un problema de frameworks, era de **dependencias**. Queda escrito que a Spring le pasará el día que suba Avro a 1.12.2+.

### 7.10 El nativo de GraalVM: cuatro tropiezos y una lección

- **Síntoma.** Primero `Discovering unresolved type ... org.bouncycastle.jsse.BCSSLSocket`; después `NoClassDefFoundError: org/brotli/dec/BrotliInputStream`; después commons-compress + xz; por último `NoSuchMethodException ... Utils.newInstance` al arrancar el binario.
- **Diagnóstico.** Los tres primeros son **dependencias opcionales** que el análisis estático de GraalVM convierte en obligatorias (TLS, compresión, API nueva de compresión). El cuarto es **reflexión**: la estrategia de nombre de subject se construye por nombre.
- **Causa raíz.** En una imagen nativa **no existe lo "opcional en tiempo de ejecución"**.
- **Arreglo.** Añadir BouncyCastle, Brotli y commons-compress+xz al classpath; y en vez de arreglar la reflexión clase a clase (cada error cuesta **una compilación de cuatro minutos**), **generar el fichero de reflexión desde los propios jars**: **169 clases** a `META-INF/native-image/.../reflect-config.json`. Con eso, el segundo binario salió **a la primera**.
- **Lección.** El número final (0,022 s, 114-124 MB) es bonito; lo que se aprende es el método: cuando el error se repite con la misma forma, deja de arreglarse a mano y genera la lista.

### 7.11 `-Dnative` decía BUILD SUCCESS y no generaba binario

- **Síntoma.** `mvn package -Dnative` termina en **BUILD SUCCESS** y en `target/` no hay ningún `-runner`.
- **Diagnóstico.** Los poms escritos a mano **no tienen el perfil `native`**, así que `-Dnative` no activa nada.
- **Causa raíz.** Un "éxito" que no produce nada es **peor que un error**.
- **Arreglo.** La propiedad que manda es `-Dquarkus.package.type=native` (está en `scripts/build-native.sh`).
- **Lección.** Comprueba el **artefacto**, no el código de salida.

### 7.12 Lag 0 con el productor muerto (~18.000 mensajes perdidos en silencio)

- **Síntoma.** En el test de estrés, el normalizer de Spring deja de publicar: el crudo avanza **+354** y su canónico **+0**. El contador del log se queda clavado. Y lo peor: su grupo sigue **Stable y con lag 0**.
- **Diagnóstico.** `jstat` del proceso de analítica: **75.055 recolecciones completas y 193 s de GC**. El `curl` al 8085 se queda colgado.
- **Causa raíz.** **13 JVM sobre 23 GB sin límite de heap**: cada JVM coge por defecto un cuarto de la RAM, el conjunto pide varias veces la memoria que hay y el GC se pone a dar vueltas. Los procesos se quedan colgados **sin decir nada** (con heap pequeño no hay `OutOfMemory`: solo sufrimiento). Y el código solo avisa cuando el envío **falla**; cuando se queda colgado para siempre, no avisa de nada.
- **Arreglo.** El arreglo ya estaba en el proyecto, en la unidad de systemd de la Fase 10 (`-Xmx320m`); los scripts locales no lo tenían. Ahora sí: `SPRING_JAVA_OPTS=-Xms128m -Xmx384m` y `QUARKUS_JAVA_OPTS=-Xms128m -Xmx320m`. Los mismos 1.500 msg/s que colgaban el pipeline pasan **sin perder un mensaje**.
- **Lección.** **Lag 0 con un productor muerto** es la trampa más cara del laboratorio: los offsets avanzan y no se publica nada. El lag es necesario, pero **no suficiente** para decir "está sano".

### 7.13 `send()` asíncrono y `abortTransaction()` que descarta lo no enviado

- **Síntoma.** El IT de exactly-once falla en CI **dos veces** con el mismo síntoma: `read_uncommitted` solo ve `["ASML"]` y no la abortada. El primer arreglo (leer esperando los dos registros) fue un **parche mal diagnosticado** y volvió a fallar.
- **Diagnóstico.** Reproducido contra el cluster local con `scripts/ExactlyOnceRaceCheck.java`: la secuencia "`send` y abortar inmediatamente" **falla 8 de 8**. No era inestable: era **determinista al fallar**.
- **Causa raíz.** `send()` es **asíncrono**: el registro se queda en un búfer y lo manda otro hilo. `abortTransaction()` **descarta lo que ese hilo todavía no ha mandado**, así que el registro abortado **nunca llegó a existir**.
- **Arreglo.** Tras el `send`, esperar a **ver** el registro con `read_uncommitted` (que no filtra por transacción) y **solo entonces** abortar. Resultado: **8 de 8 correctos** (`read_committed` = `[ASML]`, `read_uncommitted` = `[ASML, AAPL]`). El test además gana una aserción que antes no tenía: si la abortada no llega al log, falla diciendo eso.
- **Lección.** Cuando un test falla "a veces", lo primero no es darle más tiempo ni más reintentos: es preguntarse si lo que el test **supone** que ha pasado ha pasado de verdad. **Un test que da por hecho el estado que dice comprobar es peor que no tenerlo.**

### 7.14 Testcontainers y Ryuk en el devcontainer

- **Síntoma.** Los `*IT` de Spring fallan con `Could not find a valid Docker environment`. El informe de failsafe dice: `UnixSocketClientProviderStrategy: failed with BadRequestException (Status 400)`, `DockerDesktopClientProviderStrategy: failed with NullPointerException (getSocketPath() is null)`.
- **Diagnóstico.** El socket **conecta** (hay respuesta HTTP) pero devuelve **400** con la etiqueta `com.docker.desktop.address=unix:///var/run/docker-cli.sock`: dentro del devcontainer `/var/run/docker.sock` apunta al socket **del CLI**, no al del motor. No lo arreglan `DOCKER_HOST`, `TESTCONTAINERS_HOST_OVERRIDE` ni fijar `api.version`.
- **Causa raíz.** Una particularidad de **Docker Desktop en Windows + WSL**, no del proyecto. Y hay una **corrección posterior**: el diagnóstico inicial estaba a medias. El `*IT` de **Quarkus sí se ejecuta** en el devcontainer añadiendo **una sola variable**: `TESTCONTAINERS_RYUK_DISABLED=true`. Lo que no se alcanzaba no era el motor, era **Ryuk** (el contenedor de limpieza). Medido: `Tests run: 1, Failures: 0, Errors: 0` en 38 s. El árbol de Spring sigue sin poder: su Testcontainers es más viejo y **no negocia** con el socket de Docker Desktop.
- **Arreglo.** Los `*IT` de Spring se verifican en el CI (Docker nativo); el de Quarkus, en local y en el CI.
- **Lección.** "No funciona Testcontainers" era falso; lo que no funciona es **un cliente concreto** contra **un socket concreto**. Y el comando que lo demuestra:
  `docker exec -u vscode -e TESTCONTAINERS_RYUK_DISABLED=true <devcontainer> bash -lc 'cd /workspaces/aggora/services && mvn verify -pl quarkus/ingestion-normalizer -am'`.

### 7.15 `host.docker.internal` y los puertos reenviados

- **Síntoma.** Prometheus da `connection refused` contra un servicio que está **perfectamente vivo**.
- **Diagnóstico.** El puerto no está reenviado al host de WSL2.
- **Causa raíz.** `host.docker.internal` solo responde para los puertos que el host de WSL2 tiene **reenviados**.
- **Arreglo.** Los puertos de los dos gateways están declarados en `forwardPorts` del devcontainer **en vez de** depender del auto-descubrimiento de VS Code. (Y en Docker Linux hay que añadir `extra_hosts: host.docker.internal:host-gateway`, que es lo que hace `deploy/docker-compose.vm.yml`.)
- **Lección.** Un "servicio caído" que responde a mano desde la terminal suele ser un problema de red, no del servicio. Mira el panel Ports antes que los logs.

### 7.16 Un plugin en `<pluginManagement>` que no activa nada y un test que nunca se ejecutó

- **Síntoma.** El `*IT` de Quarkus **existía, compilaba y no se ejecutaba en ningún sitio**. En el CI, surefire lo ignoraba por convención y el job de integración solo listaba dos módulos de Spring.
- **Diagnóstico.** Failsafe estaba declarado en `<pluginManagement>` del padre de Quarkus, que **configura pero no activa**.
- **Causa raíz.** Confundir configuración con activación.
- **Arreglo.** Failsafe en `<plugins>` del padre y el módulo añadido al job de integración. Verificado con contenedores de verdad: `Tests run: 1, Failures: 0, Errors: 0`.
- **Lección.** Un test que no corre es documentación con sintaxis de test. Merece la pena **auditar qué corre de verdad**, no qué está escrito.

### 7.17 Dos tests que compartían tabla y contaban filas globales

- **Síntoma.** Los tests de auditoría pasaban o fallaban **según el orden** de ejecución.
- **Diagnóstico.** Los dos contaban filas de `audit_events` y compartían tabla: el segundo veía lo que dejaba el primero.
- **Causa raíz.** Estado compartido entre tests.
- **Arreglo.** Limpiar las tablas de auditoría entre tests.
- **Lección.** Un test que depende del orden es un test que miente. Si un test cuenta filas **globales**, está midiendo el mundo, no su caso.

### 7.18 Problemas más pequeños (pero que costaron tiempo)

| Síntoma | Causa y arreglo | Lección |
|---|---|---|
| Topic `market.ticks.raw` con **1 partición** que nadie declaró | Un `kafka-console-consumer` olvidado pedía metadatos y el broker lo autocreaba. Arreglo: `KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"`. | Un error tipográfico debe fallar, no crear un topic silencioso con las particiones equivocadas. |
| El productor **se queda colgado** sin errores en el log | El Schema Registry no crea su `_schemas` y el serializador Avro corre **en el mismo hilo que el `send`**. Arreglo: declarar `_schemas`. | Los timeouts del cliente del registry importan tanto como los del broker. |
| **500 en vez de 503** (ya visto en 7.5) | `try` mal colocado. | — |
| El endpoint de analítica reventaba con `HttpMessageNotWritableException` | Jackson intentaba serializar el `getSchema()` del Registro Avro. Arreglo: DTO propio. | La API HTTP no debe quedar atada al contrato de Kafka. |
| Tomcat se fue al **8080** | `server.port` estaba dentro de `spring:`. | En Spring Boot, `server.*` va en la raíz. |
| `/analytics/EUR/USD` daba **404** | La barra del par parte la URL. Arreglo: query param. | Los datos de negocio pueden contener caracteres que rompen una ruta. |
| Los ficheros montados tenían permisos **600** | Dentro del contenedor no corre root: Prometheus uid 65534, Grafana 472, en bucle de reinicio. Arreglo: `chmod 644`. | Cuando montas un fichero, mira sus permisos: el proceso de dentro no es tu usuario. |
| `--kafka.server` del exporter con comas | Intenta resolver la cadena entera como un host (`too many colons in address`). Arreglo: repetir la opción por broker. | Lee la ayuda de la herramienta, no supongas listas. |
| **327 errores** en el log de auditoría | El driver de Postgres no convierte un `Instant`: `Can't infer the SQL type`. Arreglo: `OffsetDateTime`. | Errores que solo aparecen en marcha. |
| `target/` de `root` y `...jar is read-only` | Compilar con Maven desde fuera del devcontainer. Arreglo: `chown -R vscode:vscode` y compilar con `docker exec -u vscode`. | El mismo tropiezo por la puerta de atrás. |
| **36.000 alertas en dos minutos** | Avisar en cada actualización de ventana que supera el umbral. Arreglo: una vez por episodio + histéresis (40/20) → **686**. | Una alerta que se repite no es una alerta. |
| `NoSuchMethodError` que no cuadraba con el código | Basura de compilación: el `.class` decía una cosa y el código otra. Arreglo: `mvn clean`. | Ante un error raro de firma o de clase, `mvn clean` antes de investigar nada más. |
| El test de estrés daba **CPU negativas** | `bash -lc "... java ..."` aparecía en `pgrep` y compartía directorio con el servicio (dos entradas por servicio, el `join` cruzaba filas). | Un test que miente en la medición es peor que no tenerlo. |
| Solo se sumaba el **primer servicio** en `awk` | La misma variable servía de patrón y de acumulador, y `awk` la reescribía. Arreglo: nombres distintos. | — |
| Los nombres de variables de entorno **no coincidían** entre el handler de Lambda y Terraform | Compilaba y en AWS habría arrancado apuntando a `kafka-1:9092`. Arreglo: alinear el handler con el contrato de despliegue. | Integra y verifica; no escribas y confíes. |
| A la Lambda le **faltaba la referencia de divisas** | Solo publicaba canónico y DLT: el pipeline desplegado se habría quedado sin tipos de cambio y el join habría fallado. Arreglo: tercer productor + test (5 en verde). | Los dos lados pueden estar "bien" y el conjunto mal. |
| Se subía a Lambda el **jar fino** (121 KB) en vez del gordo del shade (34 MB) | `ClassNotFoundException` en la primera invocación. Arreglo: apuntar al `-shaded.jar`; el CI lo construye y lo sube como artefacto. | — |

---

## 8. Prácticas guiadas (laboratorio)

Cada práctica: objetivo, preparación, **el comando exacto**, **qué deberías ver** y qué
significa si ves otra cosa. Los comandos van marcados **(host)** si se ejecutan fuera
del devcontainer (Docker) o **(devcontainer)** si necesitan Java/Maven.

### 8.0 Preparación común

```bash
# (host) infraestructura: 3 brokers KRaft + Schema Registry + Postgres + Grafana
docker network create aggora-net 2>/dev/null || true
docker compose -f infra/docker-compose.yml up -d

# (devcontainer) compilar los tres árboles
cd /workspaces/aggora/services && mvn -q -DskipTests package

# (devcontainer) arrancar los siete servicios de Spring, en orden y esperando
cd /workspaces/aggora && bash scripts/start-services.sh    # logs en /tmp/<servicio>.log
```

Las API keys son opcionales: sin ellas el simulador genera precios sintéticos y lo dice
por el log. Para tener datos reales:
`export TWELVEDATA_API_KEY=...` y `export ALPHAVANTAGE_API_KEY=...`.

**Si algo no arranca:** mira `/tmp/<servicio>.log`. Si es un motor de Streams y dice
`MissingSourceTopicException`, es la carrera de arranque (sección 7.1): espera y vuelve a
lanzar el script.

---

### 8.1 Romper el motor de Streams y ver la sonda en DOWN

**Objetivo.** Comprobar con tus ojos que un servicio de Streams puede estar **vivo y
roto**, y que la sonda lo dice.

**Preparación.** Los servicios en marcha. El escenario que se reprodujo en el repo fue
**invalidar el checkpoint del GlobalKTable** (que apunta a offsets que la compactación se
lleva por delante); en la práctica se hizo borrando el topic compactado
`market.fx.reference`.

```bash
# (devcontainer) estado de partida: el motor procesa
curl -s localhost:8085/actuator/health | jq -r .status          # UP
curl -s localhost:8085/analytics?symbol=EUR/USD\&minutes=3 | head -c 200

# (host) invalida el checkpoint: borra el topic compactado y déjalo recrear
docker exec aggora-kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --delete --topic market.fx.reference
# (el normalizer lo vuelve a crear; el checkpoint viejo de Streams ya no vale)
```

Si prefieres no tocar el topic, la alternativa es borrar el directorio de estado
**mientras el proceso no está mirando**: `rm -rf /tmp/aggora-streams-state` y reiniciar el
servicio. Cualquiera de las dos provoca lo mismo.

**Qué deberías ver.**

```bash
# (devcontainer) el proceso VIVE, pero el motor no procesa
curl -s -o /dev/null -w "%{http_code}\n" 'localhost:8085/analytics?symbol=EUR/USD&minutes=3'
# 503  (no 500, no 200)

curl -s localhost:8085/actuator/health | jq -c '{status, detalle: .components.kafkaStreams}' 
# {"status":"DOWN", ... "estado":"ERROR" ...}

curl -s localhost:8085/actuator/prometheus | grep aggora_kafka_streams_running
# aggora_kafka_streams_running 0.0
```

**Si ves otra cosa.** Si `/analytics` da 200, tu checkpoint no se invalidó de verdad:
repite borrando el directorio de estado con el servicio parado. Si el **proceso** se ha
muerto, entonces no has reproducido el fallo silencioso sino un crash (otra cosa).
**Arreglo**: reiniciar el servicio de analítica; Streams limpia el estado local y relee,
y la sonda vuelve a UP. Es exactamente lo que debe hacer un supervisor.

---

### 8.2 Provocar un mensaje inválido y verlo en el DLT con su cabecera

**Objetivo.** Ver el camino de descartes completo, con el motivo.

```bash
# (devcontainer) arranca el simulador inyectando un tick inválido cada 300
export AGGORA_INVALIDTICKEVERYN=300
bash scripts/start-services.sh
```

**Qué deberías ver.** En el log del normalizador (`/tmp/ingestion-normalizer.log`),
líneas `[DLT] part=... offset=... key=... -> market.ticks.raw.DLT | motivo: precio ausente
o no positivo`. Y el topic:

```bash
# (host) los descartes, con cabeceras
docker exec aggora-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic market.ticks.raw.DLT \
  --max-messages 1 --property print.headers=true
```

Verás el **valor en binario** (Avro, no legible a ojo) y, delante, la cabecera
`x-dlt-reason: precio ausente o no positivo`. La versión de Quarkus manda **el mismo
texto y la misma cabecera** (verificado en vivo): `6 mensajes al DLT` en la prueba del
port.

**Si ves otra cosa.** Si no llega nada al DLT, comprueba que el valor de
`AGGORA_INVALIDTICKEVERYN` es el que acepta **tu** implementación: Spring usa *relaxed
binding* (`AGGORA_INVALIDTICKEVERYN` → `aggora.invalidTickEveryN`); Quarkus mapea el
entorno de forma **exacta** (`AGGORA_INVALID_TICK_EVERY_N`, o mejor `-Daggora.invalid-tick-every-n=300`).
Ese desajuste ya rompió un despliegue en silencio (sección 9).

---

### 8.3 Arrancar dos instancias del normalizer: reparto y rebalanceo

**Objetivo.** Ver repartirse las 6 particiones entre dos procesos del **mismo**
`group.id`, y el hueco del rebalanceo.

```bash
# (devcontainer) primera instancia ya corriendo (start-services.sh). Lanza la segunda:
cd /workspaces/aggora/services/spring/ingestion-normalizer
java $SPRING_JAVA_OPTS -jar target/ingestion-normalizer-spring-0.1.0-SNAPSHOT.jar \
  > /tmp/normalizer-2.log 2>&1 &

# (host) el reparto, por instancia
docker exec aggora-kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group ingestion-normalizer
```

**Qué deberías ver.**

- En `/tmp/normalizer-2.log`, y en el log de la primera, las líneas del
  `RebalanceLogger`: en la primera `REVOCADAS 6 particiones` → `ASIGNADAS 3`; en la
  segunda `ASIGNADAS 3`. Total: **3 y 3**.
- La tabla del grupo con las 6 particiones repartidas y un `LAG` que baja a 0.
- El reparto de particiones **no es el que imaginas**: con 12 símbolos y 6 carriles, hay
  carriles que se quedan más cargados (en una prueba, el carril 0 se quedó vacío). Hay que
  **mirarlo, no suponerlo**.

**Si ves otra cosa.** Si la segunda instancia dice `ASIGNADAS 0`, ya hay 6 procesos
leyendo (el paralelismo máximo es el número de particiones). Si el rebalanceo tarda
**~45 s** en notarse tras matar una instancia, es porque la mataste con `kill -9`: el
grupo no se entera hasta que pasa el *timeout* de sesión. Con `Ctrl-C` (SIGTERM) el
consumidor se despide y el rebalanceo es inmediato. Por eso `scripts/stop-services.sh`
usa `pkill -TERM` y no `-9`.

---

### 8.4 Matar un broker y ver ISR y `min.insync.replicas`

**Objetivo.** Ver la diferencia entre "tengo copias" y "no pierdo datos".

```bash
# (host) estado sano: ISR con las tres copias
docker exec aggora-kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --topic market.ticks.raw

# 1) se cae UN broker: no pasa nada
docker stop aggora-kafka-3
docker exec aggora-kafka-2 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --topic market.ticks.raw
# Isr: 3,1,2  ->  Isr: 3,1   (una copia fuera; el líder se elige solo)
# los offsets siguen subiendo: 639 -> 705 en 12 s

# 2) se cae un SEGUNDO broker: se deja de escribir, a propósito
docker stop aggora-kafka-2
# offsets CONGELADOS (751 -> 751) y el productor:
#   "Got error produce response ... NOT_ENOUGH_REPLICAS"

# 3) vuelven los brokers: se recupera sin perder nada
docker start aggora-kafka-2 aggora-kafka-3
# Isr: 1 -> Isr: 1,3,2  y el offset salta: 751 -> 1310
```

**Qué significa cada cosa.**

- Con **1 caído** el clúster sobrevive: `replication.factor=3` y mayoría de 2 de 3.
- Con **2 caídos**, `acks=all` **no puede** confirmar sin arriesgarse a perder el
  mensaje, así que el productor **se niega y reintenta**. **No es un fallo: es la
  garantía funcionando.**
- El salto de 751 a 1310 son los mensajes que el productor tenía **en el buffer** durante
  el apagón: los guardó, siguió reintentando y los entregó dentro de su
  `delivery.timeout.ms`. Ningún tick perdido.

**Si ves otra cosa.** Si el clúster **no** deja de escribir con dos brokers caídos,
comprueba `KAFKA_MIN_INSYNC_REPLICAS` y que los topics no se hayan declarado con
réplicas explícitas. Y recuerda: **un clúster de 3 sobrevive a 1 caída, no a 2**; para
dos hacen falta 5.

---

### 8.5 Medir exactly-once (`read_committed` vs `read_uncommitted`)

**Objetivo.** Ver que un registro abortado **existe en el log** y que un consumidor
`read_committed` no lo ve nunca.

```bash
# (devcontainer) la carrera, contra los 3 brokers locales, sin Testcontainers
cd /workspaces/aggora/services
mvn -q -pl spring/order-matching-engine dependency:build-classpath -Dmdep.outputFile=/tmp/cp.txt
cd /workspaces/aggora
java -cp "$(cat /tmp/cp.txt)" scripts/ExactlyOnceRaceCheck.java
```

**Qué deberías ver.**

```
VIEJA (send + abort inmediato): 8/8 intentos en los que la abortada NO llego al log
NUEVA (send + esperar + abort): 0/8 intentos en los que la abortada NO llego al log
OK: la secuencia del test es determinista; la vieja era la que no probaba nada.
```

Y para la medición "de verdad" (la de la Fase 4, con el motor inyectando fallos cada 10
órdenes), el mismo topic leído con los dos niveles:

| Lectura | Mensajes |
|---|---|
| `read_committed` | **121** |
| `read_uncommitted` | **168** |

Los **47** de diferencia son ejecuciones abortadas que ocupan sitio en el log y que nadie
que lea bien va a ver. Repetido en Quarkus: **83.572** frente a **83.582** (diez
abortadas).

**Alerta de expectativas.** El IT completo (`ExactlyOnceKafkaIT`) **no corre en este
devcontainer por el árbol de Spring** (Testcontainers viejo contra el socket de Docker
Desktop, sección 7.14); corre en el CI. La comprobación local es
`ExactlyOnceRaceCheck.java`, que no necesita contenedores.

**Si ves otra cosa.** Si la secuencia VIEJA pasa a menudo, tu máquina da tiempo al hilo
emisor: la carrera tiene menos holgura, pero la conclusión no cambia (el test **no puede
dar por hecho** que el registro está en el log). Si la NUEVA falla, eso sí es un
problema: `ExactlyOnceRaceCheck` sale con código 1.

---

### 8.6 El laboratorio de evolución de esquemas

**Objetivo.** Preguntar al registro de verdad qué cambios rompen y en qué dirección.

```bash
# (devcontainer) con la infraestructura arriba
bash scripts/schema-evolution-lab.sh
```

**Qué deberías ver.** Siete pasos numerados. Los tres que importan:

```
== 1. Un cambio compatible: anadir venueMic CON valor por defecto ==
   el registro dice: COMPATIBLE

== 2. Un cambio que NO es compatible: el mismo campo SIN valor por defecto ==
   el registro dice: RECHAZADO
   motivo: {"errorType":"READER_FIELD_MISSING_DEFAULT_VALUE", ...}

== 2b. Otro que tampoco: renombrar symbol a ticker ==
   HTTP 409, respuesta: {"error_code":40901,"message":"Schema being registered is
   incompatible with an earlier schema for subject \"market.ticks.canonical-value\""}
   versiones: [1]  <- no ha crecido

== 3. La trampa: BORRAR un campo pasa el filtro BACKWARD y rompe a los antiguos ==
   con el nivel BACKWARD (el de este proyecto): COMPATIBLE
   con el nivel FORWARD: RECHAZADO

== 5. Resumen: el mismo cambio juzgado en las dos direcciones ==
   cambio                                     BACKWARD    FORWARD
   campo nuevo con valor por defecto          COMPATIBLE  COMPATIBLE
   campo nuevo sin valor por defecto          RECHAZADO   COMPATIBLE
   renombrar un campo sin alias               RECHAZADO   RECHAZADO
   renombrar un campo con alias               COMPATIBLE  RECHAZADO
   borrar un campo                            COMPATIBLE  RECHAZADO
   campo opcional (union con null)            COMPATIBLE  RECHAZADO
```

Al final, `== 7. Limpieza ==` con `versiones: [1] (baseline: [1])` y el laboratorio
termina. El script **falla** si el subject no queda como estaba.

**Si ves otra cosa.** Si el paso 0 dice que el subject tiene más de una versión, el
laboratorio se para a propósito (espera exactamente 1). Usa otro subject con
`SUBJECT=...` o parte de un registro limpio. Si no encuentra el registro, arranca la
infraestructura; busca en `localhost:8081` y en `schema-registry:8081` y puedes forzarlo
con `SCHEMA_REGISTRY_URL`.

**Lo que este laboratorio no hace, y es lo importante:** **no produce ni un mensaje** con
los esquemas de prueba. Cada mensaje apunta al ID de su esquema; si lo borras, esos
mensajes quedan **ilegibles para siempre**.

---

### 8.7 El test de estrés y cómo leer su tabla

**Objetivo.** Empujar los dos pipelines con la **misma** entrada por encima de lo que da
el simulador y comparar.

```bash
# (devcontainer) con LOS DOS stacks en marcha
bash scripts/start-services.sh
bash scripts/start-quarkus-stack.sh
bash scripts/throughput-test.sh 20 1500 4000      # ráfagas de 20 s (lo del informe)
```

**Qué deberías ver.** Un resumen como este (los números del informe, `docs/throughput-lab.md`):

```
 1500 msg/s objetivo | entrada   29998 (real  1499/s) | canonical spring   29998 quarkus   29998 |
      analytics spring  149990 quarkus  149990 | drenaje   48s | CPU/mensaje spring  0.61 ms quarkus  1.27 ms |
      pico de lag: normalizer  12270/ 8109  analytics   840/    0 | valido=si
 4000 msg/s objetivo | entrada   79996 (real  3997/s) | canonical spring   79996 quarkus   79996 |
      analytics spring  399980 quarkus  399980 | drenaje  106s | CPU/mensaje spring  0.47 ms quarkus  0.81 ms |
      pico de lag: normalizer  67003/62979  analytics  1639/    0 | valido=si
```

**Cómo se lee.**

1. **`valido=si` es la única prueba de que no se pierde nada**: el DLT no creció y los dos
   canónicos cuadran con la entrada.
2. **El pico de lag por grupo** dice **quién** no da abasto: el normalizer (transformar y
   publicar) o el motor de Streams (ventanas, joins, estado). En el informe, el cuello es
   el **normalizer** (pico de 67.003/62.979 frente a 1.639/0).
3. **La columna que compara frameworks es CPU por mensaje** (misma máquina, mismos
   mensajes, a la vez): ahí el coste **no** empata (Quarkus gasta más y acumula menos
   atraso).
4. Los dos motores de Streams gastan **lo mismo** (11,1 s frente a 11,5 s). Eso es el
   **control** de que la medición no se ha ido de las manos: por debajo es la misma
   librería.

**Si ves otra cosa.** `valido=NO` con `DLT +N` significa que el generador está mandando
algo que no pasa el validador y lo medido es el camino del descarte, no el pipeline. Si
algún grupo aparece sin consumidores, se marca como **caído**, no como "lag 0" (esa es
justamente la trampa 7.12). Y recuerda las tres letras pequeñas: **un portátil
compartido, ráfagas cortas y una sola medición por tasa**. La tasa sostenible estimada
con la ráfaga de 120 s está en **600-800 msg/s por stack**, no en las cifras de la
ráfaga.

---

### 8.8 El gateway en vivo

**Objetivo.** Ver la plataforma funcionando sin mirar topics a mano.

```bash
# (devcontainer) la página de demo y la comprobación por WebSocket
curl -s -o /dev/null -w "%{http_code}\n" localhost:8089/     # 200: la página (Spring)
java scripts/GatewayLiveCheck.java localhost 8089 12         # 101 + recuento por tipo

# (si arrancaste el stack Quarkus) el mismo chequeo en el 8189
curl -s -o /dev/null -w "%{http_code}\n" localhost:8189/
java scripts/GatewayLiveCheck.java localhost 8189 12
```

**Qué deberías ver.** Handshake `101 Switching Protocols` y, en 12 segundos, algo como:

| Gateway | handshake | ticks | posiciones | alertas |
|---|---|---|---|---|
| Spring (8089) | `101 Switching Protocols` | 180 | 10 | 30 |
| Quarkus (8189) | `101 Switching Protocols` | 180 | 10 | 11 |

Las dos páginas muestran **las mismas** posiciones y alertas al mismo tiempo, porque cada
gateway tiene su propio `group.id` y recibe **su copia**. Es la demostración más barata
de que un grupo de consumo no es un canal exclusivo. El chequeo falla con código 1 si no
llega ningún tick.

**Si ves otra cosa.** Un `connection refused` en el 8089 suele ser el puerto **no
reenviado** al host de WSL2 (sección 7.15): mira el panel Ports y los `forwardPorts` del
devcontainer, no los logs. Si el handshake va pero no llegan ticks, comprueba que el
simulador está produciendo y que el gateway ve los topics (`market.ticks.canonical`,
`portfolio.updates`, `alerts.raised`).

---

### 8.9 Reto final (sin solución en el repo)

Junta dos cosas que ya sabes hacer y **mide**: arranca los dos stacks, baja el umbral de
pico (`aggora.price-spike-bps`, p. ej. a 5 puntos básicos) e instrumenta un mercado con
movimiento; después observa la alerta **una sola vez** en el gateway, no dos. Y párate a
pensar dónde debería vivir el estado del `SpikeDetector` para que un rebalanceo no repita
el aviso: la respuesta, y por qué hoy no está ahí, está en la sección 10.

---

## 9. Preguntas de entrevista

Ordenadas por dificultad y por tema. Cada respuesta se apoya en algo **medido en este
proyecto** cuando existe; si no existe, se dice "**esto no lo probamos**". Esa frase, en
una entrevista, vale más que una invención.

### 9.1 Conceptos (nivel de entrada)

**1. ¿Qué es un topic y qué es un offset?**
Un log que solo crece, con los mensajes numerados. El número es el offset. En este
proyecto, `market.ticks.raw` con 6 particiones; "offset 1785" sin partición no significa
nada.

**2. ¿Por qué la clave de un mensaje importa?**
Porque decide la partición y, con ella, **el orden**. En Aggora la clave es el **símbolo**,
para que los ticks de AAPL se lean en orden y la media móvil tenga sentido.

**3. ¿Leer un mensaje lo borra?**
No. Kafka no es una cola que se vacía: los mensajes se borran por antigüedad o tamaño,
nunca porque alguien los lea. Lo medimos de la forma más tonta: dos consumidores con
**grupos distintos** leen los mismos mensajes a la vez (el gateway y la analítica).

**4. ¿Cuántos consumidores puedo poner?**
Hasta el número de particiones. Con 6 carriles y 8 instancias, 2 miran.

**5. ¿Qué es el lag y para qué sirve?**
Cuántas casillas va el lector por detrás. Es **la** métrica de salud, pero en este
proyecto medimos que **miente**: tuvimos **lag 0 con el productor muerto** (sección 7.12).

### 9.2 Kafka a fondo

**6. ¿At-least-once o exactly-once?**
Los dos, y sé lo que cuesta cada uno. El normalizador es **at-least-once** (commit manual
después de procesar: puede repetir, no puede perder). El motor de matching es
**exactly-once** porque una ejecución repetida es una posición contada dos veces.
Medido: `read_committed` 121 frente a `read_uncommitted` 168.

**7. ¿Cómo evitas duplicados con una reentrega?**
Con **idempotencia en el consumidor**, no con fechas: en la auditoría hay un **índice
único por (topic, partición, offset)**, así que la reentrega no vuelve a insertar. Lo
garantiza la **tabla**, no el código.

**8. ¿Qué pasa cuando entra un consumidor nuevo?**
Rebalanceo: el grupo reparte otra vez las particiones y hay un rato en el que las que se
mueven no las consume nadie. Medido: 6 particiones, dos instancias → **3 y 3**, con
`REVOCADAS 6` → `ASIGNADAS 3` en el log del `RebalanceLogger`.

**9. ¿Por qué matar con `kill -9` tarda 45 s en rebalancear?**
Porque el grupo no se entera hasta que pasa el *timeout* de sesión. Con SIGTERM el
consumidor se despide y es inmediato. Medido: ~45 s con `-9`.

**10. ¿Qué es un topic compactado y cuándo lo usas?**
Un topic que se queda con el **último valor de cada clave**. Aquí: `market.fx.reference`
(último tipo de cambio por par), `audit.events` (último estado por entidad) y `_schemas`.
Aprendizaje medido: **un compactado no es un histórico fiable**, solo el último estado por
clave (sección 7.2).

**11. ¿Qué diferencia hay entre `acks=all` y `min.insync.replicas`?**
`acks=all` es lo que pide el productor; `min.insync.replicas` es lo que exige el broker
para dar una escritura por buena. Con `acks=all` y `min.insync=2`, un mensaje se confirma
cuando **dos** brokers lo tienen. Medido: con 1 broker caído se sigue escribiendo
(offsets 639→705); con 2, **se congela** (751→751) y sale `NOT_ENOUGH_REPLICAS`.

**12. ¿Por qué `NOT_ENOUGH_REPLICAS` no es un fallo?**
Porque es la garantía funcionando: mejor parar que aceptar un mensaje que solo tiene una
copia.

**13. ¿Cuándo un grupo de consumo copia en vez de repartir?**
Cuando son **grupos distintos**. El gateway usa grupo propio y recibe **su copia** de cada
registro; si compartiera grupo con la analítica, le quitaría particiones.

### 9.3 Kafka Streams

**14. ¿KStream o KTable?**
El KStream es la película (cada tick es un hecho); la KTable es la foto (el último valor
por clave). En la cartera, la KTable de posiciones es el estado que sobrevive a un
reinicio.

**15. ¿Dónde vive el estado y qué pasa si el proceso se cae?**
En un **state store** en disco (RocksDB) con una copia en un topic de **changelog**. Si se
cae, se reconstruye desde el changelog. Medido en la práctica: tras reiniciar el entorno
entero, 29 topics, 22 esquemas y 131.932 eventos auditados seguían ahí.

**16. ¿Qué ventana usaste y por qué?**
Dos: **fija** (tumbling) de 30 s para una foto por bloque y **móvil** (hopping) de 60 s
recalculada cada 15 s para una media que se refresca. Las dos a propósito, para ver la
diferencia en el propio topic. `session windows` **no las probamos** (el spec las
menciona).

**17. ¿Cómo cruzas dos streams que no llegan a la vez?**
Con una **ventana de tiempo** en el join (aquí, 5 s). Y las dos patas tienen que caer en
la misma partición: se re-clavan por el símbolo raíz y Streams inserta el topic de
repartición.

**18. ¿Y una tabla de referencia?**
Con una **GlobalKTable**: se copia entera en cada instancia (aquí son dos pares de
divisas), así que la clave de búsqueda se calcula del propio registro y no hace falta
co-particionar. Es el patrón para enriquecer.

**19. ¿Cómo detectas que algo NO llega?**
Con un **punctuator**, que se ejecuta por reloj haya datos o no. Un filtro no puede: si no
llegan mensajes, no hay nada que reaccione.

**20. ¿Qué límite tienen las consultas interactivas?**
Cada instancia solo conoce **sus** particiones. Con varios despliegues hay que preguntar a
la que tiene la clave (o a todas). En este proyecto hay una sola instancia.

**21. Cuentas métricas y el consumidor devuelve 503 al arrancar. ¿Qué pasa?**
El state store existe pero aún no se puede leer (`the stream thread is STARTING, not
RUNNING`). **No es un 500**: es un transitorio. Medido muestreando: 503, 503, 200, ningún
500.

### 9.4 Exactly-once y transacciones

**22. ¿Cuáles son las tres piezas del exactly-once?**
`transaction-id-prefix` (productor transaccional), gestor de transacciones en el
contenedor de escucha (publicar y confirmar el offset en el **mismo** commit) y consumidor
con **`isolation.level=read_committed`**.

**23. ¿Por qué `read_committed` también en la cartera?**
Porque las ejecuciones se publican en transacciones: con `read_uncommitted` entrarían las
**abortadas** y la cartera contaría operaciones que no ocurrieron. Es la mitad del
exactly-once que se olvida.

**24. ¿Idempotencia y transacción son lo mismo?**
No. La idempotencia evita duplicados de **un** productor reintentando **un** mensaje; la
transacción evita duplicados entre **varios** mensajes y offsets, y sobrevive a que el
proceso se caiga en medio. Son complementarias: la transacción necesita idempotencia por
debajo.

**25. `send()` seguido de `abortTransaction()`. ¿El registro abortado está en el log?**
**No necesariamente**, y esto lo medimos: `send()` es asíncrono y `abortTransaction()`
**descarta lo que el hilo emisor no ha mandado**. La secuencia "send y abortar inmediato"
falló **8 de 8 veces**. La correcta: esperar a **ver** el registro con `read_uncommitted`
y solo entonces abortar. **8 de 8 correctos.**

**26. ¿Por qué el fallo inyectado se decide por el `orderId` y no por un contador?**
Porque un contador describe el **momento**, no el mensaje: al reintentar avanza, el fallo
desaparece y el mensaje se procesa bien, así que **nunca llega al DLT**. Un DLT es para
venenos.

### 9.5 Esquemas y contratos

**27. ¿Por qué Avro y no JSON?**
Tipos declarados (decimal en dinero, timestamp de verdad), mensajes más pequeños y un
notario (el Schema Registry) que **rechaza** un cambio incompatible antes de desplegar.

**28. ¿Qué cambio de esquema es seguro en las dos direcciones?**
Solo **añadir un campo con valor por defecto**. Medido contra el registro: el resto obliga
a decidir quién se despliega primero.

**29. ¿Borrar un campo rompe?**
**Pasa** el filtro BACKWARD (el lector nuevo ignora lo que no conoce) y **rompe** a los
consumidores ya desplegados: el registro contesta a la pregunta que le haces, y hay que
hacerle las dos. Por eso el laboratorio imprime las dos columnas.

**30. ¿Por qué el laboratorio no produce mensajes?**
Porque cada mensaje apunta al **ID de su esquema**: si lo borras, esos mensajes quedan
ilegibles para siempre. Se registra, se comprueba y se borra, sin producir.

**31. ¿Por qué el canónico es un subject distinto del crudo?**
Porque evolucionan por separado: el canónico puede ganar campos sin tocar el contrato del
crudo. Por eso el normalizador construye un objeto **nuevo**.

### 9.6 Spring vs Quarkus

**32. ¿Qué cambió en Kafka Streams al portar a Quarkus?**
**Nada.** Las tres topologías se copiaron tal cual, incluido el punctuator. Lo que cambia
es quién envuelve el motor (Spring: `@EnableKafkaStreams` + builder; Quarkus:
`@Produces Topology`) y de dónde sale la configuración.

**33. ¿Dónde ayudó el modelo reactivo y dónde estorbó?**
Ayudó: la transacción se pide en el código (`withTransactionAndAck`), los canales son
tipados y `/q/health` trae la sonda del motor puesta (en Spring fueron 40 líneas de
`StreamsHealth`). Estorbó: un `Emitter` no tiene `RecordMetadata`, el scheduler no baja de
1 s y la configuración es estricta.

**34. ¿Migrarías un equipo de Spring a Quarkus?**
No por gusto. Medido: Quarkus arranca antes en los siete servicios (24-47 % menos) y usa
menos memoria en seis de siete (5-33 %), pero **no migraría siete servicios sanos por un
segundo de arranque**. Quarkus se gana el sitio cuando el arranque o la escala a cero son
el problema.

**35. ¿Por qué el nativo es tan rápido?**
Porque no arranca una JVM: el análisis está hecho en la compilación. Medido: **0,022 s**
frente a 1,123 s de Quarkus JVM y 2,099 s de Spring; RSS **114-124 MB**. El precio: 91,7 MB
de binario y **cuatro gotchas** (BouncyCastle, Brotli, commons-compress+xz y la reflexión).

### 9.7 Diseño

**36. ¿Por qué el topic `orders.incoming` tiene la clave por símbolo y no por cuenta, si el spec pedía cuenta?**
Desviación consciente: el libro de órdenes es **por instrumento** y todas sus órdenes
tienen que caer en la misma partición. Con la cuenta como key, cada partición tendría un
libro incompleto. Se documenta en vez de sufrirlo en silencio.

**37. ¿Cómo mantienes el evento auditado y su publicación sin perder ninguno?**
Con **transactional outbox**: evento + recado en la **misma** transacción de Postgres, y
un publicador aparte que lee pendientes y los manda. Se puede publicar dos veces
(at-least-once) y no hace daño porque el topic es compactado con clave por entidad.

**38. ¿Por qué el estado de cartera se clava por `cuenta|símbolo` y el topic por cuenta?**
Porque la posición que interesa es por cuenta **y** instrumento, pero el spec pide
`portfolio.updates` con key = cuenta. El re-clavado obliga a reparticionar, y Streams lo
hace solo (`positions-store-repartition`).

**39. Un servicio hace tres cosas y quieres añadir una cuarta.**
Mira si alguna de las tres se puede separar (SRP). En este proyecto cada servicio es una
responsabilidad y cada clase con lógica tiene test unitario; las reglas de alerta son
clases separadas que se **fusionan**, así que añadir una es añadir y fusionar.

**40. ¿Qué patrones decidiste NO usar?**
Saga (no hay operación distribuida que compensar), circuit breaker (Kafka reintenta y no
hay llamadas externas en caliente), CQRS completo (bastaba CQRS-lite con state store) y
EKS (el plano de control se cobra por hora y no aporta nada que no dé una VM con systemd).

### 9.8 Operación y diagnóstico

**41. Un servicio de Streams responde pero no procesa nada. ¿Qué haces?**
Mirar `streams.state()` y la sonda, no el proceso. `REPLACE_THREAD` **no arregla** un
estado global inconsistente y `SHUTDOWN_APPLICATION` **empeora** las cosas (429 MB de log
en 28 s y el proceso tampoco muere). La respuesta es la sonda + el supervisor.

**42. ¿Cómo despliegas esto sin Kubernetes?**
Broker gestionado creado a mano, una VM con `systemd` y `Restart=always` para los
servicios con estado, y el normalizador en Lambda. **No se ha ejecutado en AWS**: no hay
cuenta; los artefactos están validados, no desplegados.

**43. ¿Qué se puede serverless y qué no?**
Solo lo sin estado. Los tres de Streams necesitan state store en disco, rebalanceo y
transacciones abiertas. Y en Lambda la aplicación **no es la misma**: no hay bucle de
consumo, el valor llega en base64 y los offsets los confirma el servicio.

**44. Has medido throughput. ¿Cuál gana?**
En throughput, **empatan** (47× la tasa del simulador, sin perder un mensaje): a esa
escala manda la entrada. Lo que difiere es el **coste por mensaje**, y ahí Spring gana en
esta configuración (Quarkus gasta ~1,7× y hasta 3,6× en el normalizer, pero acumula la
mitad de atraso). Con letra pequeña: un portátil compartido y ráfagas cortas.

### 9.9 "Cuéntame un fallo que hayas tenido"

Elige **una** y cuéntala con sus cuatro actos. Las cuatro mejores de este repo:

1. **El fallo silencioso.** Un servicio de Streams en ERROR, vivo y contestando 503 durante
   horas. `REPLACE_THREAD` no bastaba, `SHUTDOWN_APPLICATION` fue peor (429 MB de log en
   28 s) y la respuesta no estaba en el manejador sino en la **sonda** y el supervisor.
2. **Lag 0 con el productor muerto.** ~18.000 mensajes perdidos en silencio, 75.055 GC
   completos detrás, 13 JVM sin límite de heap. El lag, la métrica de salud, decía que
   todo iba bien.
3. **El `-Dnative` que decía BUILD SUCCESS** y no generaba binario. Un éxito que no
   produce nada es peor que un error.
4. **El test de exactly-once que no era inestable, sino falso.** `send()` asíncrono +
   `abortTransaction()` que descarta: 8 fallos de 8. Cuando un test falla "a veces", lo
   primero es preguntarse si lo que **supone** ha pasado de verdad.

---

## 10. Áreas de mejora

Honestas y accionables. No es una lista de deseos: cada punto dice **qué falta**, **dónde
está la receta** y **qué te impide cerrarlo hoy**.

### 10.1 Afinar el normalizer (la mejora con más retorno)

- **Lo que se sabe.** El cuello de botella es el **normalizer** (pico de lag 67.003/62.979
  frente a 1.639/0 en la analítica). En la ráfaga larga, Spring gasta **3,6× menos CPU**
  en el normalizer (0,21 vs 0,74 ms/msg) pero **acumula el doble de atraso** (55.746 vs
  24.939 mensajes).
- **Qué falta.** Un **barrido de tasas** para encontrar el punto donde el lag deja de
  crecer: la receta está escrita en `docs/throughput-lab.md` §8:
  `bash scripts/throughput-test.sh 120 600 800 1000` (~15 minutos).
- **El ajuste más prometedor.** Subir `max.poll.records` **en los dos** (p. ej. a 1.000):
  hoy están idénticos en 200 (verificado, para que la comparación fuera justa). Si la
  brecha de CPU se estrecha con lotes más grandes, parte del coste de Quarkus es **por
  mensaje** y se amortiza; si no, es del modelo de despacho.
- **Antes de culpar al framework.** Ajustar también hilos de trabajo y `commit-strategy`
  del lado Quarkus. La diferencia medida **puede ser configuración, no diseño**, y eso no
  está cerrado.

### 10.2 Despliegue real

- **El estado.** **Nada se ha aplicado en AWS.** No hay cuenta ni credenciales y
  desplegar cuesta dinero que nadie ha aprobado. Lo que hay son artefactos **validados**:
  Terraform (`fmt -check` + `init -backend=false` + `validate`), el compose de la VM
  (`config -q`), la unidad de systemd (`systemd-analyze verify`) y los tres árboles
  compilando con sus tests.
- **Lo que no se puede verificar sin cuenta:** que la red y los permisos dejen hablar a la
  VM con el broker, que el *event source mapping* entregue lotes y que las alarmas salten.
  Está escrito como pasos concretos en `deploy/README.md`, no como intuición.
- **Lo que dolerá primero** (documentado en el capítulo 21 de `docs/kafka-101.md`): las
  credenciales (un `Access denied` de SASL no dice qué falta), los topics con las **mismas
  particiones** (3 frente a 6 cambia el paralelismo) y la caducidad de certificados.

### 10.3 Observabilidad

- **Lo que hay.** Lag por grupo y por topic, throughput, **DLT**, brokers vivos,
  `aggora_kafka_streams_running`, arranque, hilos, memoria y CPU. Panel y datasource
  **provisionados desde el repo**.
- **Lo que falta.** **Métricas de negocio**: mensajes procesados por servicio, descartes
  por motivo, posiciones y alertas por minuto, latencia de extremo a extremo
  (`eventTime` → `normalizedAt` ya está en el contrato canónico: se puede calcular).
- **SLOs de lag**: hoy el panel lo pinta, pero **no hay una alerta** de "el lag crece sin
  parar". Es la mitad que convierte el panel en operación.
- **La asimetría consciente.** Las métricas de aplicación solo están en los servicios con
  servidor web (simulador, analítica, gateway); los otros cinco no tienen `actuator` a
  propósito, porque añadírselo cambiaría justo lo que se quería medir. **Medir la JVM de
  los tres de Streams en producción** sigue pendiente.

### 10.4 Seguridad

- **Lo que hay.** Cero ingress en la VM (solo SSM Session Manager), secretos en SSM
  `SecureString`, el state de Terraform fuera de git, `.gitignore` con `*apikey*` y
  `.secrets/`.
- **Lo que no hay, y hay que decirlo.** En local **no existe SASL ni TLS**: el cluster de
  desarrollo es `PLAINTEXT` con la red `aggora-net` de confianza. No hay autenticación en
  los endpoints HTTP ni en el WebSocket; Grafana abre anónimo con rol Admin (a propósito,
  es un entorno de desarrollo); las credenciales de Postgres y Grafana son `aggora/aggora`
  y `admin/admin`.
- **Rotación de credenciales:** no está implementada ni probada. El despliegue monta la
  cadena JAAS a partir de usuario y clave, y el módulo depende del mecanismo (PLAIN y
  SCRAM no usan el mismo), pero **no hay rotación ni prueba de caducidad**.
- **El Schema Registry** tiene su propia API key y en el despliegue es opcional (vacío =
  sin auth). **No probado** contra un SR con auth.

### 10.5 Gobierno de esquemas

- **Lo que hay.** El laboratorio mide los seis cambios en las dos direcciones y el
  playbook de `docs/schema-evolution-lab.md` §7 incluye el `curl` de compatibilidad.
- **Lo que falta.** Meter esa comprobación **en el CI**: el propio playbook dice que "se
  puede meter en el CI y que la build falle sola". Hoy la compatibilidad se verifica
  **cuando alguien ejecuta el laboratorio**, no en cada cambio.
- **El nivel de compatibilidad** se queda en BACKWARD y el laboratorio lo cambia un
  momento (dentro de `comprobar_con_nivel`) para enseñar la otra columna. Un proyecto
  real querría **decidir el nivel por subject** y comprobarlo automáticamente.

### 10.6 Los tests que faltan

- **El árbol de Quarkus tiene un solo `*IT`** (`NormalizerKafkaIT`). Los otros módulos
  Quarkus tienen tests unitarios/de topología, pero **no integración**: no hay un IT de
  exactly-once en Quarkus, ni de outbox con Postgres, ni de rebalanceo.
- **`gateway-ws` no tiene ningún test** en ninguna de las dos implementaciones. La
  comprobación es `scripts/GatewayLiveCheck.java`, manual.
- **No hay un test automatizado de at-least-once** (reprocesión tras un `kill -9`): se
  verificó a mano en la Fase 1. El spec pedía "al menos un test por servicio que pruebe
  rebalanceo o at-least-once con un fallo inyectado a mitad"; eso **no está cubierto**.
- **Los `*IT` de Spring no corren en el devcontainer** (Testcontainers viejo contra el
  socket de Docker Desktop): se verifican en el CI. El de Quarkus sí corre en local con
  `TESTCONTAINERS_RYUK_DISABLED=true`.

### 10.7 El CI como matriz

- **Lo que hay.** Cuatro jobs (`unit`, `artifacts`, `infra`, `integration`) y todos corren
  en push, PR y manual.
- **Lo que falta.** Una **matriz** por árbol (Spring / Quarkus / Lambda) para que un fallo
  diga de un vistazo **cuál** rompió, y para que los tres corran en paralelo en vez de
  dentro de un solo `mvn test`. El job de integración **lista los módulos a mano**
  (`spring/audit-log,spring/order-matching-engine,quarkus/ingestion-normalizer`): cada
  `*IT` nuevo hay que acordarse de añadirlo, y eso es exactamente el fallo que dejó el IT
  de Quarkus sin ejecutar durante fases enteras (sección 7.16). Una matriz que descubra
  módulos por patrón cerraría el agujero.

### 10.8 El coste del heap sin límites en local

- **Lo que pasó.** 13 JVM sobre 23 GB sin límite: cada una coge por defecto un cuarto de
  la RAM, el GC se pone a dar vueltas y los procesos se quedan **colgados sin decir
  nada** (75.055 GC completos, ~18.000 mensajes perdidos en silencio).
- **Lo que hay ahora.** `SPRING_JAVA_OPTS=-Xms128m -Xmx384m` en `start-services.sh` y
  `QUARKUS_JAVA_OPTS=-Xms128m -Xmx320m` en `start-quarkus-stack.sh` (los mismos topes que
  la unidad de systemd).
- **Lo que falta.** Un aviso en el propio script si detecta que ya hay un stack en marcha
  (arrancar los dos con 13 JVM sigue siendo mucho para un portátil), y **repetir la
  comparación con la máquina en reposo** (un stack cada vez) para quitar la contención.
  Los números de `SPRING_VS_QUARKUS.md` son de máquina en reposo; los del test de estrés,
  de las dos a la vez.

### 10.9 Lo que el spec pedía y no está

| Pendiente | Estado |
|---|---|
| `macro-events-simulator` (eventos macro con shock a instrumentos) | **No implementado.** No hay nada de macro en el repo (solo aparece en `SPEC.md`). |
| **Session windows** | **No practicadas.** El proyecto usa tumbling y hopping; el spec las menciona. |
| **Kafka UI** (Redpanda Console o AKHQ) | Pendiente: `CONTRIBUTING.md` lo deja como `localhost:8090 (pendiente)`; no está en el compose actual. Grafana cubre la parte de lag y throughput. |
| **Kafka Connect** (stretch) | No hecho. |
| **ksqlDB** (stretch) | No hecho. |
| **MirrorMaker 2** (stretch) | No hecho. |

### 10.10 Deuda menor (cosas pequeñas y concretas)

- El estado del `SpikeDetector` y del `StaleFeedDetector` vive **en memoria**, no en un
  state store: un rebalanceo lo reinicia y **podría repetir un aviso**. Lo mismo la regla
  de margen. Está marcado con `ponytail:` en el código.
- El **libro de órdenes vive en memoria**: si el motor se reinicia, se pierde (se
  reconstruye solo con las órdenes que vuelvan a entrar). El camino está claro: un state
  store o persistirlo junto al outbox.
- El **límite de margen es global**, no por cuenta ni instrumento.
- La analítica emite **una métrica por tick** (sin `.suppress(untilWindowCloses(...))`):
  didáctico, pero más tráfico del necesario.
- `scripts/build-native.sh` define `NATIVE_M2` y **no lo usa** en la invocación de Maven
  (la caché nativa que describe el comentario no se aplica).
- El compose de 3 brokers **ya no incluye `kafka-init`** y el repo no documenta su
  retirada.
- `docs/interview-notes.md` tiene la sección final **desactualizada** (dice que el nativo
  está pendiente y que los IT no corren). Manda `docs/decisions.md`.
- El `eventTime` de la Fase 1 viajaba como número decimal de segundos: se arregló en la
  Fase 2 con `logicalType`, pero **queda escrito como deuda conocida** en
  `docs/decisions.md`.

---

## 11. Autoevaluación

22 preguntas de confirmación. Las respuestas están **al final, separadas**: tápate y
contesta antes de mirar. Mezclan conceptos, decisiones y diagnóstico de fallos. Las
marcadas con 🔧 son "¿qué harías si ves este síntoma?".

**Preguntas**

1. ¿Qué es el lag y por qué en este proyecto no basta para decir que un servicio está sano?
2. ¿Por qué la clave de `market.ticks.raw` es el símbolo y no el exchange?
3. Explica at-least-once y exactly-once con un caso de este repo para cada uno.
4. ¿Qué tres piezas necesita el exactly-once y qué papel juega cada una?
5. ¿Por qué la cartera lee con `read_committed`?
6. ¿Qué diferencia hay entre un topic compactado y uno normal? Da los dos usos del proyecto.
7. 🔧 `/analytics` devuelve 503 al arrancar y luego 200. ¿Es un fallo? ¿Qué cambió?
8. 🔧 El servicio responde en el 8085 pero no calcula métricas y la sonda dice DOWN. ¿Qué está pasando y qué NO hay que probar?
9. 🔧 Kafka Streams se cae al arrancar con `MissingSourceTopicException`. ¿Causa y arreglo?
10. 🔧 Llega un mensaje imposible y su partición se queda atascada para siempre. ¿Qué falta?
11. ¿Qué cambia de un campo de esquema es seguro en las dos direcciones y por qué?
12. ¿Por qué borrar un campo es peligroso si el registro dice COMPATIBLE?
13. ¿Por qué el laboratorio de esquemas no produce mensajes?
14. ¿Qué cambió en Kafka Streams al portar a Quarkus?
15. Di dos cosas que Quarkus haga mejor que Spring en este proyecto y dos que estorben.
16. ¿Por qué el nativo arranca en 0,022 s y qué cuatro cosas costó?
17. Un servicio de fan-out, ¿commit manual o automático? Justifícalo.
18. ¿Cómo se evita que un duplicado de Kafka se audite dos veces?
19. ¿Por qué el fallo inyectado del motor se decide por `orderId` y no por un contador?
20. 🔧 Ves `NOT_ENOUGH_REPLICAS` con dos brokers caídos. ¿Rompes algo arreglándolo?
21. `send()` seguido de `abortTransaction()`: ¿el registro abortado está en el log? Explícalo.
22. ¿Qué se puede desplegar serverless en Aggora y por qué no los otros?

**Respuestas**

1. Cuántas casillas va el lector por detrás. No basta porque se midió **lag 0 con el
   productor muerto**: los offsets avanzan y no se publica nada (13 JVM sin límite de
   heap, 75.055 GC). Hay que mirar también que el pipeline **produzca**.
2. Porque el orden solo se garantiza dentro de una partición y los precios de un
   instrumento tienen que leerse en orden; con key = exchange, los símbolos de un mercado
   compartirían carril y el orden por instrumento se perdería.
3. At-least-once: el normalizador con commit manual, si muere a media faena **repite**, no
   pierde. Exactly-once: el motor de matching, donde una ejecución repetida sería una
   posición contada dos veces; medido 121 vs 168.
4. Productor transaccional (`transaction-id-prefix`), gestor de transacciones en el
   contenedor de escucha (publicar y confirmar el offset en el **mismo** commit) y
   consumidor con `read_committed`.
5. Porque las ejecuciones se publican en transacciones: con `read_uncommitted` entrarían
   las **abortadas** y la cartera contaría operaciones que no ocurrieron.
6. El compactado se queda con el **último valor de cada clave**; el normal borra por
   antigüedad. Usos: `market.fx.reference` (último tipo de cambio por par) y
   `audit.events` (último estado por entidad).
7. No es un fallo: el state store existe pero aún no se puede leer
   (`the stream thread is STARTING, not RUNNING`). Es un **transitorio**, y por eso 503 y
   no 500. Cuando el motor arranca, pasa a 200. Verificado: 503, 503, 200, ningún 500.
8. El **motor** de Streams está en ERROR mientras el **proceso** sigue vivo (fallo
   silencioso). No hay que probar `REPLACE_THREAD` (no arregla un estado global
   inconsistente) ni `SHUTDOWN_APPLICATION` (enreda el cierre, 429 MB de log en 28 s y el
   proceso tampoco muere). Hay que reiniciar el servicio y que la **sonda** avise.
9. Kafka Streams falla si su topic de origen no existe y los topics los crea quien escribe
   en ellos: es una carrera de arranque. Se arregla arrancando en orden y esperando
   (`start-services.sh`), con `quarkus.kafka-streams.topics` o con `Restart=always`.
10. Un **DLT**. Reintentar un veneno para siempre bloquea la partición y todo lo que venga
    detrás espera. Y el veneno se decide por el mensaje (`orderId`), no por un contador.
11. Añadir un campo **con valor por defecto**: es lo único COMPATIBLE en BACKWARD y
    FORWARD.
12. Porque BACKWARD pregunta "¿el lector nuevo lee los datos viejos?" y el lector nuevo
    ignora el campo que no conoce; pero los consumidores **ya desplegados** sí lo conocen
    y dejan de recibirlo. "Compatible" no significa nada sin decir en qué dirección.
13. Porque cada mensaje apunta al **ID de su esquema**: si produces con un esquema y luego
    lo borras, esos mensajes quedan ilegibles para siempre.
14. **Nada** en la topología: se copiaron tal cual, incluido el punctuator. Cambia quién
    envuelve el motor (`@Produces Topology` frente a `@EnableKafkaStreams`) y de dónde
    sale la configuración.
15. Mejor: la transacción se pide en el código (`withTransactionAndAck`), los canales son
    tipados y `/q/health` trae la sonda del motor puesta. Estorba: un `Emitter` no da
    `RecordMetadata`, el scheduler no baja de 1 s y la configuración es estricta.
16. Porque no arranca una JVM: el trabajo se hizo en la compilación. Costó BouncyCastle
    (TLS), Brotli (compresión), commons-compress+xz y la reflexión de los serializadores
    de Confluent (169 clases generadas desde los jars).
17. **Automático.** En un fan-out no hay dato que perder: si se cae un evento, el siguiente
    trae el precio nuevo. El commit manual solo haría ir más lento el reparto.
18. Con el **índice único por (topic, partición, offset)** y `on conflict do nothing`: la
    idempotencia la garantiza la tabla, no el código.
19. Porque un contador describe el momento: al reintentar avanza, el fallo desaparece y el
    mensaje se procesa bien, así que nunca llega al DLT. Un DLT es para fallos que
    pertenecen al mensaje.
20. No: es la **garantía** funcionando. Con una sola copia al día, `acks=all` no puede
    confirmar sin arriesgarse a perder el mensaje, así que el productor se niega y
    reintenta. Al volver el ISR, entrega lo que tenía en el buffer (751 → 1310).
21. **No necesariamente.** `send()` es asíncrono y `abortTransaction()` descarta lo que el
    hilo emisor todavía no ha mandado: la secuencia "send y abortar inmediato" falló 8 de
    8. La correcta es esperar a verlo con `read_uncommitted` y solo entonces abortar.
22. Solo `ingestion-normalizer`, que no tiene estado. Los tres de Streams necesitan state
    store en disco, rebalanceo con un proceso vivo y transacciones abiertas; apagarlos y
    encenderlos es reconstruir, no reanudar.

---

## 12. Glosario y mapa de ficheros

### 12.1 Glosario

| Término | En una línea | Dónde vive en el repo |
|---|---|---|
| **Topic** | Canal con nombre por el que viajan los mensajes. | `services/*/*/src/main/java/**/config/KafkaTopicsConfig.java`, `.../kafka/TopicCreator.java` |
| **Partición** | Cada carril paralelo de un topic; el orden solo se garantiza dentro de uno. | 6 particiones en `market.ticks.raw`, `market.ticks.canonical`, `market.analytics`, `orders.*`, `portfolio.updates`; 3 en `alerts.raised` y `audit.events`; 1 en los DLT y en `market.fx.reference` |
| **Offset** | Número de casilla de un mensaje dentro de su partición. | Visible en el log del consumidor (`offset=...`) y en `AuditStore` (`source_offset`) |
| **Key (clave)** | Dato que decide la partición; aquí casi siempre el símbolo. | `TickProducer.java`, `TickConsumer.java`, `OrderMatcher.java` |
| **Productor** | Programa que escribe en un topic. | `TickProducer.java`, `TickConsumer.java` (republica), `OrderMatcher.java` |
| **Consumidor** | Programa que lee de un topic. | `TickConsumer.java`, `AuditConsumer.java`, `LiveFeedConsumer.java` |
| **Grupo de consumo** | Equipo de lectores que se reparten las particiones. | `ingestion-normalizer`, `analytics-streams`, `audit-log`, `gateway-ws`... en los `application.yml`/`.properties` |
| **Rebalanceo** | Volver a repartir las particiones al entrar o salir un miembro. | `RebalanceLogger.java`, `RebalanceLog.java` |
| **Commit** | El "marcapáginas": confirmación del offset. | `ack-mode: manual_immediate`, `commit-strategy=latest` |
| **At-least-once** | Puede repetir, no puede perder. | Normalizador, auditoría |
| **Exactly-once** | Ni se pierde ni se duplica. | `KafkaTransactionConfig.java`, `OrderMatcher.java`, `isolation.level=read_committed` |
| **Lag** | Cuánto va el lector por detrás de lo escrito. | Panel de Grafana; `scripts/throughput-test.sh` |
| **Topic compactado** | Se queda con el último valor de cada clave. | `market.fx.reference`, `audit.events` |
| **DLT** (*dead-letter topic*) | Topic de descartes, con el motivo. | `market.ticks.raw.DLT`, `orders.incoming.DLT`; cabecera `x-dlt-reason` |
| **Retry topic** | Topic intermedio para fallos que tardan en resolverse. | `@RetryableTopic` en `AuditConsumer.java` |
| **Transacción** | Publicar y confirmar el offset como una sola operación. | `KafkaTransactionConfig.java`; `withTransactionAndAck` en Quarkus |
| **`isolation.level`** | Si el consumidor ve o no los datos de transacciones sin confirmar. | `read_committed` en matching, cartera, alertas y auditoría |
| **State store** | Almacén local de lo que el stream necesita recordar. | `metrics-tumbling-store`, `metrics-hopping-store`, `positions-store` |
| **Changelog** | Copia en Kafka del state store, para reconstruirlo. | Topics internos que crea Streams (`...-changelog`) |
| **KStream** | Secuencia de hechos (cada tick cuenta). | `MetricsTopology.java`, `ArbitrageTopology.java` |
| **KTable** | Tabla que se actualiza (último valor por clave). | `PortfolioTopology.java` |
| **Ventana tumbling** | Bloques fijos que no se solapan. | 30 s en `windows.tumbling-size` |
| **Ventana hopping** | Bloques que se solapan y se refrescan. | 60 s cada 15 s en `windows.hopping-*` |
| **Grace** | Margen antes de dar una ventana por cerrada. | `windows.grace: 5s` |
| **Punctuator** | Función que Streams llama por reloj, haya datos o no. | `StaleFeedDetector.java` |
| **GlobalKTable** | Copia entera de una tabla en cada instancia. | `market.fx.reference` en `ArbitrageTopology.java` |
| **Join stream-stream** | Cruzar dos streams dentro de una ventana. | `ASML`/`ASML.AMS`, ventana de 5 s |
| **Consulta interactiva** | Preguntar al estado en marcha sin pasar por Kafka. | `AnalyticsQueryController.java`, `AnalyticsResource.java` |
| **ISR** | Copias de una partición que están al día. | `kafka-topics.sh --describe` (`Isr:`) |
| **`min.insync.replicas`** | Copias al día necesarias para dar una escritura por buena. | `KAFKA_MIN_INSYNC_REPLICAS: 2` |
| **Réplica** | Copia de una partición en otro broker. | `KAFKA_DEFAULT_REPLICATION_FACTOR: 3` |
| **Fan-out** | Un grupo propio recibe su copia de cada mensaje. | `gateway-ws` |
| **Transactional outbox** | Evento + recado en la misma transacción de BD; un publicador los manda. | `AuditStore.java` + `OutboxRelay.java` + `schema.sql` |
| **Avro** | Formato binario con esquema declarado e ID por mensaje. | `services/schemas/*.avsc` |
| **Schema Registry** | Notario que guarda los esquemas y valida compatibilidad. | `confluentinc/cp-schema-registry:8.3.1` en `infra/docker-compose.yml` |
| **Subject** | El nombre con el que se registra un esquema (`<topic>-value`). | `market.ticks.raw-value`, `market.ticks.canonical-value` |
| **BACKWARD / FORWARD** | Direcciones de compatibilidad: el nuevo lee lo viejo / el viejo lee lo nuevo. | `scripts/schema-evolution-lab.sh` |
| **Decimal lógico** | Entero con escala declarada, para no usar coma flotante en dinero. | `price` en `tick.avsc` y `canonical.avsc` |
| **`timestamp-millis`** | Entero declarado como fecha. | `eventTime`, `normalizedAt`, `executedAt`... |
| **Tick** | Observación de precio en un instante. | `tick.avsc`; tema de la sección 2.1 |
| **Símbolo** | Nombre corto del instrumento; clave de Kafka. | `AAPL`, `ASML.AMS`, `EUR/USD` |
| **Spread** | Diferencia de precio entre las dos cotizaciones de la misma empresa. | `ArbitrageTopology.java`, `market.arbitrage` |
| **VWAP** | Precio medio ponderado por volumen. | `MetricsTopology.java` |
| **Volatilidad** | Desviación típica de los precios de la ventana. | `MetricsTopology.java` |
| **Volumen** | Suma de tamaños en la ventana. | `MetricsTopology.java` |
| **Libro de órdenes** | Órdenes en espera por instrumento. | `OrderBook.java`, `OrderBooks.java` |
| **Prioridad precio-tiempo** | Primero mejor precio; a igual precio, quien llegó antes. | `OrderBook.java` |
| **Ejecución** | Resultado de cruzar dos órdenes. | `execution.avsc`, `orders.executions` |
| **Posición** | Cuánto tienes de un instrumento. | `portfolio-position.avsc`, `PortfolioTopology.java` |
| **Coste medio** | A cuánto te salió de media lo que tienes abierto. | `PortfolioTopology.apply(...)` |
| **P&L realizado** | Resultado de la parte ya cerrada. | `PortfolioTopology.apply(...)` |
| **Exposición** | `|cantidad| × coste medio`. | `PortfolioTopology.apply(...)` |
| **Margen / `marginBreach`** | Límite y aviso de que la exposición lo superó. | `portfolio-position.avsc`, `AlertingTopology.java` |
| **Arbitraje** | Ganar con la misma cosa a dos precios. | El caso ASML de `ArbitrageTopology.java` |
| **Referencia FX** | Último tipo de cambio por par, tema compactado. | `market.fx.reference`, `fx-rate.avsc` |

### 12.2 Mapa de ficheros

Mapa ampliado del repo (el `README.md` tiene la versión corta):

```
README.md                  portada: qué es, cómo arrancarlo, mapa concepto → servicio
SPEC.md                    especificación original (inmutable)
CONTRIBUTING.md            reglas de trabajo + estado de cada fase con sus verificaciones
SPRING_VS_QUARKUS.md       informe comparativo con números y recomendación honesta
AGENTS.md                  modo de trabajo "ponytail" (senior perezoso)
docs/
  curso-aggora.md          ESTE curso
  kafka-101.md             los 21 capítulos de conceptos en lenguaje llano
  decisions.md             diario de decisiones y problemas, con números (fuente principal)
  throughput-lab.md        manual e informe del test de estrés
  schema-evolution-lab.md  manual del laboratorio de compatibilidad
  dev-environment.md       qué haría distinto la próxima vez con el entorno
  interview-notes.md       las fases leídas como preguntas de entrevista (parcialmente desactualizado)
infra/
  docker-compose.yml       3 brokers KRaft + SR + Postgres + exporter + Prometheus + Grafana
  prometheus/prometheus.yml            scrapes (exporter, apps Spring, apps Quarkus)
  grafana/provisioning/...             datasource y provider de dashboards
  grafana/dashboards/aggora-kafka.json panel provisionado (lag, throughput, DLT, motor)
scripts/
  start-services.sh / stop-services.sh          los 7 servicios Spring, en orden
  start-quarkus-stack.sh / stop-quarkus-stack.sh los 6 Quarkus en paralelo, con sufijo .q
  measure-service.sh                            arranque, RSS, hilos, descriptores
  build-native.sh                               receta del binario GraalVM (4 tropiezos comentados)
  schema-evolution-lab.sh                       laboratorio de esquemas contra el registro real
  throughput-test.sh + AvroLoadGenerator.java   test de estrés con ticks válidos
  ExactlyOnceRaceCheck.java                     la carrera de la transacción, sin Testcontainers
  GatewayLiveCheck.java                         comprobación del WebSocket con la librería del JDK
services/
  pom.xml                    agregador de los tres árboles (no hereda de nadie)
  schemas/                   los 12 contratos Avro (fuente de verdad de las clases generadas)
  spring/                    implementación Spring Boot 4.1.1 (8 módulos + pom padre)
    market-data-simulator/   precios reales + paseo aleatorio + órdenes simuladas
    ingestion-normalizer/    validación, canónico, FX y DLT (commit manual)
    analytics-streams/       ventanas, VWAP, volatilidad, arbitraje y consultas interactivas
    order-matching-engine/   libro de órdenes + exactly-once transaccional
    portfolio-risk/          KTable de posiciones, coste medio, P&L y margen
    alerting-service/        3 reglas, incluido el punctuator de feed parado
    audit-log/               transactional outbox con Postgres + topic compactado
    gateway-ws/              fan-out a navegadores por WebSocket (puerto 8089)
  quarkus/                   los mismos 8, puerto 8189 el gateway; BOM 3.39.3
  lambda/                    la tercera implementación: el normalizer sin estado
deploy/
  README.md                  runbook de despliegue (inglés)
  collect-artifacts.sh       compila y junta los jars en deploy/artifacts/
  user-data.sh               arranque de la VM (AL2023 ARM64)
  systemd/aggora@.service    una unidad plantilla para los 7 servicios
  docker-compose.vm.yml      Postgres + observabilidad en la VM (sin Kafka ni SR)
  terraform/                 S3, SSM, VM, Lambda, SQS y alarmas (validado, no aplicado)
.github/workflows/ci.yml     4 jobs: unit, artifacts, infra, integration
.devcontainer/devcontainer.json  Java 21 + Docker-outside-of-docker + forwardPorts
```

**Y el resumen que cierra el curso:** Aggora no es una plataforma de trading. Es un
**laboratorio** donde cada concepto de Kafka existe una vez, se rompe una vez y se mide
una vez. Lo que te llevas no son las tablas: es el hábito de preguntar "¿esto lo he
medido o me lo estoy creyendo?".

