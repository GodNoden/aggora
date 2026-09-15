# Kafka 101 — para entender Aggora

Esta guía está escrita para alguien que nunca ha tocado Kafka. Cada concepto lleva
tres cosas: la idea en una frase, una analogía de andar por casa, y **dónde aparece
exactamente en este proyecto** (fichero, topic o línea de log). Si algo no se
entiende, es un fallo de la guía, no tuyo: dímelo y la reescribo.

---

## 1. El problema que resuelve Kafka

En Aggora hay dos programas:

- **`market-data-simulator`**: fabrica precios (ticks) y los publica.
- **`ingestion-normalizer`**: los lee, los valida y los normaliza.

Sin nada en medio, el fabricante tendría que llamar directamente al lector. Eso
obliga a que los dos estén vivos al mismo tiempo y a la misma velocidad: si el
lector está saturado, el fabricante frena; si el lector se cae, los precios que
llegaban en ese momento se pierden. Y si mañana quieres tres lectores distintos
(analítica, alertas, auditoría), el fabricante tendría que conocerlos a todos.

Kafka se coloca en medio y los desacopla: el fabricante deja el mensaje y se olvida;
cada lector va a su ritmo, y si se cae, cuando vuelve sigue donde lo dejó.

Analogía: no obligas al panadero a esperar en la puerta de tu casa con el pan. Deja
el pan en la estantería de la panadería y tú lo recoges cuando puedes. La estantería
no se vacía porque tú lo cojas.

---

## 2. La idea central: una cinta de casillas que no se borra

Un **topic** es un fichero que solo crece, con los mensajes puestos uno detrás de
otro. Cada mensaje recibe un número al entrar (su **offset**). Leer no borra nada:
si dos lectores distintos leen el mismo topic, los dos ven los mismos mensajes.
Los mensajes se borran por **antigüedad o tamaño** (la *retention*, 7 días por
defecto), nunca porque alguien los haya leído.

Un topic se corta en varios carriles paralelos, las **particiones**:

```
        market.ticks.raw  (topic, 6 particiones = 6 carriles)

 carril 0:  [0][1][2][3][4][5][6]........      <- aquí caen ciertos símbolos
 carril 1:  [0][1][2][3]..............
 carril 2:  [0][1][2][3][4][5][6][7][8]
 carril 3:  [0][1][2][3][4][5]........
 carril 4:  [0][1][2][3]..............
 carril 5:  [0][1][2][3][4][5][6][7]..

            ^
            el número de casilla es el OFFSET
```

¿Por qué partir en carriles? Por dos motivos:

1. **Velocidad**: varios programas pueden escribir y leer a la vez, uno por carril.
2. **Escala**: el trabajo se reparte entre lectores (lo veremos con los grupos).

El precio de partir: el orden **solo se garantiza dentro de un carril**. Entre el
carril 2 y el 5, no hay ningún orden. Esto es la clave de todo lo demás.

---

## 3. Los conceptos, uno a uno

### Topic
- **Idea**: el canal con nombre por el que viajan los mensajes.
- **Analogía**: la estantería de la panadería, con su cartel.
- **En Aggora**: `market.ticks.raw` (precios en bruto). Lo crea el simulador al
  arrancar, declarado en `KafkaTopicsConfig.java`. En fases siguientes habrá más
  (`market.ticks.canonical`, `orders.incoming`, `alerts.raised`...).

### Partición
- **Idea**: cada uno de los carriles en que se corta el topic.
- **Analogía**: las cajas de un supermercado atendiendo en paralelo.
- **En Aggora**: 6 en `market.ticks.raw`. Se eligen al crear el topic y **no se
  pueden reducir** después (subir sí, pero cambia el reparto de las keys).
- **Truco de lectura**: `part=5 offset=1785` significa "carril 5, casilla 1785".

### Offset
- **Idea**: el número de casilla de un mensaje dentro de su partición.
- **Analogía**: el número de ticket.
- **En Aggora**: lo ves en cada línea de log del consumidor (`offset=1785`).
- **Ojo**: el offset identifica un mensaje *dentro de una partición*. "Offset 1785"
  sin decir el carril no significa nada.

### Key
- **Idea**: el dato que decide a qué carril va cada mensaje (Kafka calcula un hash
  de la key y lo reparte).
- **Analogía**: el código postal que decide a qué centro de reparto va tu paquete.
- **En Aggora**: la key es el **símbolo**. Por eso todos los ticks de AAPL caen
  siempre en el mismo carril, y se leen **en orden**.
- **Por qué importa**: los precios hay que leerlos en orden. Si un precio de las
  10:00 se procesara después del de las 10:01, tu media móvil sería basura. Con
  key = símbolo, el orden está garantizado por instrumento.
- **Contrapartida real que vimos**: con 12 símbolos y 6 carriles, el hash no cae
  repartido al 50%. En nuestra prueba, el carril 0 se quedó vacío. El reparto hay
  que mirarlo, no suponerlo.

### Productor
- **Idea**: el programa que escribe en el topic.
- **Analogía**: quien deja el pan en la estantería.
- **En Aggora**: `TickProducer.java`.
- **Envío asíncrono**: manda el mensaje y sigue a lo suyo; la respuesta llega
  después (un *callback*). Si esperase a cada confirmación, iría a un mensaje por
  ida y vuelta de red.
- **Idempotencia**: con `enable.idempotence: true`, si hay un fallo de red y el
  productor reintenta, Kafka descarta el duplicado. Sin esto, un reintento puede
  escribir el mismo tick dos veces.

### Consumidor
- **Idea**: el programa que lee de un topic.
- **Analogía**: quien recoge el pan.
- **En Aggora**: `TickConsumer.java`, que además valida y normaliza.

### Consumer group
- **Idea**: el "equipo" de consumidores que se reparten los carriles. **Un carril
  solo lo lee un miembro del grupo a la vez**; así nadie procesa lo mismo dos veces.
- **Analogía**: varios repartidores con la misma ruta asignada: se reparten los
  barrios, y ningún barrio lo hace dos personas.
- **En Aggora**: el grupo se llama `ingestion-normalizer` (está en
  `application.yml`).
- **Consecuencias que hay que tener claras**:
  - 6 carriles y 1 instancia → esa instancia lee los 6.
  - 6 carriles y 2 instancias → 3 y 3 (es lo que vimos: "3/3").
  - 6 carriles y **8** instancias → 2 instancias se quedan **sin nada que hacer**.
    El paralelismo máximo es el número de particiones.
  - Dos grupos distintos (por ejemplo `ingestion-normalizer` y `audit-log`) leen
    **los mismos** mensajes, cada uno a su ritmo: eso es el fan-out.

### Rebalanceo
- **Idea**: el reparto de carriles se rehace cuando entra o sale un miembro del
  grupo (o cambian las particiones).
- **Analogía**: llega un repartidor nuevo y el jefe reparte los barrios otra vez.
- **En Aggora**: lo cuenta `RebalanceLogger.java`. Al arrancar la segunda instancia
  vimos en la primera: `REVOCADAS 6 particiones` → `ASIGNADAS 3`.
- **Detalle útil de la vida real**: si matas una instancia de golpe (`kill -9`), el
  grupo no se entera hasta que pasa el *timeout* de sesión (~45 s por defecto) y el
  rebalanceo tarda. Si la cierras bien (`kill`, Ctrl-C), el consumidor se despide y
  el rebalanceo es inmediato. Nos pasó: 45 segundos de espera.

### Commit del offset (y "at-least-once")
- **Idea**: el consumidor apunta en qué casilla va, para no releer ni perderse.
  Ese apunte es el *commit*.
- **Analogía**: el **marcapáginas** del libro. No hace falta recordar la página: la
  tienes marcada.
- **En Aggora**: **commit manual**. El código llama a `ack.acknowledge()` **después**
  de procesar el mensaje (`TickConsumer.java`), no antes.
- **Qué pasa si se cae en medio**: al reiniciar, vuelve al marcapáginas y **repite**
  lo que había leído sin apuntar. Eso es *at-least-once*: **puede repetir, no puede
  perder**.
- **La alternativa** (auto-commit) apunta antes de procesar: si se cae, ese mensaje
  se da por hecho y **se pierde**.
- **Cuánto se repite**: depende de cada cuánto apuntes. Apuntando tras cada mensaje,
  como máximo 1. Apuntando por lotes de 10, hasta 9. Apuntando cada 100, hasta 99.
  Más apuntes = menos duplicados, pero más escrituras en el broker. Es un
  compromiso, no una verdad revelada.
- **Por eso el consumidor tiene que aguantar repetidos**: en la Fase 5, cuando
  `portfolio-risk` sume posiciones a partir de las ejecuciones, un mensaje repetido
  no puede sumar dos veces. Se resuelve con operaciones idempotentes o claves
  únicas. (Todavía no lo hemos hecho: es trabajo de fases siguientes.)

### Lag
- **Idea**: cuántas casillas lleva el lector de retraso respecto a lo escrito.
- **Analogía**: los libros que te quedan por leer de la pila.
- **En Aggora**: `kafka-consumer-groups.sh --describe` te da `LOG-END-OFFSET`
  (último escrito) menos `CURRENT-OFFSET` (marcapáginas) = `LAG`.
- **Para qué sirve**: es **la** métrica de salud. Si el lag crece sin parar, el
  consumidor no da abasto (o está caído). En la Fase 6 esto va a un panel de
  Grafana. Un lag que sube y baja es normal; uno que sube siempre, no.

### Réplicas y `min.insync.replicas` (avance)
- **Idea**: cada partición puede estar copiada en varios brokers. Si uno se cae,
  otro tiene la copia.
- **En Aggora**: hoy `replicas=1` porque tienes **un solo broker** en dev (una copia
  no se puede repartir). En la Fase 6 levantamos un clúster de 3 y esto se convierte
  en un experimento real: tirar un broker y ver qué pasa.

---

## 4. El viaje de un tick, de principio a fin

Con el código que ya está escrito:

1. Arranca `ReferenceFeed` y, si hay `TWELVEDATA_API_KEY`, cada 15 minutos pide a
   Twelve Data el precio real de los instrumentos marcados con `real-feed: true`.
2. Con el precio real actualiza la **referencia** de ese instrumento y emite **un**
   tick marcado `source: REFERENCE` (size 0: es una cotización, no una operación).
3. Mientras tanto, `TickEngine` emite cada 200 ms un tick **sintético** por cada
   instrumento cuyo mercado esté abierto, usando `PriceWalk`: un paseo aleatorio
   que tiende hacia la referencia real y se recorta a ±1,5% de ella. Así los ticks
   sintéticos jamás se despegan del precio verdadero.
4. `TickProducer` escribe cada tick en `market.ticks.raw` con **key = símbolo**.
   Kafka decide el carril con el hash de la key. Fin del lado productor; ni se
   entera de quién lee.
5. `TickConsumer` lee, valida (precio positivo, divisa ISO real, key coherente con
   el símbolo, fecha no futura), normaliza a UTC y **apunta el marcapáginas**.
   En la Fase 1 solo lo registra por log; en la Fase 2 lo republicará a
   `market.ticks.canonical` con un esquema Avro.

Un mensaje real del topic (recortado), que es lo que verás tú:

```json
XAU/USD	{"eventId":"d01ea957-...","symbol":"XAU/USD","assetClass":"COMMODITY",
"exchange":"COMMODITY","currency":"USD","price":4277.5259,"size":0,
"eventTime":1789395320.744403168,"source":"REFERENCE","sequence":32}
```

Fíjate en dos campos:

- **`source`**: `REFERENCE` (precio real que acaba de llegar) o `SYNTHETIC`
  (interpolado entre dos referencias). El spec lo pide para poder ser honestos con
  qué es real y qué no: ahora mismo los 12 instrumentos reciben precio real, pero de
  **dos proveedores distintos** — Twelve Data para EEUU, forex y oro, y Alpha Vantage
  para Euronext París y Shanghai, porque ningún plan gratuito cubre los tres husos
  horarios. El crudo spot no está gratis en ninguno, así que va con el ETF `USO`.
- **`eventTime`**: es un número (`1789395320.744403168`), segundos desde 1970.
  No es legible y, sobre todo, **nada en el mensaje dice que sea una fecha**. Cada
  servicio tiene que adivinarlo. Ese es el problema que resuelve la Fase 2: un
  contrato (el esquema) donde los tipos están declarados.

---

## 5. Malentendidos típicos (todos nos los hemos creído)

- **"Leer borra el mensaje"** — No. Kafka no es una cola que vacía; es un fichero
  que crece. Lo que se borra es por antigüedad/tamaño, no por lectura.
- **"Los mensajes están ordenados"** — Solo dentro de una partición. Si necesitas
  orden por instrumento, la key tiene que ser el instrumento (y por eso la key
  importa).
- **"Meto 10 consumidores y voy 10 veces más rápido"** — Solo hasta el número de
  particiones. Con 6 carriles, el séptimo consumidor mira.
- **"El offset es del mensaje"** — Es del *grupo*: cada grupo lleva su propio
  marcapáginas por partición. Otro grupo puede ir por la casilla 3 mientras tú vas
  por la 9.000.
- **"Kafka garantiza que no se procesa dos veces"** — No por defecto. Con commit
  manual tienes *at-least-once*: repetidos posibles. Para no repetir hay que usar
  transacciones (Fase 4) o hacer el procesamiento idempotente.
- **"Si el consumidor se cae, se pierden mensajes"** — Solo si confirmas antes de
  procesar. Con commit manual, como el nuestro, se repiten, no se pierden.

---

## 6. Tour: verlo con tus propios ojos

Todo desde el devcontainer. Necesitas Kafka arriba (`docker compose -f infra/docker-compose.yml up -d`
desde el host). Los comandos de la CLI de Kafka van con `docker exec aggora-kafka ...`
porque es ahí donde están las herramientas.

```bash
# Paso 0 — compilar
cd /workspaces/aggora/services && mvn -q -DskipTests package

# Paso 1 — arrancar el productor (terminal 1) y mirar su log de arranque
cd market-data-simulator && java -jar target/market-data-simulator-0.1.0-SNAPSHOT.jar

# Paso 2 — comprobar el topic por dentro: 6 carriles
docker exec aggora-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --topic market.ticks.raw

# Paso 3 — ver mensajes crudos, con su key delante
docker exec aggora-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic market.ticks.raw \
  --max-messages 3 --property print.key=true

# Paso 4 — arrancar el consumidor (terminal 2) y leer los logs de rebalanceo
cd ingestion-normalizer && java -jar target/ingestion-normalizer-0.1.0-SNAPSHOT.jar

# Paso 5 — ver los marcapáginas y el lag de cada carril
docker exec aggora-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group ingestion-normalizer

# Paso 6 — rebalanceo: arranca una SEGUNDA instancia del consumidor (terminal 3)
#          y vuelve a lanzar el comando del paso 5: verás 3 carriles en cada una.

# Paso 7 — at-least-once: procesar lento y matar de golpe
AGGORA_PROCESSINGDELAYMS=400 SPRING_KAFKA_LISTENER_ACK_MODE=manual \
SPRING_KAFKA_CONSUMER_MAXPOLLRECORDS=10 \
  java -jar target/ingestion-normalizer-0.1.0-SNAPSHOT.jar
# en otra terminal: pkill -9 -f ingestion-normalizer
# y vuelve a arrancarlo: reanuda en el último offset confirmado y REPITE lo leído.
```

---

## 7. Chuleta

| Palabra | Traducción de andar por casa |
|---|---|
| Topic | la cinta / la estantería con nombre |
| Partición | uno de los carriles paralelos de la cinta |
| Offset | número de casilla dentro de un carril |
| Key | lo que decide en qué carril cae el mensaje |
| Productor | quien escribe |
| Consumidor | quien lee |
| Consumer group | equipo de lectores que se reparten carriles |
| Rebalanceo | volver a repartir los carriles al entrar/salir alguien |
| Commit | el marcapáginas del lector |
| Lag | cuánto va el lector por detrás de lo escrito |
| at-least-once | puede repetir, no puede perder |
| Réplica | copia de una partición en otro broker |

---

## 8. Dónde se practica cada concepto

| Concepto | Fase |
|---|---|
| Topics, particiones, keys, productor, consumidor, offsets, commit manual | **1 ✅** |
| Schema Registry, Avro, evolución de esquemas | 2 |
| Kafka Streams: KStream/KTable, ventanas, joins, state stores | 3 |
| Exactly-once con transacciones | 4 |
| KTable de posiciones, alertas por ventana, outbox transaccional | 5 |
| Dead-letter topics, reintentos, réplicas, `min.insync.replicas`, Grafana | 6 |
| Evolución de esquema rota a propósito | 7 |
| Todo otra vez en Quarkus + native image | 8 |
| Informe comparativo | 9 |

---

## 9. La Fase 2 en la práctica: el contrato deja de ser adivinanza

En la Fase 1 cada servicio adivinaba qué era cada campo del JSON. En la Fase 2 el
contrato está **declarado y registrado**, y eso cambia cuatro cosas visibles:

1. **Los mensajes ya no son legibles a ojo.** Un mensaje empieza ahora con
   `[1 byte 0x00][4 bytes de ID de esquema]` y sigue con datos binarios. Al mirar el
   topic con `kafka-console-consumer` verás caracteres raros: es lo normal. Para
   leerlo hay que tener el esquema, y para eso está el registry.
2. **El registry es la fuente de verdad.** `curl localhost:8081/subjects` lista los
   contratos, `/subjects/<topic>-value/versions` da sus versiones e IDs, y
   `/config` dice el modo de compatibilidad (en el nuestro, `BACKWARD`).
3. **Los tipos están declarados.** El precio viaja como decimal (nunca coma
   flotante: en dinero 0.1 + 0.2 no es 0.3) y la fecha como `timestamp-millis`, que
   es lo que permite que el consumidor reciba un `Instant` de verdad en lugar del
   número opaco de la Fase 1.
4. **Un esquema nuevo se valida contra el anterior.** Si alguien cambia el contrato
   de forma que rompa a los consumidores, el registry **rechaza el registro** y el
   fallo aparece al desplegar, no tres servicios más adelante.

Y el matiz que más cuesta interiorizar: **el mismo dato puede tener dos contratos
distintos**. El normalizer lee tick crudo (subject `market.ticks.raw-value`) y
publica evento canónico (subject `market.ticks.canonical-value`). Son dos subjects
independientes: el canónico puede cambiar sin tocar el crudo y viceversa. Esa es la
razón de que el normalizer construya un objeto nuevo en vez de reenviar el que leyó.

---

## 10. La Fase 3 en la práctica: ventanas, estado y joins

Hasta ahora cada mensaje se procesaba por separado. La Fase 3 va de calculos que
necesitan **recordar** cosas: una media movil no existe sin memoria.

**KStream y KTable no son lo mismo.** Un `KStream` es una secuencia de hechos: dos
ticks de AAPL son dos hechos. Una `KTable` es una tabla que se actualiza: de esos dos
ticks se queda con el ultimo valor por clave. Uno es la pelicula; la otra, la foto.

**Una ventana es "de que trozo de tiempo quiero los datos".** Tres tipos:

| Tipo | Como es | Para que |
|---|---|---|
| **Tumbling** | bloques fijos que no se solapan (10:00:00-10:00:30, luego 10:00:30-10:01:00) | una foto por bloque: el VWAP de cada medio minuto |
| **Hopping** | bloques que se solapan (ventana de 60 s recalculada cada 15 s) | una media que se refresca sin esperar al cierre |
| **Session** | bloques separados por huecos de inactividad | sesiones de un usuario; en mercado continuo se usa poco |

El **grace** es el margen que se espera antes de dar una ventana por cerrada: los datos
pueden llegar desordenados o tarde, y ese margen decide cuanto se les espera.

**El estado vive en un state store.** Kafka Streams guarda lo que necesita recordar
(sumas, volumen, ultimo precio) en disco con RocksDB y mantiene una copia en un topic
interno de Kafka, el **changelog**. Por eso un reinicio no pierde la cuenta: si el
estado local desaparece, se reconstruye desde el changelog. Esos topics se crean solos
(`analytics-streams-metrics-tumbling-store-changelog`), y son la razon de que una
agregacion con ventana sobreviva a un despliegue.

**Los dos joins que se practican aqui:**

1. **Stream-stream**: cruzar las dos cotizaciones de la misma empresa. Los dos precios
   no llegan en el mismo milisegundo, asi que el join lleva **ventana de tiempo**. Para
   que funcione, las dos patas tienen que caer en la MISMA particion: se re-clavan por
   el simbolo raiz (`ASML`) y Kafka Streams inserta solo el topic de reparticion.
2. **Stream-table (global)**: convertir el precio europeo a dolares con el ultimo tipo
   de cambio. Una **GlobalKTable** es una copia entera de la tabla en cada instancia
   (aqui son dos pares de divisas, cabe de sobra), asi que **no** hace falta que la
   clave del stream coincida con la de la tabla: la clave de busqueda se calcula del
   propio registro. Es el patron para enriquecer con datos de referencia.

El tipo de cambio se guarda en `market.fx.reference`, un topic **compactado**: a quien
lo lee le interesa el ultimo valor de cada par, no el historial, y Kafka se queda con
una entrada por clave.

**Consultas interactivas**: como el estado esta en disco y en memoria, se le puede
preguntar a la aplicacion en marcha por el sin pasar por Kafka:

```bash
curl -s "localhost:8085/analytics?symbol=EUR/USD&minutes=3"
```

Devuelve las ultimas ventanas con su VWAP, media y volatilidad, calculadas de verdad.
El limite: cada instancia solo conoce SUS particiones, asi que con varios despliegues
habria que preguntar a la que tiene la clave (o a todas).

**Un detalle de mercado de verdad**: el spread de ASML solo existe cuando **los dos
mercados estan abiertos a la vez** (13:30-15:30 UTC, el solape de NASDAQ y Euronext).
Fuera de esa franja, una de las dos patas no cotiza y no hay nada que cruzar. Que el
sistema no invente un spread a partir de un precio de hace horas es la respuesta
correcta, no un fallo.

---

## 11. La Fase 4 en la práctica: exactamente una vez

Hasta aqui, el consumidor tenia **at-least-once**: puede repetir, no puede perder (el
marcapaginas se apunta despues de procesar). Para una ejecucion de bolsa eso no vale:
una ejecucion repetida es una posicion contada dos veces. La Fase 4 monta
**exactly-once**.

**El problema, en concreto.** El motor de matching tiene que hacer dos cosas que no son
la misma: (1) publicar la ejecucion en `orders.executions` y (2) apuntar "ya procese
esta orden". Si publica y se cae antes de apuntar, al reiniciar cruza la orden otra vez
y salen **dos ejecuciones de la misma operacion**. Si apunta antes de publicar y se cae,
pierde la ejecucion. Con dos pasos separados no hay forma de acertar siempre.

**La solucion: una transaccion.** Kafka permite meter la publicacion y el commit del
offset en la MISMA transaccion. Entonces ya no son dos pasos: o se confirman los dos, o
no se confirma ninguno. Eso es exactly-once.

Las tres piezas que hay que montar (y las tres estan en `application.yml` y en
`KafkaTransactionConfig`):

1. **Productor transaccional**: `transaction-id-prefix` en el productor. Eso le da un
   `transactional.id`, que es lo que permite a Kafka reconocer sus transacciones y
   limpiar las que quedaron a medias si el proceso murio.
2. **Gestor de transacciones en el contenedor de escucha**: Spring Kafka abre una
   transaccion por cada poll, el listener publica dentro de ella y, al terminar, los
   offsets se confirman **dentro de la misma transaccion**.
3. **Consumidor con `isolation.level=read_committed`**: no me ensenes nada que venga de
   una transaccion sin confirmar.

**La prueba que lo demuestra** (esta hecha de verdad en el proyecto):

```
read_committed   -> 121 mensajes
read_uncommitted -> 168 mensajes
```

Los 47 de diferencia son ejecuciones **abortadas** (se inyecto un fallo cada 10 ordenes
despues de publicar). Estan fisicamente en el log, ocupan sitio, y **un consumidor
`read_committed` no las ve nunca**. Esa es toda la magia: el dato abortado existe pero
no cuenta.

**Una trampa que cuesta cara**: el manejador de errores por defecto de Spring Kafka,
cuando un mensaje falla repetidamente, **lo descarta** (confirma el offset y sigue). Con
transacciones, eso es perder la orden en silencio. Aqui se configura para que relance la
excepcion y la transaccion se deshaga, de modo que el mensaje siga pendiente. En
produccion, el destino de un mensaje que no se puede procesar es un topic de descartes
con su aviso (Fase 6), no el olvido.

**Idempotencia no es lo mismo que transacciones.** El productor idempotente (Fase 1)
evita duplicados de UN productor reintentando UN mensaje. La transaccion evita
duplicados entre VARIOS mensajes y offsets, y sobrevive a que el proceso se caiga en
medio. Son complementarias: la transaccion necesita idempotencia por debajo.

