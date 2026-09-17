# El dashboard: contrato de datos y como se publica

Este documento es el contrato entre **este repositorio** (el backend) y el **dashboard** (una app
Angular 20 que vive en otro repositorio y que aqui no se toca). Todo lo que el dashboard necesita
saber esta aqui; la version corta y copiable para el otro repo es
[`docs/CONTRACT.md`](CONTRACT.md).

La regla de oro: **el dashboard es de solo lectura**. No escribe nada en el sistema. Lo unico que
hace es abrir un WebSocket, pedir paneles de metricas y leer la consulta interactiva. Los tres
endpoints son GET o WebSocket; no hay ni un POST.

```
   Kafka ──┐
           ├─► gateway-ws (Spring 8089 / Quarkus 8189) ──► WebSocket  (precios, cartera, alertas)
           │                                          └─► /api/metrics (proxy de Prometheus)
Prometheus ┘

   analytics-streams (8085 / 8185) ──► /analytics?symbol=...   (consulta interactiva)
   market-data-simulator   (8080)  ──► /actuator/health        (estado)
```

---

## 1. El contrato del WebSocket

Un unico endpoint por gateway: `ws://<host>:8089/ws` (Spring) y `ws://<host>:8189/ws` (Quarkus).
Sin STOMP, sin subprotocolo: **WebSocket a pelo** y un JSON por frame.

Hay **tres tipos de mensaje**, y los tres llevan el mismo sobre:

| Campo | Tipo | Significado |
|---|---|---|
| `v` | numero | version del contrato. Hoy **1**. Si cambia la forma, sube. |
| `kind` | texto | `snapshot`, `position` o `alert`. |
| `stack` | texto | `spring` o `quarkus`: quien manda el mensaje. |
| `ts` | texto | hora del servidor en ISO-8601 UTC (`2026-09-17T08:00:00.123Z`). |

### 1.1 `snapshot`: el resumen, una vez por segundo

Los ticks **no** se mandan uno a uno. El gateway se queda con el **ultimo tick de cada simbolo** y
manda una foto por segundo. De ~84 ticks/s (y hasta 4.000/s en el test de estres) la pagina pasa a
recibir un mensaje por segundo. Las posiciones y las alertas, que son pocas, se siguen mandando al
momento.

```json
{"v":1,"kind":"snapshot","stack":"spring","ts":"2026-09-17T08:00:00.123Z",
 "ticksIn":84,"ticksOut":12,
 "symbols":{
   "EUR/USD":{"price":"1.0875","currency":"USD","size":100,"source":"REFERENCE","at":"2026-09-17T08:00:00.100Z"},
   "AAPL":{"price":"333.47","currency":"USD","size":12,"source":"SYNTHETIC","at":"2026-09-17T08:00:00.080Z"}
 }}
```

- `ticksIn`: ticks consumidos de Kafka en el ultimo segundo.
- `ticksOut`: simbolos que salen en esta foto (uno por simbolo). La diferencia entre los dos es lo
  que el resumen se ha ahorrado.
- `symbols`: objeto **clave = simbolo**; se queda con el **ultimo** valor, no con una lista. Un
  simbolo que deja de cotizar mantiene su ultimo precio.
- `price` viaja como **texto** (es un `decimal` de Avro: en dinero no se usa coma flotante).

### 1.2 `position`: al momento

```json
{"v":1,"kind":"position","stack":"spring","ts":"...","account":"ACC-1","symbol":"AAPL",
 "quantity":100,"averageCost":"333.47","realizedPnl":"0","exposure":"33347","currency":"USD","marginBreach":false}
```

`realizedPnl` y `exposure` solo cambian al ejecutar; y `marginBreach` es el aviso de margen. Se
manda cada vez que se actualiza una posicion, sin esperar al segundo.

### 1.3 `alert`: al momento

```json
{"v":1,"kind":"alert","stack":"spring","ts":"...","severity":"CRITICAL","type":"VOLATILITY_SPIKE",
 "subject":"ASML","detail":"...","value":"0.0123","raisedAt":"..."}
```

`severity` es `CRITICAL`, `WARNING` o `INFO`.

### 1.4 Lo que el WebSocket NO promete

Es un **fan-out**, no un log: no hay commit manual ni reenvio. Si un cliente esta lento o se cae,
pierde los mensajes de ese rato y se corrige con el siguiente snapshot. **El dashboard no debe
guardar estado critico ni calcular nada que no pueda rehacer en el siguiente segundo.**

---

## 2. El catalogo de paneles (`/api/metrics`)

```
GET http://<host>:8089/api/metrics?panel=<nombre>     (Spring)
GET http://<host>:8189/api/metrics?panel=<nombre>     (Quarkus)
```

Es un **proxy de Prometheus con catalogo cerrado**: el panel lo elige el backend y no se acepta
PromQL libre (nada de `?query=`). Asi la pagina no puede pedirle a Prometheus cualquier cosa ni
inventarse una consulta que tumbe el servidor.

Forma de la respuesta:

```json
{"panel":"lag","ts":"2026-09-17T08:00:00.123Z","stack":"spring",
 "series":[{"label":"ingestion-normalizer","points":[[1758096000,0]]}]}
```

- `label`: nombre legible de la serie (grupo, topic/particion, target...).
- `points`: lista de `[segundos epoch, valor]`. Hoy cada serie trae **un punto** (consulta
  instantanea); la forma ya admite varios para cuando interese una grafica con historia.
- `nota`: solo aparece cuando no hay datos, explicando por que. Nunca se inventa una serie.

### 2.1 Los paneles

| Panel | Que se ve |
|---|---|
| `pulso` | ticks/s que **entran** en `market.ticks.raw` y que **salen** de `market.ticks.canonical` y `.q` |
| `lag` | retraso de los 4 grupos de trabajo: `ingestion-normalizer`, `ingestion-normalizer-q`, `analytics-streams`, `analytics-streams-q` |
| `particiones` | offset actual de cada particion de `market.ticks.raw`, `market.ticks.canonical(.q)` y `market.analytics(.q)`: el log avanzando |
| `transacciones` | confirmadas frente a abortadas. **No hay serie**: sale `series: []` y una `nota` |
| `descartes` | offset de `market.ticks.raw.DLT` y `.DLT.q` y de los topics de reintento (`*.retry-*`) |
| `salud` | `up` de los targets, `aggora_kafka_streams_running` por stack y particiones under-replicated |
| `comparativa` | las dos series (Spring y Quarkus) del panel que se pida en `de` |

### 2.2 Las consultas, literales

Estas son las consultas exactas del catalogo, contra las metricas que publica **este** Prometheus
(los nombres salen de `curl 'localhost:9090/api/v1/label/__name__/values'`, no se inventan):

```
pulso
  entrada          sum(rate(kafka_topic_partition_current_offset{topic="market.ticks.raw"}[1m]))
  salida-spring    sum(rate(kafka_topic_partition_current_offset{topic="market.ticks.canonical"}[1m]))
  salida-quarkus   sum(rate(kafka_topic_partition_current_offset{topic="market.ticks.canonical.q"}[1m]))

lag
  sum by (consumergroup) (kafka_consumergroup_lag{consumergroup=~"ingestion-normalizer|ingestion-normalizer-q|analytics-streams|analytics-streams-q"})

particiones
  sum by (topic, partition) (kafka_topic_partition_current_offset{topic=~"market.ticks.raw|market.ticks.canonical|market.ticks.canonical.q|market.analytics|market.analytics.q"})

descartes
  sum by (topic, partition) (kafka_topic_partition_current_offset{topic=~"market.ticks.raw.DLT|market.ticks.raw.DLT.q|.*\.retry-.*"})

salud
  up
  sum by (stack, service) (aggora_kafka_streams_running)
  sum by (topic) (kafka_topic_partition_under_replicated_partition)

comparativa (con de=<panel>)
  pulso        -> canonical (spring) y canonical.q (quarkus)
  lag          -> ingestion-normalizer / -q y analytics-streams / -q
  particiones  -> canonical y analytics, cada uno con su .q
  descartes    -> DLT y DLT.q
  salud        -> aggora_kafka_streams_running por stack
  transacciones-> sin series (misma nota)
```

### 2.3 De donde sale cada dato

| Panel | Metrica | Quien la publica |
|---|---|---|
| `pulso` | `kafka_topic_partition_current_offset` + `rate()` | kafka-exporter |
| `lag` | `kafka_consumergroup_lag` | kafka-exporter |
| `particiones` | `kafka_topic_partition_current_offset` | kafka-exporter |
| `descartes` | `kafka_topic_partition_current_offset` de los topics DLT/reintento | kafka-exporter |
| `salud` (targets) | `up` | Prometheus |
| `salud` (motores) | `aggora_kafka_streams_running` | la propia aplicacion (Micrometer) |
| `salud` (ISR) | `kafka_topic_partition_under_replicated_partition` | kafka-exporter |
| `transacciones` | — | **no existe** (ver abajo) |

Todo se lee de Prometheus, **no** de la pagina. Prometheus no habla el protocolo de Kafka: quien
traduce grupos, offsets y lag es `kafka-exporter`. Las metricas de la aplicacion (JVM, Kafka
Streams, sonda) las publica cada servicio en `/actuator/prometheus` (Spring) o `/q/metrics`
(Quarkus) y Prometheus las scrapea.

**El agujero que hay que decir en voz alta:** no hay ninguna metrica de confirmadas frente a
abortadas. kafka-exporter solo expone offsets, lag, grupos, ISR y poco mas, y las unicas series de
transacciones son `kafka_producer_txn_*_time_ns_total` del cliente, que son **tiempos** (y valen 0
en este entorno), no un recuento. Por eso `transacciones` devuelve `series: []` con una `nota`: es
preferible un panel vacio y explicado que un numero inventado. La semantica exactly-once se
comprueba con `scripts/ExactlyOnceRaceCheck.java` y se vigila por el lag de `orders.executions`.

Otras metricas que existen y **no** se usan todavia: `kafka_topic_partition_in_sync_replica`
(cuanta gente tiene el dato), `kafka_consumergroup_members` (miembros por grupo),
`kafka_stream_thread_thread_state`, `kafka_stream_task_*`, `jvm_*`, `http_server_*`.

### 2.4 Errores

| Codigo | Cuando | Cuerpo |
|---|---|---|
| 400 | falta `panel` o no esta en el catalogo | `{"error":"panel desconocido: X","detalle":["pulso","lag",...]}` |
| 400 | `comparativa` con un `de` que no soporta | `{"error":"comparativa no soporta de=X","detalle":[...]}` |
| 502 | Prometheus no responde o contesta error | `{"error":"prometheus no responde","detalle":"..."}` |

---

## 3. Los paneles propuestos (que esta viendo, en una frase)

| Panel | Frase para la pantalla | Como se lee |
|---|---|---|
| Pulso | "El mercado entrando y saliendo" | Dos lineas que suben y bajan juntas. Si la entrada sube y la salida no, el normalizer se ha quedado atras |
| Lag | "Cuanto le falta a cada consumidor" | Cerca de 0 y a dientes de sierra = sano. Subiendo sin volver = alguien no da abasto |
| Particiones | "El log avanzando" | Cada particion es una linea que sube. Una linea plana = esa particion no recibe |
| Descartes | "Lo que no se pudo procesar" | Plano en verde. Cada escalon es un mensaje al DLT: hay que mirar el motivo en su cabecera |
| Salud | "Quien esta vivo y quien solo lo parece" | Los targets en verde, los motores de Streams a 1 y las under-replicated a 0 |
| Comparativa | "Spring y Quarkus, lado a lado" | Las dos series del mismo panel. Misma forma = mismo comportamiento |
| Transacciones | "Exactly-once, honestamente" | Sale vacio con su nota: lo que se vigila de verdad es el lag |

---

## 4. Las cinco lecciones

Cada script **provoca algo**, termina diciendo que panel mirar y que deberia verse, y deja el
sistema como estaba. Los cinco corren dentro del devcontainer.

### Leccion 1 — se cae un broker

```bash
bash scripts/leccion-1-broker-caido.sh          # kafka-3 por defecto
```

Para el broker con aviso, ensena el ISR y el lider cambiando, y lo vuelve a levantar. **Panel
`salud`**: las particiones under-replicated pasan de 0 a N y vuelven a 0. **Panel `lag`**: no se
mueve, porque con 3 replicas y `min.insync.replicas=2` se sigue leyendo y escribiendo con una
copia menos. Eso es lo que compra tener replicas.

### Leccion 2 — el veneno acaba en el DLT

```bash
bash scripts/leccion-2-veneno-dlt.sh
```

Manda a `market.ticks.raw` los **dos venenos** y comprueba que los dos acaban en los dos DLT:

1. un tick que es **Avro perfecto** (mismo esquema del registro) pero invalido de contenido: precio
   0. El normalizer lo lee bien y lo rechaza al **validar**, asi que acaba en `market.ticks.raw.DLT`
   con la cabecera `x-dlt-reason: precio ausente o no positivo`;
2. bytes que **no son Avro**. Aqui el fallo es al **deserializar**, antes de que el codigo vea el
   mensaje: acaba igualmente en el DLT con `x-dlt-reason: fallo de deserializacion: ...`, en los dos
   stacks.

**Panel `descartes`**: el offset de `market.ticks.raw.DLT` y el de `.q` suben de uno en uno con cada
veneno. El script comprueba ademas que los dos normalizers siguen vivos y al dia: lag bajo, sin
rebalanceos nuevos y con los canonicos creciendo.

**El agujero que esto cierra, medido en vivo:** hasta la Fase 11 un mensaje que **no era Avro** no
llegaba al DLT, porque el deserializador falla *antes* que el codigo y el unico DLT que habia lo
escribia `TickConsumer`/`TickNormalizer` al **validar**. Las consecuencias se midieron: el normalizer
de Spring entraba en un bucle de reintentos que escribio **17,4 GB de log en ~6 minutos** y dejaba la
particion atascada; el de Quarkus revocaba las particiones y no volvia. Ya esta arreglado
(`ErrorHandlingDeserializer` + `DeadLetterPublishingRecoverer` en Spring; un
`DeserializationFailureHandler` en Quarkus; ver `docs/decisions.md` y el capitulo 13 de
`docs/kafka-101.md`).

Por si alguien ejecuta la leccion contra un jar viejo, el script vigila el tamano de los dos logs
mientras prueba: si uno pasa de 200 MB para el normalizer, imprime el procedimiento de recuperacion y
sale con error en vez de dejar que se repita el incidente.

### Leccion 3 — rebalanceo

```bash
bash scripts/leccion-3-rebalanceo.sh
```

Arranca una **segunda instancia** del normalizer con el **mismo `group.id`** (es lo que hace que
haya rebalanceo: con grupos distintos cada una leeria las 6 particiones enteras) y ensena el
reparto 6/0 -> 3/3. **Paneles `lag` y `particiones`**: un pico de lag de unos segundos durante el
rebalanceo, y los offsets que no se paran. Al final la segunda instancia se para y el grupo vuelve
a 6/0.

### Leccion 4 — exactly-once

```bash
bash scripts/leccion-4-exactly-once.sh
```

Envuelve `scripts/ExactlyOnceRaceCheck.java`, que ya demuestra contra el cluster de verdad que la
secuencia del test es determinista: la transaccion abortada no existe con `read_committed` y si
esta en el log con `read_uncommitted`. **Panel `transacciones`**: sale vacio con su nota (no hay
confirmadas/abortadas en Prometheus); el dato del dia a dia es el lag de `orders.executions`.

### Leccion 5 — el motor muerto con el proceso vivo

```bash
bash scripts/leccion-5-streams-muerto.sh
```

Rompe el checkpoint del GlobalKTable de la analitica (un offset que ya no existe en el topic
compactado `market.fx.reference`) y reinicia el servicio. El motor se queda en ERROR y el proceso
**sigue vivo**: `/actuator/health` a 503, `/analytics` a 503 y `aggora_kafka_streams_running` a 0.
**Paneles `salud` y `lag`**. La leccion no es "rompe cosas": es que un servicio no deberia decidir
suicidarse (quien levanta un proceso es el supervisor) y que quien avisa es la **sonda**. El trap
restaura el checkpoint y reinicia el servicio.

---

## 5. El chequeo ejecutable

```bash
bash scripts/dashboard-smoke.sh
```

Contra los dos stacks en marcha: `/api/metrics` con todos los paneles y la forma esperada en 8089 y
8189, el modo `comparativa`, que el catalogo sea cerrado (un panel inventado da 400), que el
WebSocket entregue un `snapshot` con `v`, `stack`, `symbols` y las tasas, que el CORS responda con
el origen permitido y que la pagina vieja se siga sirviendo. Sale con codigo != 0 si algo falla.
Usa `scripts/GatewayLiveCheck.java` para la parte de WebSocket.

---

## 6. Publicarlo en internet

El dashboard se va a publicar detras de TLS. Lo que hay que tener claro:

**1. `https://` obliga a `wss://`.** Una pagina servida por HTTPS no puede abrir un `ws://`: el
navegador lo bloquea como contenido mixto. Los dos gateways ya lo resuelven: si el esquema de
`server.forward-headers-strategy=framework` (Spring) o
`quarkus.http.proxy.proxy-address-forwarding=true` (Quarkus) no se pusiera, el servicio detras del
proxy no sabe que la peticion original venia por HTTPS. La pagina elige el esquema sola:

```js
const esquema = location.protocol === 'https:' ? 'wss://' : 'ws://';
const ws = new WebSocket(`${esquema}${location.host}/ws`);
```

**2. Poner TLS delante.** Dos formas, las dos sin tocar el codigo:

- **Cloudflare Tunnel** (`cloudflared`), que es lo comodo en una maquina sin IP publica ni puertos
  abiertos; ademas de TLS da el nombre y el WAF:

  ```yaml
  tunnel: aggora
  ingress:
    - hostname: aggora.midominio.com
      service: http://localhost:8089      # gateway Spring
    - hostname: aggora-q.midominio.com
      service: http://localhost:8189      # gateway Quarkus
    - service: http_status:404
  ```

  El WebSocket pasa solo: el tunel reenvia el upgrade. No hay que abrir puertos en el router.

- **Caddy con Let's Encrypt**, si hay IP publica; Caddy renueva el certificado solo:

  ```
  aggora.midominio.com {
      reverse_proxy localhost:8089
  }
  ```

**3. Anadir los origenes publicos a la allowlist.** El CORS de la API HTTP se queda en los origenes
de desarrollo hasta que se anade el dominio real. En Spring es
`aggora.ui.allowed-origins` (una lista separada por comas) y en Quarkus
`quarkus.http.cors.enabled=true` + `quarkus.http.cors.origins` (o la variable de entorno
`QUARKUS_HTTP_CORS_ORIGINS`). Nada de `*`: el comodin deja entrar a cualquiera a leer los datos.
(Ojo: en Quarkus 3.39.3 la forma clásica `quarkus.http.cors=true` ya **no** vale; el arranque
avisa de "Unrecognized configuration key" y el CORS no se aplica. Es `.enabled`.)

**4. Exponer solo un borde fino.** Lo que se publica son **los dos gateways** (WebSocket + catalogo
de metricas) y, como mucho, la lectura de la analitica; el resto del pipeline (Kafka, Prometheus,
Grafana, Postgres, los servicios de trabajo) se queda dentro. El `t4g.small` del sprint no aguanta
los dos stacks enteros: si hay que publicar de verdad, se publica **un** gateway y el otro stack se
apaga. Es la diferencia entre "el dashboard se cae" y "se cae el mercado".

**5. La allowlist no es autenticacion.** CORS lo aplica el navegador, no el servidor: cualquiera
con `curl` sigue leyendo los endpoints. Antes de publicar de verdad hace falta poner autenticacion
en el borde (Cloudflare Access, `basic_auth` de Caddy o el proxy que sea). Hoy no hay ninguna.

---

## 7. Que NO puede decir este dashboard

- **No es la fuente de verdad.** La verdad esta en Kafka (los topics) y en Prometheus (las series).
  La pagina solo lee una copia; si el gateway se cae, no hay dashboard, pero el pipeline sigue.
- **No escribe nada.** Todos los endpoints son de lectura. No hay ni un POST.
- **No puede reconstruir el pasado.** El `snapshot` es el **ultimo** valor por simbolo, no un
  historial; y el WebSocket es un fan-out: un cliente lento se pierde mensajes y se corrige con el
  siguiente snapshot. Si hace falta historia, se pide a Prometheus (que ya la guarda).
- **No puede prometer exactamente-una-vez en la pantalla.** El exactly-once es del motor de
  matching y de Kafka, no del navegador.
- **No dice nada de lo que no este en las series.** El panel `transacciones` esta vacio porque la
  metrica no existe: es un hueco del entorno, no una funcionalidad.
- **No tiene autenticacion.** Con la allowlist de CORS, el navegador respeta el limite; `curl` no.

---

## 8. Notas de implementacion (para quien toque el backend)

- El catalogo vive **duplicado** en los dos arboles (`MetricsController` en Spring,
  `MetricsResource` en Quarkus), como el resto de contratos del repo: cada implementacion tiene su
  copia a proposito. Si se anade un panel, se anade en los dos o el smoke se pone rojo.
- El cliente HTTP es el `RestClient` de Spring y el `HttpClient` **del JDK** en Quarkus: una llamada
  GET que devuelve JSON no justifica una dependencia nueva.
- El resumen por segundo usa un `ScheduledExecutorService` del JDK en las **dos**
  implementaciones: ni el planificador de Spring ni el de Quarkus (que ademas tiene un suelo de 1 s)
  hacen falta para un hilo que solo pinta una foto.
- **Cuidado con los comentarios en `.properties`**: un `#` de media linea **no** es un comentario y
  pasa a formar parte del valor. Le paso al normalizer de Quarkus con
  `...schema.registry.url=http://schema-registry:8081  # en test...` y solo se ve al arrancar el
  **jar empaquetado** (perfil `prod`), no en los tests. Ver `docs/decisions.md`.
