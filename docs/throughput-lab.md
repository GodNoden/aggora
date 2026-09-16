# Test de estrés de throughput (Fase 10)

Este documento es el manual y el informe del test de estrés: qué se mide, cómo se ejecuta, qué
salió y —lo más importante— **qué destapó que no era la pregunta**. Los conceptos están en los
capítulos 18, 19 y 21 de la [guía de Kafka](kafka-101.md).

```bash
# Dentro del devcontainer, con los dos stacks en marcha
bash scripts/throughput-test.sh                 # 60 s por tasa, tasas 300 / 1500 / 5000
bash scripts/throughput-test.sh 20 1500 4000    # ráfagas de 20 s (lo que se usó para este informe)
```

---

## 1. La pregunta, y por qué no vale `kafka-producer-perf-test`

Hasta aquí la comparación entre Spring y Quarkus era **empate**: los dos pipelines procesaban los
mismos ~84 msg/s del simulador y daban los mismos 84 msg/s de salida. Eso no distingue nada,
porque a esa escala el cuello de botella es la **entrada**. La pregunta del test es:

> Si le meto carga de verdad, ¿aguantan los dos? ¿Y a qué coste?

La herramienta obvia sería `kafka-producer-perf-test`, pero **no sirve aquí**: manda bytes
arbitrarios y detrás de `market.ticks.raw` hay un esquema Avro y un validador. Un byte cualquiera
no es un tick: acabaría en el topic de descartes y lo que se mediría sería el camino del DLT.
Por eso el generador (`scripts/AvroLoadGenerator.java`) usa **el mismo serializador de Confluent
que los servicios** y construye ticks que pasan el validador (key == symbol, precio positivo,
divisa ISO-4217, timestamp de ahora). Y el propio experimento **comprueba** que la medición es
válida: si `market.ticks.raw.DLT` crece, el resultado se marca como no válido.

## 2. Cómo se mide

- **La misma entrada para los dos**: el generador escribe en `market.ticks.raw` y los dos stacks lo
  leen con grupos de consumo distintos, así que procesan exactamente los mismos mensajes, a la vez,
  en la misma máquina.
- **El simulador se para** durante el experimento: si sigue produciendo, la tasa de entrada es
  desconocida y el número no vale. El script lo para y lo vuelve a arrancar al terminar (incluso si
  el test falla).
- **Offsets antes y después** de los topics de salida de cada stack (`market.ticks.canonical` y
  `market.analytics`, y sus gemelos `.q`). Eso da mensajes procesados de verdad, no estimaciones.
- **Pico de lag por grupo**, por separado: sin ese reparto no se sabe quién no da abasto, si el
  normalizer (transformar y publicar) o el motor de Streams (ventanas, joins, estado).
- **CPU leída de `/proc`** (`utime + stime`) por proceso de cada stack, antes y después. Es la
  única forma honesta de comparar dos frameworks en la misma máquina compartida.
- **Validez**: el pipeline tiene que haber procesado **todo** lo que entró (los dos canónicos
  cuadran con la entrada) y el DLT no puede crecer. Un grupo de consumidores sin miembros se marca
  como pipeline caído, no como "lag 0".

## 3. Resultados (ráfagas de 20 s, portátil de 16 núcleos)

| Tasa objetivo | Entrada real | Canónico Spring | Canónico Quarkus | Analítica Spring | Analítica Quarkus | Drenaje | CPU/mensaje Spring | CPU/mensaje Quarkus |
|---|---|---|---|---|---|---|---|---|
| 1.500 msg/s | 29.998 (1.499/s) | 29.998 | 29.998 | 149.990 | 149.990 | 48 s | 0,61 ms | 1,27 ms |
| 4.000 msg/s | 79.996 (3.997/s) | 79.996 | 79.996 | 399.980 | 399.980 | 106 s | 0,47 ms | 0,81 ms |

Pico de lag durante la ráfaga (el reparto que dice quién se atasca):

| Tasa | Normalizer Spring | Normalizer Quarkus | Analítica Spring | Analítica Quarkus |
|---|---|---|---|---|
| 1.500 msg/s | 12.270 | 8.109 | 840 | 0 |
| 4.000 msg/s | 67.003 | 62.979 | 1.639 | 0 |

Y la CPU por servicio en la ráfaga de 4.000 msg/s (milisegundos de CPU en toda la ventana):

| Servicio | Spring | Quarkus |
|---|---|---|
| `ingestion-normalizer` | 11.300 | 31.410 |
| `analytics-streams` (Kafka Streams) | 11.460 | 11.120 |
| `gateway-ws` | 5.530 | 11.310 |
| `alerting-service` | 3.430 | 3.740 |
| `audit-log` | 4.400 | 4.890 |
| `order-matching-engine` + `portfolio-risk` | 1.450 | 1.970 |
| **Total del stack** | **37.600** | **64.400** |

## 4. Qué dicen estos números

1. **Los dos aguantan sin perder un solo mensaje.** 47 veces la tasa del simulador, y los dos
   canónicos cuadran exactamente con la entrada: ni un registro perdido, ni uno en el DLT.
2. **El cuello de botella es el normalizer, no el motor de Streams.** Con 4.000 msg/s entrando, el
   pico de lag del normalizer es de 67.000 mensajes, y el de la analítica de 1.639 (Spring) y 0
   (Quarkus). Las ventanas, los joins y el estado no son el problema: transformar y republicar cada
   mensaje, sí.
3. **La salida es idéntica.** 399.980 eventos de analítica en los dos stacks: la misma topología
   sobre la misma entrada produce exactamente el mismo número. Es el control que dice que la
   comparación es de verdad la misma tubería.
4. **El coste no es idéntico.** El stack de Quarkus gasta ~1,7 veces más CPU por mensaje, y el
   gasto se concentra en el normalizer: **2,8 veces** más CPU que el de Spring para el mismo
   trabajo (31,4 s frente a 11,3 s). Los dos motores de Streams gastan lo mismo (11,1 frente a
   11,5 s), que es lo esperable: **por debajo es la misma librería**.
5. **Tasa sostenible, con la boca pequeña.** Durante la ráfaga el pipeline procesó lo que pudo y el
   resto lo drenó después: con 1.500 msg/s de entrada procesó unos 0,9k/s en caliente y el atraso
   tardó 28 s en vaciarse; con 4.000 msg/s, unos 0,65k/s. O sea: **absorbe ráfagas de 4.000/s y su
   tasa sostenible en este portátil está en torno a 0,7-0,9k/s**. Para afinar eso hacen falta
   ráfagas largas (300 s) y ver si el lag se estabiliza: la receta es la misma línea de comandos.

**Conclusión honesta:** para elegir framework, el throughput no decide (empatan). Lo que decide es
el **coste por mensaje**, y ahí Spring gana en esta configuración concreta —con una diferencia que
está toda en el normalizer y que probablemente se puede ajustar (tamaño de poll, número de hilos,
`max.poll.records`) antes de darla por buena como característica de los frameworks.

## 5. Lo que destapó, que era otra cosa: la máquina se asfixia sin límites de heap

El primer intento **no midió nada, y ese fue el hallazgo**. Los síntomas, en orden:

- El normalizer de Spring dejó de publicar: en 20 s el crudo avanzó **+354** y su topic canónico
  **+0**. El contador del log se quedó clavado (`recibidos=66000`, `publicados=49184`).
- Su grupo de consumo seguía **Stable y con lag 0**: iba al día de offsets… de mensajes que nunca
  se publicaban. Ese es el engaño: **lag 0 con un productor muerto**. Se perdieron ~18.000
  mensajes sin un solo error en el log (el código solo avisa cuando el envío *falla*; cuando el
  envío se queda colgado para siempre, no avisa de nada).
- La analítica de Spring dejó de contestar en el 8085: `curl` colgado. `jstat` del proceso:
  **75.055 recolecciones completas y 193 s de GC**. El proceso estaba vivo y no hacía nada útil.

La causa no era ni Spring ni Quarkus: **13 JVM compartiendo 23 GB sin un solo límite de heap**.
Cada JVM se coge por defecto un cuarto de la RAM de la máquina (unos 6 GB), así que el conjunto
pide varias veces la memoria que hay, el GC se pone a dar vueltas y los procesos se quedan
colgados sin decir nada (con el heap pequeño no hay OutOfMemory: solo sufrimiento).

Y el arreglo ya estaba escrito en el proyecto, en otro sitio: **la unidad de systemd de la Fase 10**
(`deploy/systemd/aggora@.service`) pone `-Xmx320m` por servicio. Los scripts locales no ponían
nada. Ahora sí, con el mismo valor:

```bash
SPRING_JAVA_OPTS=-Xms128m -Xmx384m   # scripts/start-services.sh
QUARKUS_JAVA_OPTS=-Xms128m -Xmx320m  # scripts/start-quarkus-stack.sh
```

Con los dos stacks reiniciados con límites, los mismos 1.500 msg/s que antes colgaban el pipeline
pasaron **sin perder un mensaje**. Verificado: la sonda del 8085 responde 200 y
`aggora_kafka_streams_running` está en 1 al terminar el test.

## 6. Lo que este experimento NO puede decir

- **Es un portátil, y todo comparte CPU**: el generador, los 14 servicios, los 3 brokers de Kafka
  y el Schema Registry. Los números son comparables **entre sí** (misma máquina, misma carga, a la
  vez) y no son números de producción.
- **Las ráfagas son de 20 s**: miden cuánto atraso absorbe el pipeline y si se recupera, no la tasa
  exacta donde el lag se estabiliza.
- **La CPU por mensaje incluye servicios que no procesan ticks** (matching, cartera, auditoría):
  es el coste del stack entero, no el de la tubería de ticks. Por eso el informe da además el
  desglose por servicio.
- **Una sola medición por tasa.** Con este montaje no se puede pretender precisión de banco de
  pruebas: lo que se afirma es la diferencia grande (2,8x en el normalizer) y el empate en
  throughput, no diferencias del 10%.

## 7. Segunda ronda: ráfaga larga (120 s), y el matiz que faltaba

Las ráfagas de 20 s miden absorción, no tasa sostenible. Con una de **120 s a 1.000 msg/s** (119.999
mensajes) aparece el matiz que decide cuál de los dos "gana" según lo que te importe:

| Medida | Spring | Quarkus |
|---|---|---|
| Canónico (procesado) | 119.999 | 119.999 |
| Analítica | 599.995 | 599.995 |
| **Pico de lag del normalizer** | **55.746** (46% de la entrada) | **24.939** (21%) |
| CPU del normalizer | 24,8 s (0,21 ms/msg) | 88,5 s (0,74 ms/msg) |
| CPU del motor de Streams | 38,0 s | 39,9 s |
| CPU del stack entero | 0,79 ms/msg | 1,56 ms/msg |

Traducido: **Spring gasta 3,6 veces menos CPU en el normalizer, pero se queda atrás el doble**.
Quarkus procesa más mensajes en caliente (por eso acumula menos atraso) a base de gastar más CPU
—hilos de trabajo y una tarea por mensaje con su propagación de contexto, frente al bucle por lotes
del listener de Spring—. Y los dos acaban igual: procesan todo, sin perder un mensaje y sin DLT.
Los dos motores de Streams vuelven a costar lo mismo (38,0 vs 39,9 s), que sigue siendo el control de
que la medición no se ha ido de las manos.

**La configuración del consumidor es la misma en los dos** (verificado, porque si no la comparación
no valdría): `max.poll.records=200`, `max.poll.interval.ms=300000`, el mismo deserializador de
Confluent y el mismo commit manual. Así que la diferencia de coste **no es un desajuste de
configuración**: es el modelo de despacho.

**Tasa sostenible, ahora con mejor base.** Con 120 s a 1.000 msg/s ninguno de los dos va sobrado
(Spring procesó ~535/s en caliente y Quarkus ~790/s), así que la tasa donde el atraso deja de
crecer está **por debajo de 1.000 msg/s**: la estimación con ráfagas de 20 s (~900/s) era optimista
y esta la baja al entorno de **600-800 msg/s** por stack en este portátil, con los dos stacks, los
tres brokers y Grafana compartiendo CPU. Para clavarla: `bash scripts/throughput-test.sh 120 600 800 1000`
(unos 15 minutos) y ver a partir de qué tasa el pico de lag deja de crecer.

## 8. Qué haría falta para cerrar la pregunta

1. ~~Ráfagas largas~~ (hecho a 1.000 msg/s, ver la sección 7): falta barrer varias tasas
   (`bash scripts/throughput-test.sh 120 600 800 1000`) para encontrar el punto exacto. También se
   probó igualar `max.poll.records` (ya eran iguales) y el siguiente ajuste razonable es **subirlo
   en los dos** (p. ej. a 1.000): si la brecha de CPU se estrecha, parte del coste de Quarkus es por
   mensaje y se amortiza con lotes más grandes.
2. **Ajustar el normalizer de Quarkus** (`max.poll.records`, hilos de trabajo, `commit-strategy`)
   antes de atribuir los 2,8x al framework: puede ser configuración, no diseño.
3. **Repetir con la máquina en reposo** (solo un stack cada vez) para quitar la contención entre
   los dos: la comparación sería secuencial en vez de simultánea, y más limpia.
4. **Volver a medir en el despliegue** (Fase 10): allí los límites están en la unidad de systemd y
   la máquina no tiene 13 JVM peleando, así que los números serán otros — y esos sí serán los que
   importen para decidir.
