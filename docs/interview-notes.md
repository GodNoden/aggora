# Aggora contado en una entrevista

Este proyecto se construyó para aprender Kafka, pero está montado de forma que **cada fase responde
a una pregunta que se hace en una entrevista técnica**, y todas las respuestas están **medidas** en
el repo. Esto es el guion: la pregunta, la respuesta corta, y dónde está la prueba.

## El resumen de cinco minutos

> "Aggora es una plataforma de datos de mercado sobre Kafka: siete servicios en Spring Boot y los
> mismos siete otra vez en Quarkus, con los mismos contratos Avro, así que son intercambiables en
> marcha. La construí por fases y **cada fase mide una cosa**: cómo se reparten las particiones,
> qué pasa cuando un consumidor se cae, qué cambia exactamente between at-least-once y
> exactly-once, qué rompe un cambio de esquema, y qué se lleva cada framework por delante.
> Todo lo que digo tiene un número medido en el repo, incluido lo que me salió al revés de lo que
> esperaba."

Esa última frase es la que separa a quien ha usado Kafka de quien lo ha operado.

---

## Las preguntas, con la prueba de cada una

| Pregunta | Respuesta corta | Dónde está la prueba |
|---|---|---|
| **¿Qué pasa cuando entra un consumidor nuevo en un grupo?** | Rebalanceo: el grupo reparte otra vez las particiones y hay un rato en el que las que están en movimiento no las consume nadie. Con 6 particiones y dos instancias, 3 y 3. | Fase 1 (`RebalanceLogger`), y el capítulo 7 de `kafka-101` |
| **¿At-least-once o exactly-once? ¿Cuál usaste y por qué?** | Los dos, y sé lo que cuesta cada uno. Con commit manual tras procesar tengo at-least-once: **puede repetirse, no puede perderse**. Para las ejecuciones del motor de matching monté exactly-once con productor transaccional y `read_committed`. | Fase 4: el mismo topic leído con los dos niveles dio **121 frente a 168** mensajes (los 47 abortados existen y no los ve nadie). Repetido en Quarkus: **83.572 frente a 83.582** |
| **¿Cómo evitas duplicados si llega el mismo mensaje dos veces?** | Con idempotencia en el consumidor, no con fechas: en la auditoría hay un **índice único por (topic, partición, offset)**, así que la reentrega no vuelve a insertar. | Fase 5 (`schema.sql`) y el test de integración `OutboxPostgresIT` |
| **¿Qué haces con un mensaje que no se puede procesar?** | Al principio lo dejaba pendiente, y aprendí que **eso bloquea la partición para siempre**. Ahora: reintentos cortos y, si es venenoso, al topic de descartes **con el motivo en una cabecera**. Y el veneno se decide por el mensaje (su `orderId`), nunca por un contador: con un contador el reintento avanza y el mensaje se cuela. | Fase 6, `docs/kafka-101.md` capítulo 13 |
| **¿Por qué la clave del mensaje importa?** | Es lo que decide la partición y, con ella, **el orden**: mismo símbolo, misma partición, mismo orden. En el motor de matching la clave es el **símbolo** y no la cuenta, porque el libro de órdenes es por instrumento y sus órdenes tienen que caer juntas. | Fase 4, `docs/decisions.md` |
| **¿Qué pasa si cambias un esquema?** | Depende de la dirección, y "compatible" no significa nada sin decir en cuál: añadir un campo **con valor por defecto** funciona en las dos; **borrar un campo pasa el filtro BACKWARD y rompe a los consumidores ya desplegados**. Lo medí con el registro de verdad. | Fase 7: `scripts/schema-evolution-lab.sh` y la tabla de veredictos en `docs/schema-evolution-lab.md` |
| **¿Cómo saber si un servicio de Kafka está sano?** | Que el proceso exista no dice nada: **Kafka Streams puede estar vivo y en ERROR sin procesar nada**. Hay que preguntarle al motor, con una sonda que baje a DOWN, y que sea el supervisor quien reinicie. | Fase 8: `StreamsHealth` y el capítulo 18 (con el experimento: proceso vivo, `/analytics` 503, sonda DOWN) |
| **¿Se pierden datos si se cae un broker?** | Con un broker, sí. Con tres réplicas y `min.insync.replicas=2`: con **uno** caído se sigue escribiendo (offsets 639→705); con **dos**, la escritura **se para** con `NOT_ENOUGH_REPLICAS` en vez de arriesgar (751→751); al volver se recupera sin perder nada (salto a 1310 con lo que estaba en el buffer). | Fase 6, capítulo 16 |
| **¿Cómo pruebas esto?** | Unitarios en cada push (milisegundos: la aritmética del libro de órdenes, la cartera, el validador) y **tests de integración con Testcontainers** en los pull requests, que levantan Kafka, el Schema Registry y Postgres de verdad. | `services/spring/*/src/test/**/*IT.java` y `.github/workflows/ci.yml` |
| **Spring o Quarkus, ¿qué eliges?** | La respuesta con números: arranque, Quarkus gana en los siete servicios (24-47% menos); memoria, en seis de siete (5-33% menos). Pero **no migraría siete servicios Spring sanos por un segundo de arranque**: Quarkus se gana el sitio cuando el arranque o la escalada a cero son el problema. | `SPRING_VS_QUARKUS.md` |

---

## Las cuatro historias que conviene tener preparadas

En una entrevista no se recuerdan tablas: se recuerdan historias. Estas cuatro son de este proyecto,
son verdad, y cada una termina en algo que aprendiste.

1. **El fallo silencioso.** Un servicio de Kafka Streams se quedó en ERROR y siguió vivo, contestando
   peticiones, durante horas: el peor tipo de fallo, el que no avisa. Ahora hay sonda, y el
   `SHUTDOWN_APPLICATION` resultó **peor** que el problema (429 MB de log en 28 segundos y el proceso
   tampoco moría). Moraleja: no es cuestión de encontrar la respuesta buena del manejador, es que un
   servicio no debería decidir suicidarse; eso es del supervisor.
2. **La mina del DLT con contador.** Inyectaba el fallo "cada 20 órdenes" y el mensaje **nunca**
   llegaba al DLT: al reintentar, el contador avanzaba, el fallo desaparecía y el mensaje se
   procesaba. Un DLT es para mensajes **venenosos** (el fallo pertenece al mensaje), no para fallos
   del momento. Se decide por el `orderId`.
3. **El exactly-once cruzando implementaciones.** Paré el motor de matching de Spring y arranqué el
   de Quarkus con los mismos topics: el port de Quarkus leyó en `read_committed` lo que el productor
   transaccional de Spring había confirmado. Es la prueba de que lo que importa no es el framework,
   son los contratos.
4. **El `-Dnative` que decía BUILD SUCCESS.** No generaba binario: los poms no tenían el perfil
   `native`. Un "éxito" que no produce nada es peor que un error, y me obligó a comprobar el
   artefacto en vez de fiarme del código de salida.

---

## Lo que este proyecto NO es (y conviene decirlo antes de que lo pregunten)

- No hay autenticación, ni ejecución real de órdenes, ni conexión a un bróker de producción: es un
  laboratorio.
- Los tests de integración **existen y compilan**, pero en la máquina donde se desarrolló no hay
  socket de Docker para ejecutarlos (el devcontainer no lo tiene): corren en el CI, que es donde
  tienen que correr.
- El binario nativo está **pendiente**: la receta y los cuatro tropiezos están documentados en
  `scripts/build-native.sh` y `docs/decisions.md`, que es más útil que un número sin contexto.

Decir esto sin que te lo pregunten vale más que cualquier tabla: demuestra que sabes dónde están los
bordes de lo que has construido.
