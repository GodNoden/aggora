# Laboratorio de evolución de esquemas (Fase 7)

Este documento es el manual del laboratorio: qué se prueba, cómo se ejecuta, qué significa
cada veredicto y qué se hace en la vida real cuando hay que cambiar un contrato que ya está
en marcha. Los conceptos, explicados desde cero con analogías, están en el
[capítulo 17 de la guía de Kafka](kafka-101.md).

```
bash scripts/schema-evolution-lab.sh
```

---

## 1. El problema que resuelve

En la Fase 2 el proyecto dejó de mandar JSON por los topics y pasó a mandar **Avro**: cada
mensaje lleva un byte mágico, el **ID del esquema** con el que se escribió y los datos en
binario. El esquema en sí no viaja en el mensaje: quien lee pide ese ID al **Schema
Registry** y con él traduce los bytes a campos con nombre.

Eso es lo que hace posible cambiar un contrato sin parar el sistema… y también lo que
convierte un cambio mal hecho en un 3 de la mañana. Porque los servicios no se despliegan
a la vez: durante un rato hay productores nuevos y consumidores viejos conviviendo, leyendo
el mismo topic.

La pregunta del laboratorio es exactamente esa:

> Si cambio el contrato mientras el sistema está en marcha, ¿a quién rompo y cómo me entero
> **antes** de romperlo?

El Schema Registry es quien contesta, y lo hace antes de que nadie escriba un mensaje: si el
esquema nuevo no es compatible, **se niega a registrarlo** y el despliegue no llega a
producción.

---

## 2. Qué toca el laboratorio (y qué no)

Lo que **sí** hace:

- Pregunta al registro si seis cambios distintos son compatibles, sin modificar nada.
- Intenta registrar de verdad uno que rompe, para enseñar el `409` y su mensaje.
- Registra de verdad el cambio bueno (y lo borra al final).
- Registra el que rompe en un **topic nuevo**, que sí lo acepta (y lo borra al final).

Lo que **no** hace: **no produce ni un mensaje** con los esquemas nuevos.

Esa última línea tiene su porqué. En el topic, cada mensaje apunta a un ID de esquema. Si
produces 100 mensajes con el esquema número 20 y luego **borras** ese esquema, esos 100
mensajes se quedan **ilegibles para siempre**: quien los lea pedirá el ID 20 al registro, el
registro dirá que no existe y el consumidor se quedará atascado. Es la mina antipersona de
este tema, y por eso el laboratorio se queda en el registro.

Al terminar, el script comprueba que el subject vuelve a estar como estaba (`versiones: [1]
(baseline: [1])`) y, si se corta a medias, un `trap` deja limpio lo que hubiera registrado.

---

## 3. Los veredictos, medidos

Esto no es teoría: es la salida real del registro de Aggora. **BACKWARD** significa "el
esquema nuevo es capaz de leer los datos que ya están escritos con el viejo". **FORWARD**
significa lo contrario: "un lector viejo es capaz de leer los datos que escriba el nuevo".

| Cambio | BACKWARD | FORWARD |
| --- | --- | --- |
| Añadir un campo **con** valor por defecto | COMPATIBLE | COMPATIBLE |
| Añadir un campo **sin** valor por defecto | RECHAZADO | COMPATIBLE |
| Renombrar un campo sin alias | RECHAZADO | RECHAZADO |
| Renombrar un campo con alias | COMPATIBLE | RECHAZADO |
| Borrar un campo | COMPATIBLE | RECHAZADO |
| Hacer un campo opcional (`["null","long"]`, default `null`) | COMPATIBLE | RECHAZADO |

La frase que resume la tabla: **lo único que es seguro en las dos direcciones es añadir un
campo con valor por defecto**. Todo lo demás obliga a decidir quién se despliega primero.

Los dos rechazos más útiles, con el mensaje literal del registro:

```
{errorType:'READER_FIELD_MISSING_DEFAULT_VALUE',
 description:'The field 'venueMic' at path '/fields/13' in the new schema has no default
              value and is missing in the old schema'}
```

```
HTTP 409 {"error_code":40901,
          "message":"Schema being registered is incompatible with an earlier schema
                     for subject \"market.ticks.canonical-value\""}
```

Traducido: *"el lector nuevo pide un campo que los mensajes viejos no traen y no has dicho
qué poner cuando falte"*. Los mensajes viejos están en el topic y no se pueden reescribir,
así que ese campo **se quedará vacío para siempre** en todo el histórico. O le pones valor
por defecto, o no hay cambio.

---

## 4. La trampa: borrar un campo pasa el filtro

Es el resultado más incómodo de la tabla y el que más veces pilla a la gente:

```
== 3. La trampa: BORRAR un campo pasa el filtro BACKWARD y rompe a los antiguos ==
   con el nivel BACKWARD (el de este proyecto): COMPATIBLE
   con el nivel FORWARD: RECHAZADO
```

Borrar `originOffset` de `CanonicalTick` es **compatible hacia atrás**, porque el lector
nuevo simplemente ignora un campo que no conoce. Si el nivel del subject es BACKWARD (el
que trae el registro por defecto), **el registro te deja hacerlo** y no te avisa de nada.

Y sin embargo rompe: los consumidores que ya están desplegados **sí** conocen el campo, lo
necesitan, y a partir de ese despliegue no lo van a recibir. El error aparece en producción,
en el consumidor, no en el registro.

La lección no es "el registro está mal", es: **la palabra "compatible" no significa nada
sin decir en qué dirección**. Por eso el laboratorio imprime las dos columnas.

---

## 5. Los arreglos, y cuándo usar cada uno

| Quiero… | Cómo se hace sin romper |
| --- | --- |
| Añadir información | Campo nuevo, opcional y **con valor por defecto**. El más barato: no hay que tocar a nadie. |
| Renombrar un campo | Deja el nombre viejo como **alias** del nuevo. El lector nuevo entiende los datos viejos; los consumidores viejos siguen leyendo si despliegas consumidores primero. |
| Borrar un campo | En dos tiempos: primero se declara **opcional** (`["null","long"]` con default `null`) y se despliegan los consumidores; cuando ya nadie lo escribe, se quita. |
| Cambiar el contrato de verdad | **Topic nuevo** (`market.ticks.canonical.v2`), que es un subject nuevo y por tanto no tiene historial con el que ser incompatible. Se escribe en los dos durante la migración y el viejo se apaga cuando nadie lo lee. |

Ninguno es gratis: el alias hay que mantenerlo, el borrado en dos tiempos son dos
despliegues, y el topic nuevo significa dos contratos vivos y consumidores migrados a mano.
Lo que no existe es "cambiarlo y ya está".

Sobre el nivel de compatibilidad: BACKWARD es el que quiere un proyecto donde **los
consumidores se despliegan después** de los productores (lo normal: primero se escribe el
dato, luego se aprende a leerlo). FORWARD es el que quiere quien despliega consumidores
antes que productores. FULL exige las dos y es el más caro de mantener; el registro de
Aggora se queda en BACKWARD a propósito, y el laboratorio cambia el nivel solo un momento,
dentro de `comprobar_con_nivel`, para enseñar la otra columna.

---

## 6. Los tests que lo comprueban sin broker

El script mide lo que dice el **registro**. Para comprobar que lo que dice es verdad hace
falta ver la traducción de Avro funcionando, y eso no necesita ni Kafka ni registro:
`services/ingestion-normalizer/src/test/java/com/aggora/normalizer/schema/SchemaEvolutionTest.java`.

Cinco tests, con el fichero `canonical-v2.avsc` como propuesta, que demuestran lo mismo con
la clase `CanonicalTick` **ya compilada** (el "consumidor antiguo" de verdad, sin
recompilar nada):

1. el consumidor antiguo lee los mensajes del esquema nuevo y el campo que sobra se ignora;
2. el consumidor nuevo lee los mensajes viejos y el campo que falta lo pone el default;
3. un campo nuevo **sin** default rompe la lectura y el mismo campo **con** default no;
4. renombrar rompe la lectura y con un alias no;
5. borrar un campo deja leer al nuevo pero deja ciego al antiguo.

`mvn -pl ingestion-normalizer test -Dtest=SchemaEvolutionTest`

---

## 7. Playbook: cambiar un contrato que ya está en marcha

1. **Escribe el cambio** en el `.avsc`.
2. **Pregunta antes de registrar** (es el paso 1 del laboratorio y es una línea de `curl`;
   se puede meter en el CI y que la build falle sola):
   ```bash
   jq -Rs '{schema: .}' tu-esquema-nuevo.avsc \
     | curl -s -X POST 'localhost:8081/compatibility/subjects/market.ticks.canonical-value/versions/latest?verbose=true' \
         -H 'Content-Type: application/vnd.schemaregistry.v1+json' --data @-
   ```
   Si sale `false`, el campo `messages` trae el motivo exacto y el campo culpable.
3. **Mira las dos direcciones**, no una: `BACKWARD` con el nivel del subject y `FORWARD`
   cambiando el nivel un momento (o pregunta por la columna que te importa según el orden de
   despliegue que vas a seguir).
4. **Registra** el esquema (`POST /subjects/<subject>/versions`) y **despliega** en el orden
   que diga la tabla: si es compatible solo hacia atrás, los consumidores van después.
5. Si el cambio rompe y no hay vuelta de hoja: **topic nuevo**, `v2`, y migra a los
   consumidores leyendo de los dos sitios hasta que el viejo se pueda apagar.
6. Y lo que nunca: **borrar un esquema del registro** mientras haya mensajes en el topic
   escritos con él. Los mensajes no se pueden reescribir; el esquema es su única llave.
