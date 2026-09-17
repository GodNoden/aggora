# Prompt de arranque para el chat del dashboard (repo `aggoraAngular`)

Este fichero **es el prompt**. Se copia entero y se pega como primer mensaje en un chat nuevo cuyo
workspace sea `/home/noei/aggoraAngular`.

Antes de pegarlo, ten a mano el backend levantado (`bash scripts/start-services.sh` y
`bash scripts/start-quarkus-stack.sh` en este repo) porque el prompt pide verificar contra él.

---

Trabajas en el repositorio `/home/noei/aggoraAngular`: una app **Angular 20.3** ya creada (standalone, sin routing, sin SSR, CSS, `ng build` en verde) que va a ser el **dashboard visual** de un proyecto de Kafka que vive en **otro repositorio** (`/home/noei/aggora`, "Aggora": plataforma de eventos de mercados con dos implementaciones completas del mismo pipeline, Spring Boot y Quarkus). Esa app **solo consulta** al backend: es de lectura, no escribe nada, y no es la fuente de verdad de ningún dato.

## Lo primero que tienes que hacer: leer el contrato

El backend ya está hecho y **te deja el contrato escrito** para que no tengas que adivinar nada ni
conocer el otro proyecto:

- `/home/noei/aggora/docs/CONTRACT.md` — **léelo entero y trátalo como la especificación**: endpoints,
  puertos, forma exacta de los mensajes del WebSocket, catálogo de paneles, códigos de error.
- `/home/noei/aggora/docs/dashboard.md` — el detalle largo: de dónde sale cada dato, qué enseña cada
  panel, las cinco lecciones y cómo se publica esto en internet.

Si algo del contrato no te cuadra o falta, **pregunta antes de inventarlo**. Y si detectas que la
documentación y la respuesta real del backend no coinciden, gana la respuesta real: dímelo y
anotamos la diferencia.

## El backend, en una tabla (el contrato manda si hay discrepancia)

| Qué | Dónde (local) | Notas |
|---|---|---|
| WebSocket de eventos en vivo | `ws://localhost:8089/ws` (Spring) y `ws://localhost:8189/ws` (Quarkus) | Mensajes `{v, kind, stack, ts, ...}`: `snapshot` **una vez por segundo** con el último tick por símbolo y `ticksIn`/`ticksOut`; `position` y `alert` al momento |
| Catálogo de métricas | `http://localhost:8089/api/metrics?panel=<nombre>` y `:8189` | Paneles: `pulso`, `lag`, `particiones`, `transacciones`, `descartes`, `salud`, `comparativa&de=<panel>`. **No** hay PromQL libre. Si un panel devuelve `series: []` con una `nota`, es que no hay esa serie: **muéstralo como "no hay dato"**, no lo rellenes |
| Consultas interactivas al estado | `http://localhost:8085/analytics?symbol=<sym>&minutes=<n>` (Spring) y `:8185` (Quarkus) | Es la ventana calculada en vivo (VWAP, volatilidad); el panel del *state store* |
| Salud | `/actuator/health` (Spring) y `/q/health` (Quarkus) | |

Detalles del entorno que te van a afectar:

- **El backend corre dentro de un devcontainer** de Docker y sus puertos están reenviados al host: el
  navegador los ve como `localhost`. Tú no tienes que arrancar nada del backend.
- **Node está en el host** (v24), no hace falta contenedor para el frontend.
- **CORS ya está resuelto** para `http://localhost:4200` (allowlist en el backend): `ng serve` puede
  llamar a esos endpoints sin configurar nada.
- **El WebSocket es cross-origin-friendly**: `ws://localhost:8089/ws` funciona desde una página en
  `:4200`. Ojo con una cosa que importa el día del despliegue: si la página se sirve por **https**,
  el navegador **bloquea** `ws://` y hay que usar `wss://`. Deja eso resuelto en el código (elige el
  esquema según `location.protocol`) en vez de escribirlo a mano.
- **Trampa de esta máquina**: `~/.npm` tiene ficheros de root de una ejecución antigua, así que los
  comandos de npm pueden fallar con `EACCES`. Usa una caché temporal:
  `npm_config_cache=/tmp/npm-cache-ang npx …` / `npm_config_cache=/tmp/npm-cache-ang npm install`.

## Lo que hay que construir

Un dashboard **limpio, intuitivo y explicativo** (no un panel de Grafana más). Tres partes:

1. **El panel en vivo** (A): los 7 paneles del contrato, con el **selector de stack** (Spring,
   Quarkus o **los dos en paralelo, que es la firma del proyecto**: el mismo dato en dos columnas).
   Cada panel lleva **una frase en lenguaje llano** de "qué estás viendo" y "por qué importa" — eso es
   lo que hace que esto sea material de aprendizaje y no una tabla bonita.
2. **El modo lección** (B): un panel que lista las cinco lecciones del backend (`leccion-1-broker-caido.sh`
   … `leccion-5-streams-muerto.sh`), cada una con **el comando exacto para copiar** y **qué panel hay
   que mirar y qué debería cambiar**. El botón no ejecuta nada en el backend: las lecciones se lanzan
   desde la terminal (la página solo observa y narra). Esto es deliberado: **el dashboard es de solo
   lectura**.
3. **Publicable**: la app se compila a estáticos y se publica en un hosting estático (GitHub Pages,
   Netlify, Vercel). Documenta en el README cómo se apunta al backend público y **el problema de
   `https` → `wss`** (hace falta TLS delante del gateway: túnel o reverse proxy; está explicado en
   `docs/dashboard.md` del otro repo).

## Requisitos técnicos

- **Modelos tipados** para el contrato (los tres mensajes del WebSocket y las respuestas de
  `/api/metrics`): si el backend añade un campo, quieres que se vea en el tipo.
- **Un servicio de WebSocket** con **reconexión y espera creciente**, y que exponga los datos como
  *signals*; guarda una **ventana móvil corta** (por ejemplo los últimos 60 puntos) para las gráficas,
  porque llega un `snapshot` por segundo.
- **Un servicio de métricas** que consulte los paneles cada **5-10 segundos** (no cada segundo: es de
  buena educación con Prometheus).
- **Configuración por entorno** (`src/environments/`): las URLs base de los dos stacks, en un solo
  sitio.
- **Sin dependencias de CDN**: si quieres gráficas, vendoriza la librería (uPlot, Chart.js) en el
  repo, o dibuja SVG a mano. Una demo que depende de un CDN es una demo que un día no carga.
- **Nada de escribir en el backend** ni de inventar datos: si un panel viene vacío, se dice.
- Idioma: **UI y README en inglés** (esto es tu carta de presentación), y los textos largos del modo
  lección pueden ir en español si el dueño lo prefiere — pregúntale antes de mezclar idiomas.

## Cómo verificar (obligatorio antes de dar algo por terminado)

1. `npm_config_cache=/tmp/npm-cache-ang npx ng build` en verde.
2. Con el backend levantado: `npm_config_cache=/tmp/npm-cache-ang npx ng serve` y comprobar **en la
   app** que (a) llegan snapshots cada segundo, (b) el lag y el pulso se pintan con datos reales,
   (c) el selector cambia entre Spring y Quarkus, (d) un panel sin serie muestra "no hay dato" y no
   una gráfica vacía.
3. Un test unitario pequeño del parseo del contrato (que un `snapshot` inválido no rompa la app).
4. `npm_config_cache=/tmp/npm-cache-ang npx ng test --watch=false` (o el runner que traiga Angular 20)
   en verde, y `ng build` como parte del README.

## Reglas

- **No toques `/home/noei/aggora`** (el backend): solo se lee de ahí el contrato y la documentación.
- Nada de frameworks de estado ni librerías nuevas sin preguntar; Angular trae de sobra lo que hace
  falta (signals, HttpClient, RxJS).
- Comenta en **español sin acentos** (como el otro repo) y mantén el README en inglés.
- Al terminar, dime: qué paneles están completos, cuáles a medias y por qué, y qué del contrato no
  has podido cumplir.
