# Entorno de desarrollo: qué haría distinto la próxima vez

Este documento existe porque el entorno de Aggora nos costó tres reconstrucciones del contenedor en
una sola sesión, y ninguno de esos tropiezos tenía que ver con Kafka ni con el código: eran del
**montaje del entorno**. Va escrito como recomendación para el próximo proyecto.

## Lo que usa Aggora (y por qué se tuerce)

```
devcontainer.json  ->  "image": java:21      (la toolchain vive aquí)
infra/docker-compose.yml  ->  Kafka, SR, Postgres, Prometheus, Grafana (en el host)
los 7 servicios    ->  procesos JVM dentro del devcontainer
```

Dos mundos unidos por una red externa (`aggora-net`) y por un doble listener en Kafka
(INTERNAL para dentro, EXTERNAL para el host). Lo que costó:

| Tropiezo | Por qué |
|---|---|
| El devcontainer no tenía Docker | Los tests de integración y el nativo quedaron bloqueados hasta montar `docker-outside-of-docker` |
| El socket montado no era el del motor | Con Docker Desktop, `/var/run/docker-host.sock` es el socket **del CLI**: Testcontainers 1.21.3 recibe 400 y no encuentra Docker. Con la 2.x (la que usa Quarkus, y a la que se subió el árbol de Spring) sí funciona; lo único que hay que desactivar es Ryuk |
| La ruta del proyecto era distinta dentro y fuera | `/workspaces/aggora` contra `/home/noei/aggora`: el contenedor de Mandrel no podía montar el proyecto para compilar el nativo |
| El nombre del contenedor cambia al reconstruir | `eager_allen` → `charming_spence` → `condescending_keldysh`: todos los comandos con el nombre a pelo dejaron de funcionar |
| `target/` quedó de `root` | Por compilar con Maven desde fuera del devcontainer; después, `...jar is read-only` dentro |

## Lo que propongo: un solo compose, con la toolchain en la imagen

El patrón de Laravel Sail (el devcontainer **es** un servicio del compose) resuelve casi todo lo de
arriba, y en un proyecto con broker, base de datos y observabilidad es el que elegiría:

```
.devcontainer/devcontainer.json
    "dockerComposeFile": "../compose.yaml",
    "service": "app",
    "workspaceFolder": "/app",
    "remoteUser": "vscode",
    "postCreateCommand": "mvn -q -DskipTests package"
```

```yaml
services:
  app:                              # la toolchain va EN LA IMAGEN, no en features
    build: { context: ., dockerfile: .devcontainer/app.Dockerfile }
    volumes: [ ".:/app" ]           # UNA sola ruta para el proyecto
    command: sleep infinity
    depends_on: [ kafka ]
  kafka: { image: apache/kafka:3.9.0 }
  schema-registry: { image: confluentinc/cp-schema-registry:8.3.1 }
  postgres: { image: postgres:16-alpine }
  prometheus: { ... }
  grafana: { ... }
```

Y en `.devcontainer/app.Dockerfile`, además del JDK y Maven, **Mandrel** si vas a compilar nativo.

### Por qué esto arregla lo de arriba

1. **Una red, un nombre para cada cosa**: `kafka:9092`, `postgres:5432`. Se acaban la red externa y
   el **doble listener** (que en Aggora existe solo por el reparto entre host y contenedor). El CI
   usa los mismos nombres que tu máquina.
2. **Una sola ruta** para el proyecto, la que fije el compose. Nada de `workspaceFolder` a mano.
3. **Toolchain en la imagen** = entorno reproducible con `docker compose build`, en vez de features
   que cambian con cada reconstrucción del editor.
4. **Mandrel en la imagen** elimina el `container-build`, la necesidad del socket para el nativo y
   el problema de rutas del contenedor de Mandrel.
5. **El nombre del contenedor deja de importar**: los comandos son `docker compose exec app ...`.

### ACTUALIZACIÓN (fase 10): Testcontainers sí funciona, pero depende de la versión y del árbol

Probando esto en serio (al habilitar el IT de Quarkus en el CI) aparecieron tres cosas:

1. **Lo que no se alcanzaba era Ryuk**, el contenedor que Testcontainers levanta para limpiar. Con
   `TESTCONTAINERS_RYUK_DISABLED=true` (y `TESTCONTAINERS_HOST_OVERRIDE=host.docker.internal`) el
   motor de Docker sí se alcanza desde el devcontainer. El `*IT` de Quarkus corre así, en local y en
   el CI: `Tests run: 1, Failures: 0` con Kafka y registro de verdad.
2. **El árbol de Quarkus usa Testcontainers 2.0.5** (lo decide su BOM) y **el de Spring 1.21.3** (lo
   decide el BOM de Boot 4.1.1). La 1.21.3 no negocia con el socket de Docker Desktop (400 en la
   estrategia de socket y `NullPointerException` en la de Docker Desktop), así que **sus `*IT` solo
   corren en el CI**; la 2.x sí funciona en local.
3. **Se intentó subir el árbol de Spring a la 2.x y no ha quedado.** Localmente los dos `*IT`
   pasaban, pero en un runner Linux nativo el registro de esquemas no llegaba al broker: su chequeo
   `kafka-ready` falla con `Expected 1 brokers but found only 0` /
   `TimeoutException: Timed out waiting for a node assignment`, porque entra por
   `PLAINTEXT://kafka:9092` y el broker le devuelve el *listener* que anuncia para el host
   (`localhost:<puerto mapeado>`), inalcanzable desde dentro del contenedor. Es un cambio del
   cableado interno de la 2.x que **no se puede reproducir en el devcontainer** (Docker Desktop
   enruta de otra forma), así que se revirtió: mejor un CI verde con la versión que Boot ya prueba
   que una mejora que no se puede verificar. Queda escrito arriba lo que habría que intentar
   (`BROKER://kafka:9092`, o montar Kafka a mano con las variables de KRaft).

**El arreglo que se está probando (intento 2)**: el registro apuntaba a `PLAINTEXT://kafka:9092`, y
9092 es el listener que el broker anuncia **para el host** (`localhost:<puerto mapeado>`), inalcanzable
desde dentro de la red de contenedores; de ahí el `Expected 1 brokers but found only 0`. El listener
**interno** del contenedor de Confluent es el **9093** y su dirección anunciada sí es el alias de la
red (`kafka:9093`). El puerto del registro pasa a 9093 (el prefijo sigue siendo `PLAINTEXT` porque ahí
va el protocolo de seguridad, no el nombre del listener). Verificado en local: los dos IT de Spring en
verde. **Y verificado en el runner: los tres `*IT` en verde.** El puerto era el problema.

**Estado final (comprobado en local y en el CI):**

| Árbol | Testcontainers | `mvn verify` en local | En el CI |
|---|---|---|---|
| Spring | 2.0.5 (por encima de la que gestiona Boot, con el motivo escrito en `services/spring/pom.xml`) | Sí, con Ryuk desactivado | Sí |
| Quarkus | 2.0.5 (BOM de Quarkus) | Sí, con Ryuk desactivado | Sí, con Dev Services |

Los **tres** `*IT` del proyecto se ejecutan en los dos sitios, y los dos árboles usan la misma línea de
Testcontainers. La receta, en una línea:

```bash
docker exec -u vscode -e TESTCONTAINERS_RYUK_DISABLED=true <devcontainer> \
  bash -lc 'cd /workspaces/aggora/services && mvn verify \
    -pl spring/audit-log,spring/order-matching-engine,quarkus/ingestion-normalizer -am'
```

**El plan B que NO hizo falta** (queda escrito por si algún día se rompe): volver el árbol de Spring a
1.21.3 y aceptar el reparto asimétrico, con sus `*IT` verificándose solo en el CI. Se barajó porque el
primer intento (subir a la 2.x sin tocar el puerto del registro) dejaba el CI en rojo, y una mejora
que no se puede verificar en local no compensa un CI roto. El intento 2 lo resolvió.

**Lo que sí se quedó del intento**: la espera explícita del registro
(`waitingFor(Wait.forHttp("/subjects")...)`), que era un bug real del test —daba el contenedor por
listo en cuanto existía el proceso— y que las dos versiones se benefician.

| Árbol | Testcontainers | `mvn verify` en local | En el CI |
|---|---|---|---|
| Spring | 1.21.3 (BOM de Boot) | No (socket de Docker Desktop) | Sí, verde |
| Quarkus | 2.0.5 (BOM de Quarkus) | Sí, con Ryuk desactivado | Sí, con Dev Services |

### Lo que NO arregla: Testcontainers

**No, el patrón de Sail no hace que Testcontainers funcione por sí solo.** Es la pregunta que hay
detrás, y la respuesta honesta es que va por otro camino:

- Testcontainers necesita **hablar con el motor de Docker**, no con tu red de compose. Da igual que
  tu app esté en un compose: sin socket (o Docker-in-Docker) no hay contenedores de test.
- Y **el socket es justo lo que falló aquí**: en Docker Desktop el socket que se monta dentro de un
  contenedor puede ser el del *CLI* (`/var/run/docker-cli.sock`) en vez del motor, y el cliente Java
  de Testcontainers recibe 400 mientras el CLI funciona. Eso es una limitación de Docker Desktop,
  no del montaje del entorno, y **cambiar a Sail no la toca**.
- Lo que sí gana Sail: que montar el socket (si funciona) es una línea más del servicio `app` en vez
  de un cambio de devcontainer + reconstrucción. O sea, **convierte el problema en algo de cinco
  minutos en el día 1** en vez de un muro.

Las opciones reales, por orden de preferencia:
1. **Correr los tests de integración en el CI** (Docker nativo, sin proxy de Desktop). Es lo que hace
   este proyecto y es lo que hacen los equipos.
2. Montar el socket en el servicio `app` y **probarlo el primer día** (`docker run hello-world` y
   luego un test trivial con Testcontainers). Si funciona en tu Docker, funciona.
3. Docker-in-Docker (privilegiado) o Testcontainers Cloud, si de verdad hace falta en local.

## Cuándo NO usar el patrón de Sail

- Si el proyecto es **un solo servicio** sin broker ni base de datos: un devcontainer con `image:`
  es más simple y arranca antes.
- En una **máquina compartida**, montar el socket da al contenedor control sobre el Docker del host.
- Ojo con el ciclo de vida: con Sail, un `docker compose down` desde la terminal **te tumba el
  contenedor del editor** y VS Code tiene que reconectar. Es el precio del patrón.
