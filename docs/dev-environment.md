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
| El socket montado no era el del motor | Con Docker Desktop, `/var/run/docker-host.sock` resultó ser el socket **del CLI**: el Testcontainers viejo (el del árbol de Spring) recibe 400 y no encuentra Docker. El de Quarkus, más nuevo, sí funciona: ver la actualización al final |
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

### ACTUALIZACIÓN (fase 10): Testcontainers SÍ funciona, y el diagnóstico era a medias

Al habilitar el IT de Quarkus en el CI se volvió a probar esto, y el diagnóstico anterior se queda
corto en un punto importante:

- **El `*IT` de Quarkus SÍ se ejecuta en el devcontainer**, con sus contenedores de verdad (Kafka y
  el Schema Registry por Dev Services), añadiendo **una sola variable**:
  `TESTCONTAINERS_RYUK_DISABLED=true`. Lo que fallaba no era el motor de Docker: era **Ryuk** (el
  contenedor que Testcontainers levanta para limpiar los recursos y al que luego no puede volver a
  llegar desde el devcontainer). Medido: `Tests run: 1, Failures: 0, Errors: 0` en 38 s.
- **El árbol de Spring sigue sin poder**: el Testcontainers que arrastra Spring Boot 4.1.1 es más
  viejo, y contra el socket de Docker Desktop falla con `Status 400` en
  `UnixSocketClientProviderStrategy` y un `NullPointerException` en
  `DockerDesktopClientProviderStrategy`. El de Quarkus (más nuevo) sí sabe hablar con ese proxy. Así
  que la limitación no es del entorno entero: **depende de la versión del cliente de Testcontainers**.
- Efecto práctico: los `*IT` de Spring se siguen verificando en el CI (donde hay Docker nativo y
  ya pasan), y el de Quarkus se puede verificar en local y en el CI.

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
