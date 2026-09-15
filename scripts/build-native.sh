#!/usr/bin/env bash
#
# Compila un servicio de Quarkus como BINARIO NATIVO (imagen de GraalVM), que es lo que pide el
# spec para dos servicios y la materia prima del informe de la Fase 9.
#
#   bash scripts/build-native.sh ingestion-normalizer
#   bash scripts/build-native.sh market-data-simulator
#
# Hay tres caminos, y este script usa el que funcione segun la maquina:
#
#   1. Con Mandrel/GraalVM instalado en el sistema: `mvn package -Dnative`. Es el mas rapido y el
#      que usa cualquiera que compile nativos a menudo (sdk install java <mandrel>, o SDKMAN).
#      OJO: `-Dnative` a secas solo funciona si el pom tiene el perfil `native` (los poms de este
#      proyecto estan escritos a mano y no lo tienen, asi que se pasa la propiedad que activa ese
#      perfil: `-Dquarkus.package.type=native`). Sin ella, Maven dice BUILD SUCCESS y deja el jar
#      de siempre, que es justo lo que paso la primera vez.
#   2. Con Docker pero SIN GraalVM: `-Dquarkus.native.container-build=true`, que hace que el
#      plugin de Quarkus arranque el contenedor de Mandrel por su cuenta. Necesita dos cosas:
#      Maven (con Java) y acceso al socket de Docker.
#   3. La que usa este script cuando no hay GraalVM: un contenedor de Maven al que se le monta el
#      socket de Docker y el repositorio EN LA MISMA RUTA que en el host (si no, el contenedor
#      que arranca el plugin no encuentra el proyecto: los volumenes los resuelve el Docker del
#      host).
#
# Ojo con lo que NO vale: la imagen `ubi9-quarkus-mandrel-builder-image` **no trae Maven** dentro
# (solo `/opt/mandrel`), asi que no se puede hacer `docker run ... mvn` con ella; esta pensada
# para que la use el plugin de Quarkus. Y el devcontainer del proyecto no tiene ni GraalVM ni el
# socket de Docker, asi que el nativo se compila desde WSL/Windows.
#
# Los tres tropiezos que costo llegar aqui, por si vuelven a aparecer:
#   1. `-Dnative` a secas no hace NADA si el pom no tiene el perfil `native`: dice BUILD SUCCESS y
#      deja el jar de siempre. La propiedad que de verdad activa el empaquetado nativo es
#      `-Dquarkus.package.type=native`.
#   2. El contenedor de Maven necesita el ejecutable de `docker` DENTRO (no basta con montarle el
#      socket): sin el, falla con `ContainerRuntimeUtil.detectContainerRuntime`.
#   3. No fijar `quarkus.native.builder-image` a mano: el tag tiene que ser el que corresponde a la
#      version de Quarkus, y si no, falla en `checkGraalVMVersion`.
#
# El binario sale en `services/quarkus/<servicio>/target/*-runner` y los datos de la imagen en
# `target/*-runner-sources.jar`. Ojo: un binario nativo NO es portable entre sistemas operativos ni
# arquitecturas, asi que se compila para la maquina donde va a correr (Linux x86_64 en este caso).

set -euo pipefail

SERVICIO="${1:-ingestion-normalizer}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# OJO: NO se fija la imagen del builder. Quarkus trae la suya por defecto y esa es la que
# garantiza que la version de GraalVM cuadra con la de Quarkus: si se le pasa un tag a mano
# (por ejemplo jdk-21) puede fallar con "checkGraalVMVersion" porque no es la que espera.
# Cache de Maven propia para las compilaciones nativas: asi no se mezcla con la del devcontainer
# (que tiene otra ruta de usuario) ni obliga a bajarse todo cada vez.
M2="${NATIVE_M2:-$HOME/.m2-native}"

if [ ! -d "$ROOT/services/quarkus/$SERVICIO" ]; then
  echo "No existe services/quarkus/$SERVICIO. Los que hay:"
  ls "$ROOT/services/quarkus" | grep -v pom.xml
  exit 1
fi

echo "Compilando $SERVICIO como binario nativo (Quarkus elige la imagen del builder)"
echo "(la primera vez tarda: se baja la imagen del builder y todas las dependencias)"

# El contenedor de Maven, con el socket de Docker montado (para que el plugin pueda arrancar el
# contenedor de Mandrel) y el proyecto en la MISMA ruta dentro y fuera.
# Al contenedor de Maven hay que darle TAMBIEN el ejecutable de docker: el plugin de Quarkus lo
# invoca para arrancar el builder de Mandrel, y sin el falla con
# "ContainerRuntimeUtil.detectContainerRuntime" (no encuentra ni docker ni podman).
docker run --rm \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v "$(readlink -f "$(command -v docker)"):/usr/local/bin/docker" \
  -v "$ROOT:$ROOT" \
  -v "$M2:/root/.m2" \
  -w "$ROOT/services" \
  "${NATIVE_MAVEN_IMAGE:-maven:3.9-eclipse-temurin-21}" \
  mvn -pl "quarkus/$SERVICIO" -am package -DskipTests \
      -Dquarkus.package.type=native \
      -Dquarkus.native.container-build=true \
      -Dquarkus.native.builder-image="$BUILDER" \
      -Dquarkus.native.native-image-xmx=3g

echo
echo "Binario:"
ls -la "$ROOT/services/quarkus/$SERVICIO/target/"*-runner 2>/dev/null || echo "  (no se genero; mira el log de arriba)"
