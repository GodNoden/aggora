package com.aggora.normalizer.it;

import java.util.Map;
import java.util.Set;

import jakarta.inject.Inject;

import org.apache.kafka.clients.admin.AdminClient;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import io.quarkus.test.junit.QuarkusTest;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test de INTEGRACION de la version Quarkus, y aqui esta la diferencia de DX que merece la pena
 * documentar: en Quarkus no hay que levantar los contenedores a mano.
 *
 * <p><b>Dev Services.</b> Al arrancar un {@code @QuarkusTest}, Quarkus levanta solo lo que necesita
 * (Kafka, y el Schema Registry con la extension de Confluent) usando Testcontainers por debajo, SIN
 * una linea de configuracion. En la version Spring hay que declarar los contenedores, esperar a que
 * arranquen y apuntar la configuracion a sus puertos: mas control y mas codigo.
 *
 * <p>Lo que se comprueba es lo que de verdad se rompe al portar: que la aplicacion <b>arranca
 * contra un broker y un registro de verdad</b> y que <b>declara sus topics</b>. Es donde aparecieron
 * los problemas de configuracion estricta y de reflexion, asi que es lo que tiene que cubrir un test
 * en esta implementacion.
 *
 * <p>Se ejecuta en el CI (que tiene Docker nativo). En el devcontainer de este proyecto no corre por
 * el socket de Docker Desktop, que es una limitacion del entorno y no del test.
 */
@QuarkusTest
class NormalizerKafkaIT {

    @Inject
    @ConfigProperty(name = "kafka.bootstrap.servers")
    String bootstrapServers;

    @Test
    @DisplayName("Arranca contra Kafka de verdad y declara sus tres topics")
    void arranca_y_declara_los_topics() throws Exception {
        try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", bootstrapServers))) {
            Set<String> topics = admin.listTopics().names().get();
            assertTrue(topics.contains("market.ticks.canonical"),
                    "el normalizer tiene que declarar el topic del que es productor: " + topics);
            assertTrue(topics.contains("market.fx.reference"),
                    "y el compactado de tipos de cambio: " + topics);
            assertTrue(topics.contains("market.ticks.raw.DLT"),
                    "y el de descartes: " + topics);
        }
    }
}
