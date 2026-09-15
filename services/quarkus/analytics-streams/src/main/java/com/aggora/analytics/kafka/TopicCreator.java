package com.aggora.analytics.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

import com.aggora.analytics.config.AggoraConfig;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.quarkus.runtime.StartupEvent;

/**
 * Los topics de salida los declara este servicio, porque es quien escribe en ellos.
 *
 * <p>Kafka Streams crea por su cuenta los topics INTERNOS que necesita (los changelog de los state
 * stores y el de reparticion), asi que esos no se declaran aqui. Y sin replicas explicitas: manda
 * el default del broker ({@code KAFKA_DEFAULT_REPLICATION_FACTOR}).
 */
@ApplicationScoped
public class TopicCreator {

    private static final Logger log = LoggerFactory.getLogger(TopicCreator.class);

    private final String bootstrapServers;
    private final String analytics;
    private final String arbitrage;

    @Inject
    public TopicCreator(@ConfigProperty(name = "kafka.bootstrap.servers") String bootstrapServers,
                        AggoraConfig props) {
        this.bootstrapServers = bootstrapServers;
        this.analytics = props.topics().analytics();
        this.arbitrage = props.topics().arbitrage();
    }

    void alArrancar(@Observes StartupEvent evento) {
        List<NewTopic> deseados = List.of(
                new NewTopic(analytics, Optional.of(6), Optional.<Short>empty()),
                new NewTopic(arbitrage, Optional.of(6), Optional.<Short>empty()));

        try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", bootstrapServers))) {
            Set<String> existentes = admin.listTopics().names().get();
            List<NewTopic> faltan = new ArrayList<>();
            for (NewTopic topic : deseados) {
                if (existentes.contains(topic.name())) {
                    log.info("[topics] {} ya existe, no se toca", topic.name());
                } else {
                    faltan.add(topic);
                }
            }
            if (faltan.isEmpty()) {
                return;
            }
            admin.createTopics(faltan).all().get();
            faltan.forEach(t -> log.info("[topics] creado {} con {} particiones", t.name(), t.numPartitions()));
        } catch (ExecutionException ex) {
            if (ex.getCause() instanceof TopicExistsException) {
                log.info("[topics] otro servicio los creo antes");
            } else {
                log.error("[topics] no se pudieron declarar los topics: {}", ex.getCause().getMessage());
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }
}
