package com.aggora.alerting.kafka;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

import com.aggora.alerting.config.AggoraConfig;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.quarkus.runtime.StartupEvent;

/**
 * El topic de alertas lo declara quien escribe en el. Kafka Streams crea por su cuenta los
 * topics internos (changelog del state store y reparticion), asi que esos no se declaran aqui.
 *
 * <p>Sin replicas explicitas: manda el default del broker.
 */
@ApplicationScoped
public class TopicCreator {

    private static final Logger log = LoggerFactory.getLogger(TopicCreator.class);

    private final String bootstrapServers;
    private final String alertsRaised;

    @Inject
    public TopicCreator(@ConfigProperty(name = "kafka.bootstrap.servers") String bootstrapServers,
                        AggoraConfig props) {
        this.bootstrapServers = bootstrapServers;
        this.alertsRaised = props.topics().alertsRaised();
    }

    void alArrancar(@Observes StartupEvent evento) {
        NewTopic alertas = new NewTopic(alertsRaised, Optional.of(3), Optional.<Short>empty());
        try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", bootstrapServers))) {
            Set<String> existentes = admin.listTopics().names().get();
            if (existentes.contains(alertsRaised)) {
                log.info("[topics] {} ya existe, no se toca", alertsRaised);
                return;
            }
            admin.createTopics(java.util.List.of(alertas)).all().get();
            log.info("[topics] creado {} con {} particiones", alertas.name(), alertas.numPartitions());
        } catch (ExecutionException ex) {
            if (ex.getCause() instanceof TopicExistsException) {
                log.info("[topics] otro servicio lo creo antes");
            } else {
                log.error("[topics] no se pudo declarar el topic: {}", ex.getCause().getMessage());
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }
}
