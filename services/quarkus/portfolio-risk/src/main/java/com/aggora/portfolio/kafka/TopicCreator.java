package com.aggora.portfolio.kafka;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

import com.aggora.portfolio.config.AggoraConfig;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.quarkus.runtime.StartupEvent;

/**
 * El topic de posiciones lo declara quien escribe en el. Kafka Streams crea por su cuenta los
 * topics internos (changelog del state store y reparticion), asi que esos no se declaran aqui.
 *
 * <p>Sin replicas explicitas: manda el default del broker.
 */
@ApplicationScoped
public class TopicCreator {

    private static final Logger log = LoggerFactory.getLogger(TopicCreator.class);

    private final String bootstrapServers;
    private final String portfolioUpdates;

    @Inject
    public TopicCreator(@ConfigProperty(name = "kafka.bootstrap.servers") String bootstrapServers,
                        AggoraConfig props) {
        this.bootstrapServers = bootstrapServers;
        this.portfolioUpdates = props.topics().portfolioUpdates();
    }

    void alArrancar(@Observes StartupEvent evento) {
        NewTopic posiciones = new NewTopic(portfolioUpdates, Optional.of(6), Optional.<Short>empty());
        try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", bootstrapServers))) {
            Set<String> existentes = admin.listTopics().names().get();
            if (existentes.contains(portfolioUpdates)) {
                log.info("[topics] {} ya existe, no se toca", portfolioUpdates);
                return;
            }
            admin.createTopics(java.util.List.of(posiciones)).all().get();
            log.info("[topics] creado {} con {} particiones", posiciones.name(), posiciones.numPartitions());
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
