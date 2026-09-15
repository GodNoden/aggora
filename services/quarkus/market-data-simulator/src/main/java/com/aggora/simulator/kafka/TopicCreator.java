package com.aggora.simulator.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

import com.aggora.simulator.config.AggoraConfig;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.quarkus.runtime.StartupEvent;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * Los topics que declara ESTE servicio, igual que la version Spring: {@code market.ticks.raw} y
 * {@code orders.incoming}, los dos con 6 particiones porque la key es el simbolo.
 *
 * <p>Sin replicas explicitas: el broker aplica su default
 * ({@code KAFKA_DEFAULT_REPLICATION_FACTOR}), asi el mismo codigo vale para un broker suelto o
 * para el cluster de tres.
 */
@ApplicationScoped
public class TopicCreator {

    private static final Logger log = LoggerFactory.getLogger(TopicCreator.class);

    private final String bootstrapServers;
    private final String ticksRaw;
    private final String ordersIncoming;

    @Inject
    public TopicCreator(@ConfigProperty(name = "kafka.bootstrap.servers") String bootstrapServers,
                        AggoraConfig props) {
        this.bootstrapServers = bootstrapServers;
        this.ticksRaw = props.topics().ticksRaw();
        this.ordersIncoming = props.topics().ordersIncoming();
    }

    void alArrancar(@Observes StartupEvent evento) {
        List<NewTopic> deseados = List.of(
                new NewTopic(ticksRaw, Optional.of(6), Optional.<Short>empty()),
                new NewTopic(ordersIncoming, Optional.of(6), Optional.<Short>empty()));

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
