package com.aggora.matching.kafka;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

import com.aggora.matching.config.AggoraConfig;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.quarkus.runtime.StartupEvent;

/**
 * El topic de ejecuciones lo declara este servicio, que es quien escribe en el. El de ordenes
 * entrantes lo declara el simulador, que es su productor.
 *
 * <p>El de descartes SI hay que declararlo: con la autocreacion de topics apagada (Fase 2), el
 * publicador del DLT no puede crearlo por su cuenta y falla con "unknown topic". Misma regla de
 * siempre: cada topic lo declara quien escribe en el. Una sola particion, como fija el spec.
 */
@ApplicationScoped
public class TopicCreator {

    private static final Logger log = LoggerFactory.getLogger(TopicCreator.class);

    private final String bootstrapServers;
    private final String executions;
    private final String ordersIncomingDlt;

    @Inject
    public TopicCreator(@ConfigProperty(name = "kafka.bootstrap.servers") String bootstrapServers,
                        AggoraConfig props) {
        this.bootstrapServers = bootstrapServers;
        this.executions = props.topics().executions();
        this.ordersIncomingDlt = props.topics().ordersIncoming() + ".DLT";
    }

    void alArrancar(@Observes StartupEvent evento) {
        List<NewTopic> deseados = List.of(
                new NewTopic(executions, Optional.of(6), Optional.<Short>empty()),
                new NewTopic(ordersIncomingDlt, Optional.of(1), Optional.<Short>empty()));

        try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", bootstrapServers))) {
            Set<String> existentes = admin.listTopics().names().get();
            java.util.ArrayList<NewTopic> faltan = new java.util.ArrayList<>();
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
