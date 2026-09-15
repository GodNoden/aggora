package com.aggora.audit.kafka;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

import com.aggora.audit.config.AggoraConfig;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.quarkus.runtime.StartupEvent;

/**
 * El topic de auditoria lo declara quien escribe en el. Es COMPACTADO y su clave es el entityId,
 * asi que quien lo lea se queda con el ultimo evento de cada entidad.
 */
@ApplicationScoped
public class TopicCreator {

    private static final Logger log = LoggerFactory.getLogger(TopicCreator.class);

    private final String bootstrapServers;
    private final String auditEvents;

    @Inject
    public TopicCreator(@ConfigProperty(name = "kafka.bootstrap.servers") String bootstrapServers,
                        AggoraConfig props) {
        this.bootstrapServers = bootstrapServers;
        this.auditEvents = props.topics().auditEvents();
    }

    void alArrancar(@Observes StartupEvent evento) {
        // Una sola particion y compactado: el mismo que en la version Spring.
        NewTopic auditoria = new NewTopic(auditEvents, Optional.of(1), Optional.<Short>empty())
                .configs(Map.of("cleanup.policy", "compact"));
        try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", bootstrapServers))) {
            Set<String> existentes = admin.listTopics().names().get();
            if (existentes.contains(auditEvents)) {
                log.info("[topics] {} ya existe, no se toca", auditEvents);
                return;
            }
            admin.createTopics(java.util.List.of(auditoria)).all().get();
            log.info("[topics] creado {} compactado", auditEvents);
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
