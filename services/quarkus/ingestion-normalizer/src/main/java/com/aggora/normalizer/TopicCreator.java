package com.aggora.normalizer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.quarkus.runtime.StartupEvent;

/**
 * Los topics que declara ESTE servicio, igual que hace la version Spring.
 *
 * <p>{@code market.ticks.canonical} lo crea el normalizer porque es su productor: la regla es
 * que cada topic lo declara quien escribe en el, y asi las particiones quedan versionadas en
 * el repo. Tambien el de descartes y el compactado de tipos de cambio.
 *
 * <p>Se declaran <b>sin replicas explicitas</b>: el broker aplica su default
 * ({@code KAFKA_DEFAULT_REPLICATION_FACTOR}, que en el cluster de la Fase 6 es 3). Asi el
 * mismo codigo vale para un broker suelto o para tres.
 *
 * <p>Esto es lo que la version Spring hace con beans {@code NewTopic}: en Quarkus no hay
 * autoconfiguracion de topics, asi que se piden al broker a mano con el {@code AdminClient}
 * al arrancar. Si ya existen, no se toca nada (los topicos no se pueden recrear: el log es
 * el log).
 */
@ApplicationScoped
public class TopicCreator {

    private static final Logger log = LoggerFactory.getLogger(TopicCreator.class);

    private final String bootstrapServers;
    private final String canonicalTicks;
    private final String fxReference;
    private final String rawTicksDlt;

    public TopicCreator(@ConfigProperty(name = "kafka.bootstrap.servers") String bootstrapServers,
                        @ConfigProperty(name = "aggora.topics.ticks-canonical",
                                defaultValue = "market.ticks.canonical") String canonicalTicks,
                        @ConfigProperty(name = "aggora.topics.fx-reference",
                                defaultValue = "market.fx.reference") String fxReference,
                        @ConfigProperty(name = "aggora.topics.ticks-raw-dlt",
                                defaultValue = "market.ticks.raw.DLT") String rawTicksDlt) {
        this.bootstrapServers = bootstrapServers;
        this.canonicalTicks = canonicalTicks;
        this.fxReference = fxReference;
        this.rawTicksDlt = rawTicksDlt;
    }

    void alArrancar(@Observes StartupEvent evento) {
        // 6 particiones, igual que el crudo: la key sigue siendo el simbolo, asi que el
        // reparto entre consumidores funciona igual.
        NewTopic canonico = new NewTopic(canonicalTicks, Optional.of(6), Optional.<Short>empty());
        // Descartes: una sola particion. Es un sitio para mirar y arreglar, no para throughput.
        NewTopic descartes = new NewTopic(rawTicksDlt, Optional.of(1), Optional.<Short>empty());
        // Tipos de cambio como DATO DE REFERENCIA: compactado (interesa el ultimo valor de
        // cada par) y una sola particion (son dos pares, no hay que repartir).
        NewTopic tiposDeCambio = new NewTopic(fxReference, Optional.of(1), Optional.<Short>empty())
                .configs(Map.of("cleanup.policy", "compact"));

        try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", bootstrapServers))) {
            Set<String> existentes = admin.listTopics().names().get();
            List<NewTopic> faltan = new ArrayList<>();
            for (NewTopic topic : List.of(canonico, descartes, tiposDeCambio)) {
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
                // Carrera con otro servicio que lo creo a la vez: no es un problema.
                log.info("[topics] otro servicio los creo antes");
            } else {
                log.error("[topics] no se pudieron declarar los topics: {}", ex.getCause().getMessage());
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }
}
