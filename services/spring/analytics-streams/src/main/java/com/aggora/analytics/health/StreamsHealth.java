package com.aggora.analytics.health;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

import org.apache.kafka.streams.KafkaStreams;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Component;

/**
 * La salud del MOTOR, que no es la salud del proceso.
 *
 * <p>Aqui se juntan dos cosas que en la Fase 6 se descubrieron por las malas: un servicio de
 * Kafka Streams puede estar vivo (el proceso responde, Tomcat escucha, el endpoint contesta)
 * y a la vez estar en estado ERROR sin procesar ni un mensaje. Mirar si el proceso existe no
 * dice NADA de si el servicio hace su trabajo.
 *
 * <p>Asi que se publican las dos caras:
 *
 * <ul>
 *   <li>{@code /actuator/health} baja a DOWN cuando el motor no esta procesando. Es lo que
 *       mira un orquestador (el healthcheck de Docker, la sonda de Kubernetes, el
 *       {@code restart: unless-stopped}) para decidir si el contenedor sirve o hay que
 *       levantar otro. Decir UP mientras no se procesa es peor que caerse.</li>
 *   <li>La metrica {@code aggora_kafka_streams_running} (1 o 0) va a Prometheus, para poder
 *       pintarla en Grafana y avisar cuando se ponga a 0.</li>
 * </ul>
 *
 * <p>El estado se lee del {@link KafkaStreams} que gestiona Spring, no de una copia: es el
 * mismo objeto que mueve el motor.
 */
@Component
public class StreamsHealth implements HealthIndicator {

    private final StreamsBuilderFactoryBean factories;

    public StreamsHealth(StreamsBuilderFactoryBean factories, MeterRegistry registry) {
        this.factories = factories;
        Gauge.builder("aggora_kafka_streams_running", this, StreamsHealth::procesando)
                .description("1 si Kafka Streams esta procesando (RUNNING o REBALANCING), 0 si esta en ERROR")
                .register(registry);
    }

    /** Se le pregunta al motor cada vez que se lee: el estado cambia solo. */
    private double procesando() {
        KafkaStreams streams = factories.getKafkaStreams();
        return streams != null && streams.state().isRunningOrRebalancing() ? 1 : 0;
    }

    @Override
    public Health health() {
        KafkaStreams streams = factories.getKafkaStreams();
        if (streams == null) {
            // Todavia no ha arrancado: ni bien ni mal, arrancando. La sonda de readiness
            // se queda esperando en vez de reiniciar el contenedor sin motivo.
            return Health.unknown().withDetail("estado", "arrancando").build();
        }
        Health.Builder salud = procesando() == 1 ? Health.up() : Health.down();
        return salud.withDetail("estado", streams.state().name()).build();
    }
}
