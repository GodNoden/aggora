package com.aggora.normalizer.config;


import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Topics que declara ESTE servicio.
 *
 * market.ticks.canonical lo crea el normalizer porque es su productor: la regla es
 * que cada topic lo declara quien escribe en él, y así las particiones quedan
 * versionadas en el repo.
 *
 * 6 particiones, igual que el crudo: la key sigue siendo el símbolo, así que el
 * reparto entre consumidores funciona igual.
 */
@Configuration
public class KafkaTopicsConfig {

    @Bean
    public NewTopic canonicalTicks(AggoraProperties props) {
        return TopicBuilder.name(props.topics().ticksCanonical())
                .partitions(6)
                .replicas(1)
                .build();
    }

    /**
     * Tipos de cambio como DATO DE REFERENCIA. Es un topic COMPACTADO: a quien lo lee
     * le interesa el ultimo valor de cada par, no el historial de todos los ticks. Kafka
     * se queda con una entrada por clave y va tirando las viejas. Una sola particion:
     * son 2 pares, no hace falta repartir.
     */
    @Bean
    public NewTopic fxReference(AggoraProperties props) {
        return TopicBuilder.name(props.topics().fxReference())
                .partitions(1)
                .replicas(1)
                .config("cleanup.policy", "compact")
                .build();
    }
}
