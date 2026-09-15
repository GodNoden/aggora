package com.aggora.analytics.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * El topic de salida lo declara este servicio, porque es quien escribe en el.
 *
 * Kafka Streams crea por su cuenta los topics INTERNOS que necesita (los changelog de
 * los state stores y el de reparticion), asi que esos no se declaran aqui.
 */
@Configuration
public class KafkaTopicsConfig {

    // Los topics se declaran SIN replicas explicitas: el broker aplica su default
    // (KAFKA_DEFAULT_REPLICATION_FACTOR, que en el cluster de la Fase 6 es 3). Asi el mismo
    // codigo vale para un broker suelto o para tres, y no hay que tocar el codigo al
    // cambiar de tamano el cluster.

    @Bean
    public NewTopic analytics(AggoraProperties props) {
        return TopicBuilder.name(props.topics().analytics())
                .partitions(6)
                .build();
    }

    @Bean
    public NewTopic arbitrage(AggoraProperties props) {
        return TopicBuilder.name(props.topics().arbitrage())
                .partitions(6)
                .build();
    }
}
