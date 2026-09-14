package com.aggora.simulator.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Creación de topics desde el lado productor.
 *
 * Spring Boot detecta los beans NewTopic y los crea al arrancar (KafkaAdmin).
 * El topic se declara AQUÍ y no a mano por CLI para que las particiones queden
 * versionadas en el repo y el entorno sea reproducible.
 *
 * 6 particiones: es el número que fija el spec para dev y el techo de paralelismo
 * del consumidor (una partición solo la lee un consumidor del mismo grupo).
 */
@Configuration
public class KafkaTopicsConfig {

    @Bean
    public NewTopic rawTicks(AggoraProperties props) {
        return TopicBuilder.name(props.topics().ticksRaw())
                .partitions(6)
                .replicas(1)
                .build();
    }
}
