package com.aggora.matching.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * El topic de ejecuciones lo declara este servicio, que es quien escribe en el.
 * El de ordenes entrantes lo declara el simulador, que es su productor.
 */
@Configuration
public class KafkaTopicsConfig {

    @Bean
    public NewTopic executions(AggoraProperties props) {
        return TopicBuilder.name(props.topics().executions())
                .partitions(6)
                .replicas(1)
                .build();
    }
}
