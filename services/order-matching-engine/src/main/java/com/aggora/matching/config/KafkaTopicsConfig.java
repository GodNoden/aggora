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

    /**
     * Topic de descartes de las ordenes que no se pueden procesar.
     *
     * Hay que declararlo: con la autocreacion de topics apagada (Fase 2), el publicador
     * del DLT no puede crearlo por su cuenta y falla con "unknown topic". Es la misma
     * regla de siempre: cada topic lo declara quien escribe en el.
     *
     * Una sola particion, como fija el spec: es un sitio para mirar y arreglar, no para
     * dar throughput.
     */
    @Bean
    public NewTopic ordersDeadLetter(AggoraProperties props) {
        return TopicBuilder.name(props.topics().ordersIncoming() + ".DLT")
                .partitions(1)
                .replicas(1)
                .build();
    }
}
