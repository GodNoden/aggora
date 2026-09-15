package com.aggora.audit.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicsConfig {

    /**
     * El topic de auditoria. Dos cosas importantes:
     *
     *  - COMPACTADO: la clave es la entidad, y de cada entidad interesa su ultimo estado,
     *    no todo el historial de cambios. Kafka va tirando los valores viejos de cada
     *    clave. Es lo que permite que un publicador que repita un mensaje no haga dano.
     *  - 3 particiones (como fija el spec), suficiente para el volumen de auditoria.
     */
    // Los topics se declaran SIN replicas explicitas: el broker aplica su default
    // (KAFKA_DEFAULT_REPLICATION_FACTOR, que en el cluster de la Fase 6 es 3). Asi el mismo
    // codigo vale para un broker suelto o para tres, y no hay que tocar el codigo al
    // cambiar de tamano el cluster.

    @Bean
    public NewTopic auditEvents(AggoraProperties props) {
        return TopicBuilder.name(props.topics().auditEvents())
                .partitions(3)
                .config("cleanup.policy", "compact")
                .build();
    }
}
