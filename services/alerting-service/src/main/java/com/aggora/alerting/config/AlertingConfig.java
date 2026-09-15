package com.aggora.alerting.config;

import com.aggora.alerting.topology.AlertingTopology;
import com.aggora.avro.alerts.Alert;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class AlertingConfig {

    @Bean
    public AvroSerdes avroSerdes(AggoraProperties props) {
        return new AvroSerdes(props.schemaRegistryUrl());
    }

    @Bean
    public KStream<String, Alert> alertingTopology(StreamsBuilder builder,
                                                   AggoraProperties props,
                                                   AvroSerdes serdes) {
        return AlertingTopology.apply(builder, props, serdes);
    }

    /** El topic de alertas lo declara quien escribe en el. */
    @Bean
    public NewTopic alertsRaised(AggoraProperties props) {
        return TopicBuilder.name(props.topics().alertsRaised())
                .partitions(3)
                .replicas(1)
                .build();
    }
}
