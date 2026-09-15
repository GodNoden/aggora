package com.aggora.analytics.config;

import com.aggora.analytics.topology.MetricsTopology;
import com.aggora.avro.analytics.SymbolMetrics;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Enganche de la topologia con Spring.
 *
 * @EnableKafkaStreams (en la clase principal) crea y arranca el StreamsBuilder y su
 * ciclo de vida. Aqui solo le decimos QUE construir: un bean que recibe el builder y
 * devuelve el stream final, que es la forma que documenta Spring Boot.
 */
@Configuration
public class AnalyticsConfig {

    @Bean
    public AvroSerdes avroSerdes(AggoraProperties props) {
        return new AvroSerdes(props.schemaRegistryUrl());
    }

    @Bean
    public KStream<String, SymbolMetrics> analyticsTopology(StreamsBuilder builder,
                                                           AggoraProperties props,
                                                           AvroSerdes serdes) {
        return MetricsTopology.apply(builder, props, serdes);
    }
}
