package com.aggora.portfolio.config;

import com.aggora.avro.portfolio.PortfolioPosition;
import com.aggora.portfolio.topology.PortfolioTopology;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class PortfolioConfig {

    @Bean
    public AvroSerdes avroSerdes(AggoraProperties props) {
        return new AvroSerdes(props.schemaRegistryUrl());
    }

    @Bean
    public KStream<String, PortfolioPosition> portfolioTopology(StreamsBuilder builder,
                                                               AggoraProperties props,
                                                               AvroSerdes serdes) {
        return PortfolioTopology.apply(builder, props, serdes);
    }

    /** El topic de posiciones lo declara quien escribe en el. */
    @Bean
    public NewTopic portfolioUpdates(AggoraProperties props) {
        return TopicBuilder.name(props.topics().portfolioUpdates())
                .partitions(6)
                .build();
    }
}
