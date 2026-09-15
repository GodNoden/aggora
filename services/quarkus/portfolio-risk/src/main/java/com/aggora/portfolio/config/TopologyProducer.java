package com.aggora.portfolio.config;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;

import com.aggora.portfolio.topology.PortfolioTopology;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;

/**
 * La topologia, contada a Quarkus: se produce un {@code Topology} y la extension de Kafka Streams
 * se encarga de configurarlo, arrancarlo y pararlo. Es el mismo montaje que en
 * {@code analytics-streams}, y por eso el port de este servicio es casi todo copiar.
 */
@ApplicationScoped
public class TopologyProducer {

    private final AggoraConfig props;

    @Inject
    public TopologyProducer(AggoraConfig props) {
        this.props = props;
    }

    @Produces
    public Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        PortfolioTopology.apply(builder, props, new AvroSerdes(props.schemaRegistryUrl()));
        return builder.build();
    }
}
