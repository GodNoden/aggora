package com.aggora.alerting.config;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;

import com.aggora.alerting.topology.AlertingTopology;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;

/**
 * La topologia, contada a Quarkus: se produce un {@code Topology} y la extension de Kafka Streams
 * lo configura, lo arranca y lo para. Los dos detectores (el pico por ventana y el feed parado con
 * punctuator) son codigo de Kafka Streams puro y se copian tal cual.
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
        AlertingTopology.apply(builder, props, new AvroSerdes(props.schemaRegistryUrl()));
        return builder.build();
    }
}
