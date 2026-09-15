package com.aggora.analytics.config;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;

import com.aggora.analytics.topology.ArbitrageTopology;
import com.aggora.analytics.topology.MetricsTopology;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;

/**
 * La topologia, contada a Quarkus.
 *
 * <p>Esta es toda la diferencia de montaje con la version Spring, y es grande:
 *
 * <ul>
 *   <li>En Spring hay que anotar la clase de arranque con {@code @EnableKafkaStreams}, declarar un
 *       bean que recibe el {@code StreamsBuilder} y devuelve el stream final, y configurar el
 *       motor con {@code spring.kafka.streams.*}.</li>
 *   <li>Aqui se <b>produce un {@code Topology}</b> y ya esta: la extension de Quarkus lo configura
 *       con lo que diga {@code quarkus.kafka-streams.*}, arranca el motor, lo para al cerrar y
 *       expone el {@code KafkaStreams} como bean para las consultas interactivas.</li>
 * </ul>
 *
 * <p>El resto (las dos topologias, los serdes, la aritmetica) es codigo de Kafka Streams puro y se
 * copia tal cual: esa es la conclusion interesante de esta fase, que la API de Streams no cambia,
 * lo que cambia es quien la envuelve.
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
        // Los serdes leen la URL del registro de la configuracion; en los tests se les pasa una
        // "mock://" y la topologia se prueba sin broker ni registry.
        AvroSerdes serdes = new AvroSerdes(props.schemaRegistryUrl());

        // Las dos topologias viven en la MISMA aplicacion de Kafka Streams (un solo builder), asi
        // que comparten instancia, hilos y estado.
        ArbitrageTopology.apply(builder, props, serdes);
        MetricsTopology.apply(builder, props, serdes);

        return builder.build();
    }
}
