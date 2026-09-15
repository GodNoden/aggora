package com.aggora.portfolio.config;

import com.aggora.avro.portfolio.PortfolioPosition;
import com.aggora.portfolio.topology.PortfolioTopology;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class PortfolioConfig {

    private static final Logger log = LoggerFactory.getLogger(PortfolioConfig.class);

    /**
     * Si Kafka Streams se topa con un error que no puede manejar, por defecto **para** y deja
     * de procesar, mientras el proceso Java sigue vivo y el servicio parece sano. Con esto se
     * sustituye el hilo que fallo y el servicio se recupera solo.
     */
    @Bean
    public StreamsBuilderFactoryBeanConfigurer streamsResilience() {
        return factoryBean -> factoryBean.setStreamsUncaughtExceptionHandler(throwable -> {
            log.error("[streams] error no controlado ({}): se sustituye el hilo para seguir procesando",
                    throwable.getMessage());
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
        });
    }

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

    // Los topics se declaran SIN replicas explicitas: el broker aplica su default
    // (KAFKA_DEFAULT_REPLICATION_FACTOR, que en el cluster de la Fase 6 es 3). Asi el mismo
    // codigo vale para un broker suelto o para tres, y no hay que tocar el codigo al
    // cambiar de tamano el cluster.

    /** El topic de posiciones lo declara quien escribe en el. */
    @Bean
    public NewTopic portfolioUpdates(AggoraProperties props) {
        return TopicBuilder.name(props.topics().portfolioUpdates())
                .partitions(6)
                .build();
    }
}
