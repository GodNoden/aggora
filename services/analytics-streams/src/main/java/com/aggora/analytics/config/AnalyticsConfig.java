package com.aggora.analytics.config;

import com.aggora.analytics.topology.ArbitrageTopology;
import com.aggora.analytics.topology.MetricsTopology;
import com.aggora.avro.analytics.SymbolMetrics;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer;

/**
 * Enganche de la topologia con Spring.
 *
 * @EnableKafkaStreams (en la clase principal) crea y arranca el StreamsBuilder y su
 * ciclo de vida. Aqui solo le decimos QUE construir: un bean que recibe el builder y
 * devuelve el stream final, que es la forma que documenta Spring Boot.
 */
@Configuration
public class AnalyticsConfig {

    private static final Logger log = LoggerFactory.getLogger(AnalyticsConfig.class);

    /**
     * Que hacer cuando Kafka Streams se encuentra un error que no puede manejar el solo.
     *
     * Por defecto, el cliente se para: pasa a estado ERROR y deja de procesar. Y aqui esta
     * lo peligroso, que se descubrio operando el cluster: **el proceso Java sigue vivo**, el
     * endpoint HTTP responde, y el servicio PARECE sano mientras no hace nada. Un fallo
     * silencioso, que es el peor tipo.
     *
     * Con este manejador se sustituye el hilo que fallo en vez de matar el cliente, asi que
     * el servicio se recupera solo (por ejemplo, cuando el hilo global de una GlobalKTable
     * muere porque el topic compactado se recreo y su checkpoint apunta a offsets que ya no
     * existen). Es la respuesta que recomienda la propia documentacion de Kafka Streams.
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
    public KStream<String, SymbolMetrics> analyticsTopology(StreamsBuilder builder,
                                                           AggoraProperties props,
                                                           AvroSerdes serdes) {
        // Las dos topologias viven en la MISMA aplicacion de Kafka Streams (un solo
        // StreamsBuilder), asi que comparten instancia, hilos y ciclo de vida.
        ArbitrageTopology.apply(builder, props, serdes);
        return MetricsTopology.apply(builder, props, serdes);
    }
}
