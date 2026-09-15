package com.aggora.analytics;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.kafka.annotation.EnableKafkaStreams;

/**
 * analytics-streams: lee el stream canonico de ticks y calcula metricas por ventana.
 *
 * Usa la API de Kafka Streams "a pelo" (Topology con KStream y KTable), que es la
 * misma que usara Quarkus en la Fase 8: asi la comparacion entre frameworks sera
 * justa. @EnableKafkaStreams solo se encarga de arrancar y parar Kafka Streams y de
 * dar el StreamsBuilder; la topologia la escribimos nosotros.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
@EnableKafkaStreams
public class AnalyticsStreamsApplication {

    public static void main(String[] args) {
        SpringApplication.run(AnalyticsStreamsApplication.class, args);
    }
}
