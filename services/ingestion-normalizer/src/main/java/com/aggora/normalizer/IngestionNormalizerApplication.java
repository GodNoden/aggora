package com.aggora.normalizer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/**
 * ingestion-normalizer: lee market.ticks.raw, valida y normaliza.
 *
 * En la Fase 1 es "solo consumidor" (la republicación a market.ticks.canonical
 * llega en la Fase 2, cuando exista el esquema Avro del evento canónico). Lo que
 * se practica aquí es el consumo en JSON plano con COMMIT MANUAL de offsets.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class IngestionNormalizerApplication {

    public static void main(String[] args) {
        SpringApplication.run(IngestionNormalizerApplication.class, args);
    }
}
