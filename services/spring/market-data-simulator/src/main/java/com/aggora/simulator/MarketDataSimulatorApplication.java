package com.aggora.simulator;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * market-data-simulator: produce ticks de mercado hacia market.ticks.raw.
 *
 * Dos fuentes de precio:
 *   - REFERENCE: el precio real que trae Twelve Data cada X minutos (ReferenceFeed).
 *   - SYNTHETIC: la interpolación de alta frecuencia que genera TickEngine entre
 *     referencias, que es la que realmente da volumen a Kafka.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
@EnableScheduling
public class MarketDataSimulatorApplication {

    public static void main(String[] args) {
        SpringApplication.run(MarketDataSimulatorApplication.class, args);
    }
}
