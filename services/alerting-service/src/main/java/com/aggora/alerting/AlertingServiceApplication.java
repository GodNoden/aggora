package com.aggora.alerting;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.kafka.annotation.EnableKafkaStreams;

/**
 * alerting-service: vigila el mercado y la cartera y levanta alertas.
 *
 * Tres reglas, y la tercera es la interesante porque detecta AUSENCIA de datos:
 *   - pico de precio: el ultimo precio se aleja de la media de su ventana
 *   - margen superado: lo marca portfolio-risk en la posicion
 *   - feed parado: un simbolo lleva demasiado tiempo sin dar senales (punctuator)
 */
@SpringBootApplication
@ConfigurationPropertiesScan
@EnableKafkaStreams
public class AlertingServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(AlertingServiceApplication.class, args);
    }
}
