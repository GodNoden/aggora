package com.aggora.gateway;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/** El gateway de la demo: de Kafka al navegador, en vivo y por WebSocket. */
@SpringBootApplication
@ConfigurationPropertiesScan
public class GatewayWsApplication {

    public static void main(String[] args) {
        SpringApplication.run(GatewayWsApplication.class, args);
    }
}
