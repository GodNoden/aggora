package com.aggora.portfolio;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.kafka.annotation.EnableKafkaStreams;

/**
 * portfolio-risk: convierte el flujo de ejecuciones en posiciones vivas por cuenta.
 *
 * La clave del diseno: una ejecucion NO es un movimiento de una cuenta, son DOS. El
 * comprador suma y el vendedor resta. Por eso la topologia "abre" cada ejecucion en dos
 * movimientos, los vuelve a agrupar por cuenta+simbolo y los va acumulando en una KTable:
 * el estado de cada posicion es el resultado de aplicar todos sus movimientos en orden.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
@EnableKafkaStreams
public class PortfolioRiskApplication {

    public static void main(String[] args) {
        SpringApplication.run(PortfolioRiskApplication.class, args);
    }
}
