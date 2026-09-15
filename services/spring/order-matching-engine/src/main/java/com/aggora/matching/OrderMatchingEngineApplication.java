package com.aggora.matching;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/**
 * order-matching-engine: mantiene un libro de ordenes por instrumento, cruza las
 * ordenes que se pueden cruzar y publica las ejecuciones con EXACTLY-ONCE.
 *
 * La idea de exactly-once en este servicio: leer una orden, cruzarla y publicar la
 * ejecucion tiene que ser todo o nada. Si el proceso se cae despues de publicar la
 * ejecucion pero antes de confirmar el offset de la orden, al reiniciar se cruzaria
 * otra vez y saldria una ejecucion duplicada. Con una transaccion de Kafka, la
 * ejecucion publicada y el offset confirmado viajan en el mismo commit: o los dos, o
 * ninguno. Eso es lo que se monta en KafkaTransactionConfig.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class OrderMatchingEngineApplication {

    public static void main(String[] args) {
        SpringApplication.run(OrderMatchingEngineApplication.class, args);
    }
}
