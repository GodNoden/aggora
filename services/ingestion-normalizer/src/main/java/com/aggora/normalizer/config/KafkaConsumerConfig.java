package com.aggora.normalizer.config;

import com.aggora.normalizer.consumer.RebalanceLogger;

import org.springframework.boot.kafka.autoconfigure.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;

/**
 * Fábrica de contenedores de escucha.
 *
 * Hace falta una propia por un único motivo: colgarle el RebalanceLogger, que al ser
 * un callback del cliente y no una property no se puede configurar desde YAML.
 *
 * configurer.configure(...) aplica igualmente todo lo de spring.kafka.listener.*
 * (ack-mode=manual_immediate, concurrency, missing-topics-fatal...), así que la
 * configuración sigue viviendo en application.yml y no aquí.
 *
 * Los genéricos son <Object, Object> porque es lo que exige el configurer de Spring
 * Boot 3.5: el tipo real de los mensajes lo fijan los deserializadores del yml, no
 * el bean de la fábrica.
 */
@Configuration
public class KafkaConsumerConfig {

    @Bean
    public ConcurrentKafkaListenerContainerFactory<Object, Object> kafkaListenerContainerFactory(
            ConsumerFactory<Object, Object> consumerFactory,
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            RebalanceLogger rebalanceLogger) {

        ConcurrentKafkaListenerContainerFactory<Object, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, consumerFactory);
        factory.getContainerProperties().setConsumerRebalanceListener(rebalanceLogger);
        return factory;
    }
}
