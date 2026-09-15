package com.aggora.matching.config;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.KafkaException;
import org.springframework.boot.kafka.autoconfigure.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;
import org.springframework.kafka.transaction.KafkaTransactionManager;

/**
 * El montaje de EXACTLY-ONCE.
 *
 * El productor se configura con un transactional.id (en el yml, con
 * spring.kafka.producer.transaction-id-prefix) y el consumidor lee con
 * isolation.level=read_committed, asi que solo ve lo que se ha confirmado.
 *
 * La pieza que lo une es este gestor de transacciones colgado del contenedor de
 * escucha: Spring Kafka abre una transaccion por cada poll, el listener publica las
 * ejecuciones dentro de ella y, al terminar, los offsets del consumidor se confirman
 * DENTRO de la misma transaccion (sendOffsetsToTransaction). Consecuencia practica:
 *
 *   - Si el listener termina bien: se publican las ejecuciones y avanza el offset.
 *   - Si el listener lanza una excepcion: se deshace todo. Las ejecuciones
 *     publicadas quedan abortadas (invisibles para los consumidores read_committed) y
 *     el offset NO avanza, asi que la orden se volvera a procesar.
 *
 * Sin esto tendrias at-least-once: se podria publicar la ejecucion y caerse antes de
 * confirmar el offset, y al reintentar saldria una ejecucion duplicada.
 */
@Configuration
public class KafkaTransactionConfig {

    @Bean
    public KafkaTransactionManager<Object, Object> kafkaTransactionManager(
            ProducerFactory<Object, Object> producerFactory) {
        return new KafkaTransactionManager<>(producerFactory);
    }

    /**
     * Fabrica de contenedores con la transaccion enganchada. Se llama igual que la de
     * Spring Boot para sustituirla, asi que todos los @KafkaListener la usan.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<Object, Object> kafkaListenerContainerFactory(
            ConsumerFactory<Object, Object> consumerFactory,
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            KafkaTransactionManager<Object, Object> transactionManager) {

        ConcurrentKafkaListenerContainerFactory<Object, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, consumerFactory);
        factory.getContainerProperties().setKafkaAwareTransactionManager(transactionManager);
        factory.setCommonErrorHandler(abortInsteadOfSkipping());
        return factory;
    }

    /**
     * Ojo con esto, que es una trampa clasica: el manejador de errores por defecto de
     * Spring Kafka reintenta unas cuantas veces y, si sigue fallando, DESCARTA el
     * mensaje (confirma el offset y sigue). Con transacciones, eso significa perder la
     * orden en silencio.
     *
     * Aqui se configura lo contrario: sin reintentos y relanzando la excepcion, para que
     * el contenedor deshaga la transaccion y el mensaje siga pendiente (se reprocesara).
     * En produccion, el destino de un mensaje que no se puede procesar es un topic de
     * descartes con su aviso (Fase 6), no el olvido.
     */
    @Bean
    public CommonErrorHandler abortInsteadOfSkipping() {
        return new DefaultErrorHandler(
                (record, exception) -> {
                    throw new KafkaException(
                            "no se puede procesar " + record.key() + ": se deshace la transaccion", exception);
                },
                new FixedBackOff(0L, 0L));
    }
}
