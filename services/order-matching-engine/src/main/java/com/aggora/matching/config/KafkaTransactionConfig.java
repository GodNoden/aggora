package com.aggora.matching.config;

import org.apache.kafka.common.TopicPartition;
import org.springframework.boot.kafka.autoconfigure.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
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
            KafkaTransactionManager<Object, Object> transactionManager,
            KafkaTemplate<String, Object> kafkaTemplate) {

        ConcurrentKafkaListenerContainerFactory<Object, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, consumerFactory);
        factory.getContainerProperties().setKafkaAwareTransactionManager(transactionManager);
        factory.setCommonErrorHandler(errorHandler(kafkaTemplate));
        return factory;
    }

    /**
     * Que hacer cuando una orden no se puede procesar.
     *
     * En la Fase 4 se decidio lo minimo: deshacer la transaccion y dejar el mensaje
     * pendiente, para no descartarlo en silencio. El problema de aquello, que se ve al
     * pensarlo dos veces, es que un mensaje imposible bloquea SU PARTICION para siempre:
     * el consumidor se queda reintentando lo mismo y todo lo que venga detras espera.
     *
     * Ahora: dos reintentos cortos y, si sigue fallando, al topic de descartes CON el
     * motivo. La particion sigue avanzando y el mensaje problematico queda a la vista.
     *
     * El publicador del DLT usa la MISMA plantilla transaccional, que es lo que pide
     * Spring Kafka cuando el contenedor es transaccional: asi la publicacion al DLT y el
     * commit del offset van juntos y no se puede dar el caso de "descartado pero no
     * confirmado" (ni al reves).
     */
    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<String, Object> template) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template,
                (record, exception) -> new TopicPartition(record.topic() + ".DLT", -1));
        return new DefaultErrorHandler(recoverer, new FixedBackOff(200L, 2L));
    }
}
