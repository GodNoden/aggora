package com.aggora.normalizer;

import java.util.Collection;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Named;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.smallrye.reactive.messaging.kafka.KafkaConsumerRebalanceListener;

/**
 * Cuenta en el log lo que pasa en cada rebalanceo, igual que la version Spring.
 *
 * <p>Es una de las cosas que se vienen a aprender con este proyecto: cuando entra o sale una
 * instancia, el grupo reparte las particiones otra vez y hay un rato en el que nadie consume
 * de las que estan en movimiento. Verlo en el log es la diferencia entre entenderlo y creerselo.
 *
 * <p>Ojo con la interfaz: SmallRye no usa el {@code ConsumerRebalanceListener} de Kafka, tiene
 * el suyo ({@link KafkaConsumerRebalanceListener}), que ademas pasa el {@code Consumer} en cada
 * evento (util para mirar o mover offsets desde dentro). Se engancha con
 * {@code mp.messaging.incoming.ticks-raw.consumer-rebalance-listener.name=rebalance-log}: ese
 * valor es el nombre del bean de CDI, de ahi el {@code @Named}.
 */
@ApplicationScoped
@Named("rebalance-log")
public class RebalanceLog implements KafkaConsumerRebalanceListener {

    private static final Logger log = LoggerFactory.getLogger(RebalanceLog.class);

    @Override
    public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> particiones) {
        log.info("[rebalance] ASIGNADAS {} particiones: {}", particiones.size(), particiones);
    }

    @Override
    public void onPartitionsRevoked(Consumer<?, ?> consumer, Collection<TopicPartition> particiones) {
        log.info("[rebalance] REVOCADAS {} particiones: {}", particiones.size(), particiones);
    }

    @Override
    public void onPartitionsLost(Consumer<?, ?> consumer, Collection<TopicPartition> particiones) {
        log.warn("[rebalance] PERDIDAS {} particiones sin commit: {}", particiones.size(), particiones);
    }
}
