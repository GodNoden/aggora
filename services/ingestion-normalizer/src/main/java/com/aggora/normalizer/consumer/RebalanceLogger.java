package com.aggora.normalizer.consumer;

import java.util.Collection;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;
import org.springframework.stereotype.Component;

/**
 * Registra cada rebalanceo del grupo: qué particiones me tocan, cuáles suelto y
 * desde qué offset sigo.
 *
 * Esto es lo que hace visible el reparto: al arrancar una segunda instancia del
 * normalizador (mismo group.id), Kafka le quita particiones a la primera y el log
 * lo cuenta. Con más particiones que consumidores, un consumidor atiende varias.
 */
@Component
public class RebalanceLogger implements ConsumerAwareRebalanceListener {

    private static final Logger log = LoggerFactory.getLogger(RebalanceLogger.class);

    @Override
    public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        log.info("[rebalance] ASIGNADAS {} particiones: {}", partitions.size(), partitions);
        partitions.forEach(partition ->
                log.info("[rebalance]   {} -> continuaré desde offset {}", partition, consumer.position(partition)));
    }

    @Override
    public void onPartitionsRevokedBeforeCommit(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        log.info("[rebalance] REVOCADAS {} particiones (antes del commit): {}", partitions.size(), partitions);
    }

    @Override
    public void onPartitionsLost(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        log.warn("[rebalance] PERDIDAS {} particiones sin commit: {}", partitions.size(), partitions);
    }
}
