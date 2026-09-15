package com.aggora.audit.consumer;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import com.aggora.audit.store.AuditStore;
import com.aggora.avro.alerts.Alert;
import com.aggora.avro.orders.Execution;
import com.aggora.avro.portfolio.PortfolioPosition;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.smallrye.common.annotation.Blocking;
import io.smallrye.reactive.messaging.kafka.KafkaRecord;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;

/**
 * Guarda en Postgres todo lo que pasa por los tres topics, con el patron outbox.
 *
 * <p><b>Diferencia con Spring que se ve en el codigo:</b> alli hay UN {@code @KafkaListener} con los
 * tres topics y el valor se declara como {@code Object}, porque los tres traen tipos Avro distintos.
 * En SmallRye cada canal tiene su tipo, asi que hay un metodo por topic (mas seguro de tipos) que
 * delega en el comun.
 *
 * <p>La idempotencia no la da este codigo: la da el indice unico por (topic, particion, offset) de
 * la tabla. Si Kafka reentrega, la insercion no se repite.
 */
@ApplicationScoped
public class AuditConsumer {

    private static final Logger log = LoggerFactory.getLogger(AuditConsumer.class);

    private final AuditStore store;
    private final AtomicLong stored = new AtomicLong();
    private final AtomicLong duplicates = new AtomicLong();

    @Inject
    public AuditConsumer(AuditStore store) {
        this.store = store;
    }

    @Incoming("executions")
    @Blocking
    public CompletionStage<Void> onExecution(KafkaRecord<String, Execution> record) {
        return guardar(record, record.getPayload());
    }

    @Incoming("portfolio-updates")
    @Blocking
    public CompletionStage<Void> onPosition(KafkaRecord<String, PortfolioPosition> record) {
        return guardar(record, record.getPayload());
    }

    @Incoming("alerts")
    @Blocking
    public CompletionStage<Void> onAlert(KafkaRecord<String, Alert> record) {
        return guardar(record, record.getPayload());
    }

    /**
     * El topic, la particion y el offset NO estan en el propio {@code KafkaRecord}: viajan en los
     * metadatos del mensaje entrante (la misma leccion que en el normalizer).
     */
    private CompletionStage<Void> guardar(KafkaRecord<String, ?> record,
                                          org.apache.avro.specific.SpecificRecord event) {
        IncomingKafkaRecordMetadata<String, ?> meta = record.getMetadata(IncomingKafkaRecordMetadata.class)
                .orElseThrow(() -> new IllegalStateException("el mensaje no trae metadatos de Kafka"));
        String topic = meta.getTopic();
        int partition = meta.getPartition();
        long offset = meta.getOffset();
        boolean isNew = store.append(new org.apache.kafka.clients.consumer.ConsumerRecord<>(
                topic, partition, offset, record.getKey(), event), event, AuditStore.toJson(event));
        if (isNew) {
            long total = stored.incrementAndGet();
            if (total % 500 == 0) {
                log.info("[auditoria] {} guardados ({} ya estaban) | ultimo: {} part={} offset={}",
                        total, duplicates.get(), topic, partition, offset);
            }
        } else {
            duplicates.incrementAndGet();
        }
        return record.ack();
    }

    public long storedCount() {
        return stored.get();
    }
}
