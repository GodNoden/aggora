package com.aggora.audit.relay;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import com.aggora.audit.config.AggoraConfig;
import com.aggora.audit.store.AuditStore;
import com.aggora.audit.store.OutboxMessage;
import com.aggora.avro.audit.AuditEvent;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.quarkus.scheduler.Scheduled;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.smallrye.reactive.messaging.kafka.KafkaRecord;

/**
 * El publicador: lee los recados pendientes de la tabla outbox y los manda al topic compactado
 * {@code audit.events}.
 *
 * <p>Es el otro lado del patron: el evento se guardo en Postgres junto a su recado en la MISMA
 * transaccion (ver {@link AuditStore#append}), y este job hace la parte que puede fallar sin
 * perderse, porque el recado sigue en la tabla hasta que se publica. Si el servicio se cae entre
 * publicar y marcar, al volver lo publica otra vez: at-least-once. En este topic no hace dano,
 * porque es COMPACTADO y la clave es el entityId, asi que quien lo lea se queda con el ultimo.
 */
@ApplicationScoped
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    private final AuditStore store;
    private final MutinyEmitter<AuditEvent> auditEmitter;
    private final long intervalMs;
    private final int batchSize;

    @Inject
    public OutboxRelay(AuditStore store,
                       @Channel("audit-events") MutinyEmitter<AuditEvent> auditEmitter,
                       AggoraConfig props) {
        this.store = store;
        this.auditEmitter = auditEmitter;
        this.intervalMs = props.relayIntervalMs();
        this.batchSize = props.relayBatchSize();
    }

    /** {@code every} + SKIP en vez de {@code fixedDelay}: si una vuelta tarda, la siguiente espera. */
    @Scheduled(every = "${aggora.relay-interval-ms:1000}ms",
            concurrentExecution = Scheduled.ConcurrentExecution.SKIP,
            skipExecutionIf = Scheduled.ApplicationNotRunning.class)
    public void publishPending() {
        List<OutboxMessage> pending = store.pending(batchSize);
        if (pending.isEmpty()) {
            return;
        }
        // Se cuenta ANTES de publicar: el envio es asincrono, asi que si se contara despues
        // saldrian los que todavia no se han marcado y pareceria que la outbox no se vacia.
        long pendientesAntes = store.pendingCount();
        for (OutboxMessage message : pending) {
            // La clave es el entityId: el topic es compactado y asi cada entidad se queda con su
            // ultimo evento.
            auditEmitter.sendMessage(KafkaRecord.of(message.entityId(), toAuditEvent(message)))
                    .subscribe().with(
                            ignorado -> store.markPublished(message.id()),
                            ex -> log.error("[outbox] no se pudo publicar {}: {}", message.id(), ex.getMessage()));
        }
        log.info("[outbox] {} eventos publicados (pendientes antes: {}) | quedan {} por publicar",
                pending.size(), pendientesAntes, store.pending(batchSize).size());
    }

    private AuditEvent toAuditEvent(OutboxMessage message) {
        return AuditEvent.newBuilder()
                .setAuditId(UUID.randomUUID().toString())
                .setEntityType(message.entityType())
                .setEntityId(message.entityId())
                .setSourceTopic("outbox")
                .setSourcePartition(0)
                .setSourceOffset(message.id())
                .setPayload(message.payload())
                .setOccurredAt(message.createdAt())
                .setRecordedAt(Instant.now())
                .build();
    }
}
