package com.aggora.audit.relay;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import com.aggora.audit.config.AggoraProperties;
import com.aggora.audit.store.AuditStore;
import com.aggora.audit.store.OutboxMessage;
import com.aggora.avro.audit.AuditEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * El publicador: lee los recados pendientes de la tabla outbox y los manda al topic
 * compactado audit.events.
 *
 * Por que asi y no publicando directamente al consumir el evento: porque entonces la
 * escritura en base de datos y la publicacion serian dos operaciones sueltas y una caida
 * en medio dejaria el sistema descoordinado. Aqui la base de datos es la fuente de verdad
 * (el recado ya esta guardado) y la publicacion se puede reintentar sin miedo.
 *
 * Si el proceso se cae despues de publicar y antes de marcar, al volver a arrancar lo
 * publica otra vez: at-least-once. En este topic no hace dano, porque es COMPACTADO y la
 * clave es la entidad: el duplicado se queda como el mismo ultimo estado.
 *
 * Los offsets de origen (topic, particion, offset) viajan dentro del evento para que se
 * pueda rastrear de donde salio cada registro auditado.
 */
@Component
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    private final AuditStore store;
    private final KafkaTemplate<String, AuditEvent> auditTemplate;
    private final AggoraProperties props;

    public OutboxRelay(AuditStore store, KafkaTemplate<String, AuditEvent> auditTemplate, AggoraProperties props) {
        this.store = store;
        this.auditTemplate = auditTemplate;
        this.props = props;
    }

    @Scheduled(fixedDelayString = "${aggora.relay-interval-ms}")
    public void publishPending() {
        List<OutboxMessage> pending = store.pending(props.relayBatchSize());
        if (pending.isEmpty()) {
            return;
        }
        for (OutboxMessage message : pending) {
            auditTemplate.send(props.topics().auditEvents(), message.entityId(), toAuditEvent(message));
            store.markPublished(message.id());
        }
        log.info("[outbox] {} eventos publicados | pendientes ahora: {}", pending.size(), store.pendingCount());
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
