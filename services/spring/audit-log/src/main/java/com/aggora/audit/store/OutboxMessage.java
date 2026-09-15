package com.aggora.audit.store;

import java.time.Instant;

/** Una fila pendiente de publicar de la tabla outbox. */
public record OutboxMessage(
        long id,
        String entityType,
        String entityId,
        String payload,
        Instant createdAt) {
}
