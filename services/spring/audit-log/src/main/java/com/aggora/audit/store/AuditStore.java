package com.aggora.audit.store;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;

import com.aggora.avro.alerts.Alert;
import com.aggora.avro.orders.Execution;
import com.aggora.avro.portfolio.PortfolioPosition;

import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

/**
 * El corazon del patron outbox: guardar el evento y el recado de publicarlo en la MISMA
 * transaccion de base de datos.
 *
 * Nota sobre duplicados: si Kafka reentrega un mensaje (at-least-once), la insercion no
 * se repite porque hay un indice unico por (topic, particion, offset). Asi la tabla de
 * auditoria tambien es idempotente por su cuenta, sin depender de nadie.
 */
@Repository
public class AuditStore {

    private static final Logger log = LoggerFactory.getLogger(AuditStore.class);

    private static final String INSERT_EVENT = """
            insert into audit_events
                (source_topic, source_partition, source_offset, entity_type, entity_id, payload, occurred_at)
            values (?, ?, ?, ?, ?, ?, ?)
            on conflict (source_topic, source_partition, source_offset) do nothing
            """;

    private static final String INSERT_OUTBOX = """
            insert into audit_outbox (entity_type, entity_id, payload)
            values (?, ?, ?)
            """;

    private static final String SELECT_PENDING = """
            select id, entity_type, entity_id, payload, created_at
            from audit_outbox
            where published_at is null
            order by id
            limit ?
            """;

    private static final String MARK_PUBLISHED = """
            update audit_outbox set published_at = now() where id = ?
            """;

    private final JdbcTemplate jdbc;

    public AuditStore(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Guarda el evento y su recado de publicacion. Las dos escrituras van en la misma
     * transaccion: o quedan las dos, o no queda ninguna.
     *
     * @return true si el evento era nuevo (false si ya estaba: reentrega)
     */
    @Transactional
    public boolean append(ConsumerRecord<String, ?> record, SpecificRecord event, String payloadJson) {
        String entityType = event.getSchema().getFullName();
        String entityId = entityIdOf(event);

        // Ojo: el driver de Postgres no sabe convertir un Instant por su cuenta; hay que
        // pasarle un OffsetDateTime. Es el tipico error que solo aparece en marcha.
        int inserted = jdbc.update(INSERT_EVENT,
                record.topic(), record.partition(), record.offset(),
                entityType, entityId, payloadJson,
                OffsetDateTime.ofInstant(occurredAtOf(event), ZoneOffset.UTC));
        if (inserted == 0) {
            log.debug("[auditoria] ya estaba: {} {} offset {}", record.topic(), record.partition(), record.offset());
            return false;
        }
        jdbc.update(INSERT_OUTBOX, entityType, entityId, payloadJson);
        return true;
    }

    public List<OutboxMessage> pending(int limit) {
        return jdbc.query(SELECT_PENDING,
                (rs, rowNum) -> new OutboxMessage(
                        rs.getLong("id"),
                        rs.getString("entity_type"),
                        rs.getString("entity_id"),
                        rs.getString("payload"),
                        rs.getTimestamp("created_at").toInstant()),
                limit);
    }

    public void markPublished(long id) {
        jdbc.update(MARK_PUBLISHED, id);
    }

    public long eventCount() {
        Long count = jdbc.queryForObject("select count(*) from audit_events", Long.class);
        return count == null ? 0 : count;
    }

    public long pendingCount() {
        Long count = jdbc.queryForObject("select count(*) from audit_outbox where published_at is null", Long.class);
        return count == null ? 0 : count;
    }

    static String entityIdOf(SpecificRecord event) {
        return switch (event) {
            case Execution execution -> execution.getSymbol();
            case PortfolioPosition position -> position.getAccountId();
            case Alert alert -> alert.getSubject();
            default -> "desconocido";
        };
    }

    static Instant occurredAtOf(SpecificRecord event) {
        return switch (event) {
            case Execution execution -> execution.getExecutedAt();
            case PortfolioPosition position -> position.getLastUpdated();
            case Alert alert -> alert.getRaisedAt();
            default -> Instant.now();
        };
    }

    /**
     * El evento a JSON con el codificador de Avro. Se guarda asi, y no en binario, porque
     * una auditoria se lee: con SQL se entiende que paso sin descodificar nada.
     */
    public static String toJson(SpecificRecord event) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            var encoder = EncoderFactory.get().jsonEncoder(event.getSchema(), out);
            new SpecificDatumWriter<SpecificRecord>(event.getSchema()).write(event, encoder);
            encoder.flush();
            return out.toString(StandardCharsets.UTF_8);
        } catch (IOException | DataAccessException ex) {
            throw new IllegalStateException("no se pudo serializar " + event.getSchema().getFullName(), ex);
        }
    }
}
