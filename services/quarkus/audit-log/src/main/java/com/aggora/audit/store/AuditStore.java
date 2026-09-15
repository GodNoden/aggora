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
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;

import javax.sql.DataSource;

/**
 * El corazon del patron outbox: guardar el evento y el recado de publicarlo en la MISMA
 * transaccion de base de datos.
 *
 * Nota sobre duplicados: si Kafka reentrega un mensaje (at-least-once), la insercion no
 * se repite porque hay un indice unico por (topic, particion, offset). Asi la tabla de
 * auditoria tambien es idempotente por su cuenta, sin depender de nadie.
 */
@ApplicationScoped
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

    private final DataSource dataSource;

    @Inject
    public AuditStore(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /**
     * Guarda el evento y su recado de publicacion. Las dos escrituras van en la misma
     * transaccion: o quedan las dos, o no queda ninguna.
     *
     * @return true si el evento era nuevo (false si ya estaba: reentrega)
     */
    @Transactional
    public boolean append(org.apache.kafka.clients.consumer.ConsumerRecord<String, ?> record, SpecificRecord event, String payloadJson) {
        String entityType = event.getSchema().getFullName();
        String entityId = entityIdOf(event);

        // Ojo: el driver de Postgres no sabe convertir un Instant por su cuenta; hay que
        // pasarle un OffsetDateTime. Es el tipico error que solo aparece en marcha.
        // JDBC a pelo, como en Spring con JdbcTemplate: la transaccion la abre @Transactional
        // (Narayana, que viene con el JDBC de Quarkus) y las dos escrituras van dentro.
        try (var con = dataSource.getConnection()) {
            int inserted;
            try (var ps = con.prepareStatement(INSERT_EVENT)) {
                ps.setString(1, record.topic());
                ps.setInt(2, record.partition());
                ps.setLong(3, record.offset());
                ps.setString(4, entityType);
                ps.setString(5, entityId);
                ps.setString(6, payloadJson);
                // Ojo: el driver de Postgres no sabe convertir un Instant por su cuenta; hay que
                // pasarle un OffsetDateTime. Es el tipico error que solo aparece en marcha.
                ps.setObject(7, OffsetDateTime.ofInstant(occurredAtOf(event), ZoneOffset.UTC));
                inserted = ps.executeUpdate();
            }
            if (inserted == 0) {
                log.debug("[auditoria] ya estaba: {} {} offset {}", record.topic(), record.partition(), record.offset());
                return false;
            }
            try (var ps = con.prepareStatement(INSERT_OUTBOX)) {
                ps.setString(1, entityType);
                ps.setString(2, entityId);
                ps.setString(3, payloadJson);
                ps.executeUpdate();
            }
        } catch (java.sql.SQLException ex) {
            throw new IllegalStateException("no se pudo auditar " + entityType + " " + entityId, ex);
        }
        return true;
    }

    public List<OutboxMessage> pending(int limit) {
        List<OutboxMessage> pendientes = new java.util.ArrayList<>();
        try (var con = dataSource.getConnection();
             var ps = con.prepareStatement(SELECT_PENDING)) {
            ps.setInt(1, limit);
            try (var rs = ps.executeQuery()) {
                while (rs.next()) {
                    pendientes.add(new OutboxMessage(
                            rs.getLong("id"), rs.getString("entity_type"), rs.getString("entity_id"),
                            rs.getString("payload"), rs.getTimestamp("created_at").toInstant()));
                }
            }
        } catch (java.sql.SQLException ex) {
            throw new IllegalStateException("no se pudo leer la outbox", ex);
        }
        return pendientes;
    }

    public void markPublished(long id) {
        try (var con = dataSource.getConnection();
             var ps = con.prepareStatement(MARK_PUBLISHED)) {
            ps.setLong(1, id);
            ps.executeUpdate();
        } catch (java.sql.SQLException ex) {
            throw new IllegalStateException("no se pudo marcar como publicado " + id, ex);
        }
    }

    public long eventCount() {
        return cuenta("select count(*) from audit_events");
    }

    public long pendingCount() {
        return cuenta("select count(*) from audit_outbox where published_at is null");
    }

    private long cuenta(String sql) {
        try (var con = dataSource.getConnection();
             var ps = con.prepareStatement(sql);
             var rs = ps.executeQuery()) {
            return rs.next() ? rs.getLong(1) : 0L;
        } catch (java.sql.SQLException ex) {
            throw new IllegalStateException("no se pudo contar", ex);
        }
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
        } catch (IOException ex) {
            throw new IllegalStateException("no se pudo serializar " + event.getSchema().getFullName(), ex);
        }
    }
}
