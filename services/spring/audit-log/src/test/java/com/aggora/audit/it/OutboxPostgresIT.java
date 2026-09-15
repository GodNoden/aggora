package com.aggora.audit.it;

import java.math.BigDecimal;
import java.time.Instant;

import javax.sql.DataSource;

import com.aggora.audit.store.AuditStore;
import com.aggora.avro.alerts.Alert;
import com.aggora.avro.alerts.AlertType;
import com.aggora.avro.alerts.Severity;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.ScriptUtils;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test de INTEGRACION del patron outbox, contra un Postgres de verdad.
 *
 * <p>Lo que se prueba aqui no lo puede probar un mock: que el SQL corra contra Postgres, que el
 * {@code schema.sql} cree las tablas y el indice unico, y sobre todo que **la ingesta sea
 * idempotente**. Si Kafka reentrega un mensaje (at-least-once), la segunda insercion no puede
 * repetir la auditoria: eso lo garantiza el indice unico de la tabla, no el codigo, y por eso hay
 * que probarlo contra la base de datos de verdad.
 *
 * <p>Se ejecuta con {@code mvn verify}, no con {@code mvn test}: levantar contenedores cuesta
 * segundos y no puede estar en el bucle de cada guardado.
 */
@Testcontainers
class OutboxPostgresIT {

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("aggora")
            .withUsername("aggora")
            .withPassword("aggora");

    private static AuditStore store;

    @BeforeAll
    static void prepararEsquema() throws Exception {
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setUrl(POSTGRES.getJdbcUrl());
        ds.setUser(POSTGRES.getUsername());
        ds.setPassword(POSTGRES.getPassword());

        // El MISMO schema.sql que ejecuta el servicio al arrancar: si aqui falla, en produccion
        // tambien.
        try (var con = ((DataSource) ds).getConnection()) {
            ScriptUtils.executeSqlScript(con, new ClassPathResource("schema.sql"));
        }
        store = new AuditStore(new JdbcTemplate(ds));
    }

    @Test
    @DisplayName("El mismo (topic, particion, offset) dos veces solo se audita una")
    void la_ingesta_es_idempotente() {
        Alert alerta = alerta("AAPL");
        ConsumerRecord<String, Object> record = new ConsumerRecord<>("alerts.raised", 3, 42L, "AAPL", (Object) alerta);
        String json = AuditStore.toJson(alerta);

        // Primera entrega: se guarda el evento Y su recado de publicacion.
        assertThat(store.append(record, alerta, json)).isTrue();
        // Reentrega de Kafka (at-least-once): el indice unico hace que no se repita.
        assertThat(store.append(record, alerta, json))
                .as("la segunda vez el evento ya estaba")
                .isFalse();

        assertThat(store.eventCount()).isEqualTo(1);
        assertThat(store.pendingCount()).as("un solo recado, no dos").isEqualTo(1);

        // El publicador lo manda y lo marca: la outbox queda vacia.
        long id = store.pending(10).getFirst().id();
        store.markPublished(id);
        assertThat(store.pendingCount()).isZero();
        assertThat(store.eventCount()).as("publicar no borra la auditoria").isEqualTo(1);
    }

    @Test
    @DisplayName("Dos eventos distintos se auditan los dos")
    void eventos_distintos_no_se_pisan() {
        store.append(new ConsumerRecord<>("alerts.raised", 3, 100L, "AAPL", (Object) alerta("AAPL")),
                alerta("AAPL"), AuditStore.toJson(alerta("AAPL")));
        store.append(new ConsumerRecord<>("alerts.raised", 3, 101L, "MSFT", (Object) alerta("MSFT")),
                alerta("MSFT"), AuditStore.toJson(alerta("MSFT")));

        assertThat(store.eventCount()).isEqualTo(2);
    }

    private static Alert alerta(String subject) {
        return Alert.newBuilder()
                .setAlertId("alert-" + subject)
                .setType(AlertType.PRICE_SPIKE)
                .setSeverity(Severity.WARNING)
                .setSubject(subject)
                .setDetail("pico de precio en " + subject)
                .setValue(new BigDecimal("200.0000"))
                .setThreshold(new BigDecimal("100.0000"))
                .setRaisedAt(Instant.now())
                .build();
    }
}
