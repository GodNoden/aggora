package com.aggora.audit.config;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Statement;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.quarkus.runtime.StartupEvent;

/**
 * Crea las tablas al arrancar, ejecutando el MISMO {@code schema.sql} que la version Spring.
 *
 * <p>En Spring lo hace {@code spring.sql.init.mode=always}. Quarkus no ejecuta el schema por su
 * cuenta (el sitio natural seria Flyway), asi que se lee del classpath y se ejecuta. Es idempotente
 * porque todo el fichero es {@code create ... if not exists}: arrancar dos veces no rompe nada.
 */
@ApplicationScoped
public class SchemaInit {

    private static final Logger log = LoggerFactory.getLogger(SchemaInit.class);

    private final DataSource dataSource;

    @Inject
    public SchemaInit(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    void alArrancar(@Observes StartupEvent evento) {
        String sql;
        try (var in = Thread.currentThread().getContextClassLoader().getResourceAsStream("schema.sql")) {
            if (in == null) {
                throw new IllegalStateException("no encuentro schema.sql en el classpath");
            }
            sql = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (java.io.IOException ex) {
            throw new IllegalStateException("no se pudo leer schema.sql", ex);
        }

        try (Connection con = dataSource.getConnection(); Statement st = con.createStatement()) {
            for (String sentencia : sql.split(";")) {
                String limpia = sentencia.lines().filter(l -> !l.trim().startsWith("--")).reduce("", (a, b) -> a + "\n" + b);
                if (!limpia.isBlank()) {
                    st.execute(limpia);
                }
            }
            log.info("[esquema] tablas de auditoria listas (audit_events, audit_outbox)");
        } catch (java.sql.SQLException ex) {
            throw new IllegalStateException("no se pudo crear el esquema de auditoria", ex);
        }
    }
}
