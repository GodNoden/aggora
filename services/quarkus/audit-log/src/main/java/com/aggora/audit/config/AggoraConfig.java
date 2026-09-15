package com.aggora.audit.config;

import io.smallrye.config.ConfigMapping;

/**
 * Configuracion del servicio (application.yaml -> {@code aggora.*}), el MISMO bloque que usa la
 * version Spring.
 */
@ConfigMapping(prefix = "aggora")
public interface AggoraConfig {

    String schemaRegistryUrl();

    Topics topics();

    /** Cada cuanto mira el publicador si hay recados pendientes en la outbox. */
    long relayIntervalMs();

    int relayBatchSize();

    interface Topics {
        String executions();

        String portfolioUpdates();

        String alerts();

        String auditEvents();
    }
}
