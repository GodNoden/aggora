package com.aggora.portfolio.config;

import java.math.BigDecimal;

import io.smallrye.config.ConfigMapping;

/**
 * Configuracion del servicio (application.yaml -> {@code aggora.*}), el MISMO bloque que usa la
 * version Spring.
 */
@ConfigMapping(prefix = "aggora")
public interface AggoraConfig {

    String schemaRegistryUrl();

    Topics topics();

    /**
     * Limite de exposicion por posicion: si se supera, se marca {@code marginBreach} y
     * alerting-service levanta la alerta. En un sistema real seria por cuenta y por instrumento,
     * con reglas de margen de verdad.
     */
    BigDecimal marginLimit();

    interface Topics {
        String executions();

        String portfolioUpdates();
    }
}
