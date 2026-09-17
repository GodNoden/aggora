package com.aggora.gateway.config;

import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuracion del servicio (application.yml -> aggora.*).
 *
 * @param stack          nombre de esta implementacion ("spring"), para que el dashboard pueda
 *                       comparar las dos series en el mismo panel
 * @param prometheusUrl  base de Prometheus, de la que el gateway hace de proxy en /api/metrics
 * @param ui             de donde puede leer el dashboard
 */
@ConfigurationProperties(prefix = "aggora")
public record AggoraProperties(
        String schemaRegistryUrl,
        String stack,
        String prometheusUrl,
        Ui ui,
        Topics topics) {

    /** Allowlist de origenes para CORS. Solo lectura: el dashboard nunca escribe nada. */
    public record Ui(List<String> allowedOrigins) {
    }

    public record Topics(String ticksCanonical, String portfolioUpdates, String alerts) {
    }
}
