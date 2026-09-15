package com.aggora.analytics.config;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuracion del servicio (application.yml -> aggora.*).
 *
 * Las ventanas son configurables a proposito: en dev son cortas (decenas de
 * segundos) para poder ver resultados mientras miras, y en un entorno real serian
 * de minutos.
 */
@ConfigurationProperties(prefix = "aggora")
public record AggoraProperties(
        String schemaRegistryUrl,
        Topics topics,
        Windows windows) {

    public record Topics(String ticksCanonical, String analytics) {
    }

    /**
     * @param tumblingSize   tamano de la ventana fija (no se solapa)
     * @param hoppingSize    tamano de la ventana deslizante
     * @param hoppingAdvance cada cuanto se recalcula la ventana deslizante
     * @param grace          margen que se espera antes de dar una ventana por cerrada
     */
    public record Windows(
            Duration tumblingSize,
            Duration hoppingSize,
            Duration hoppingAdvance,
            Duration grace) {
    }
}
