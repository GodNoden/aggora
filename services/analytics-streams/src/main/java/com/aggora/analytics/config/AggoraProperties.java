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
        Windows windows,
        Arbitrage arbitrage) {

    public record Topics(String ticksCanonical, String analytics, String arbitrage, String fxReference) {
    }

    /**
     * La pareja de cotizaciones que se cruzan.
     *
     * @param rootSymbol      la empresa, sin sufijo de mercado (ASML)
     * @param europeanSymbol  la cotizacion europea (ASML.AMS)
     * @param americanSymbol  la cotizacion americana (ASML)
     * @param quoteCurrency   divisa en la que se comparan los precios (USD)
     * @param joinWindow      cuanto pueden separarse los dos precios para considerarlos comparables
     */
    public record Arbitrage(
            String rootSymbol,
            String europeanSymbol,
            String americanSymbol,
            String quoteCurrency,
            Duration joinWindow) {
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
