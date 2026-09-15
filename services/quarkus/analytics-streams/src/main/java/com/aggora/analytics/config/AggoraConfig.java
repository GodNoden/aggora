package com.aggora.analytics.config;

import java.time.Duration;

import io.smallrye.config.ConfigMapping;

/**
 * Configuracion del servicio (application.yml -> {@code aggora.*}), el MISMO bloque que usa la
 * version Spring.
 *
 * <p>Es una interfaz con metodos de acceso y no un record, igual que en el simulador: en Quarkus
 * el mapeo lo genera el build.
 */
@ConfigMapping(prefix = "aggora")
public interface AggoraConfig {

    String schemaRegistryUrl();

    Topics topics();

    Windows windows();

    Arbitrage arbitrage();

    interface Topics {
        String ticksCanonical();

        String analytics();

        String arbitrage();

        String fxReference();
    }

    /**
     * Las ventanas son configurables a proposito: en dev son cortas (decenas de segundos) para
     * poder ver resultados mientras miras, y en un entorno real serian de minutos.
     *
     * @param tumblingSize   tamano de la ventana fija (no se solapa)
     * @param hoppingSize    tamano de la ventana deslizante
     * @param hoppingAdvance cada cuanto se recalcula la ventana deslizante
     * @param grace          margen que se espera antes de dar una ventana por cerrada
     */
    interface Windows {
        Duration tumblingSize();

        Duration hoppingSize();

        Duration hoppingAdvance();

        Duration grace();
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
    interface Arbitrage {
        String rootSymbol();

        String europeanSymbol();

        String americanSymbol();

        String quoteCurrency();

        Duration joinWindow();
    }
}
