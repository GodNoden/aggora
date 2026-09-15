package com.aggora.alerting.config;

import java.time.Duration;

import io.smallrye.config.ConfigMapping;

/**
 * Configuracion del servicio (application.yaml -> {@code aggora.*}), el MISMO bloque que usa la
 * version Spring. Los umbrales de histeresis y el tiempo de feed parado son los mismos valores.
 */
@ConfigMapping(prefix = "aggora")
public interface AggoraConfig {

    String schemaRegistryUrl();

    Topics topics();

    /** Desviacion minima (en puntos basicos) para considerar que un precio es un pico. */
    double priceSpikeBps();

    /** Histeresis: para volver a "normal" el precio tiene que bajar de aqui. */
    double priceSpikeRearmBps();

    /** Feed parado: si un simbolo lleva mas de este tiempo sin datos, se avisa. */
    Duration staleFeedTimeout();

    Duration staleFeedCheckInterval();

    interface Topics {
        String analytics();

        String portfolioUpdates();

        String alertsRaised();
    }
}
