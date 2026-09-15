package com.aggora.alerting.config;

import java.time.Duration;

/**
 * Una configuracion de mentira para el test de topologia, con los MISMOS valores que usa el test
 * de la version Spring (40 bps para disparar, 20 para rearmar, 30 s de feed parado). En Quarkus
 * {@code AggoraConfig} es una interfaz, asi que el test trae su propia implementacion.
 */
public final class ConfigDePrueba {

    private ConfigDePrueba() {
    }

    public static AggoraConfig de(String schemaRegistryUrl, String analytics,
                                  String portfolioUpdates, String alertsRaised) {
        return new Fake(schemaRegistryUrl, new FakeTopics(analytics, portfolioUpdates, alertsRaised),
                40.0, 20.0, Duration.ofSeconds(30), Duration.ofSeconds(10));
    }

    record Fake(String schemaRegistryUrl, AggoraConfig.Topics topics, double priceSpikeBps,
                double priceSpikeRearmBps, Duration staleFeedTimeout, Duration staleFeedCheckInterval)
            implements AggoraConfig {
    }

    record FakeTopics(String analytics, String portfolioUpdates, String alertsRaised)
            implements AggoraConfig.Topics {
    }
}
