package com.aggora.analytics.config;

import java.time.Duration;

/**
 * Una configuracion de mentira para los tests de topologia.
 *
 * <p>En Spring, {@code AggoraProperties} es un record y el test hace {@code new AggoraProperties(...)}
 * directamente. Aqui {@code AggoraConfig} es una <b>interfaz</b> (la implementa el build de Quarkus
 * a partir del yaml), asi que el test trae su propia implementacion: unos records con los mismos
 * accesores. Son los mismos valores que usa el test de la version Spring, para que las dos
 * implementaciones se prueben con lo mismo.
 */
public final class ConfigDePrueba {

    private ConfigDePrueba() {
    }

    public static AggoraConfig de(String schemaRegistryUrl, String canonical, String analytics,
                                  String arbitrage, String fxReference) {
        return new Fake(schemaRegistryUrl,
                new FakeTopics(canonical, analytics, arbitrage, fxReference),
                new FakeWindows(Duration.ofSeconds(30), Duration.ofSeconds(60),
                        Duration.ofSeconds(15), Duration.ofSeconds(5)),
                new FakeArbitrage("ASML", "ASML.AMS", "ASML", "USD", Duration.ofSeconds(5)));
    }

    record Fake(String schemaRegistryUrl, AggoraConfig.Topics topics,
                AggoraConfig.Windows windows, AggoraConfig.Arbitrage arbitrage) implements AggoraConfig {
    }

    record FakeTopics(String ticksCanonical, String analytics, String arbitrage, String fxReference)
            implements AggoraConfig.Topics {
    }

    record FakeWindows(Duration tumblingSize, Duration hoppingSize, Duration hoppingAdvance, Duration grace)
            implements AggoraConfig.Windows {
    }

    record FakeArbitrage(String rootSymbol, String europeanSymbol, String americanSymbol,
                         String quoteCurrency, Duration joinWindow) implements AggoraConfig.Arbitrage {
    }
}
