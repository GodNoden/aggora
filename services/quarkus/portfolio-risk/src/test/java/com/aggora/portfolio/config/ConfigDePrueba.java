package com.aggora.portfolio.config;

import java.math.BigDecimal;

/**
 * Una configuracion de mentira para el test de topologia: {@code AggoraConfig} es una interfaz
 * (la implementa el build de Quarkus), asi que el test trae su propia version con los mismos
 * valores que usa el test de la version Spring.
 */
public final class ConfigDePrueba {

    private ConfigDePrueba() {
    }

    public static AggoraConfig de(String schemaRegistryUrl, String executions,
                                  String portfolioUpdates, BigDecimal marginLimit) {
        return new Fake(schemaRegistryUrl, new FakeTopics(executions, portfolioUpdates), marginLimit);
    }

    record Fake(String schemaRegistryUrl, AggoraConfig.Topics topics, BigDecimal marginLimit)
            implements AggoraConfig {
    }

    record FakeTopics(String executions, String portfolioUpdates) implements AggoraConfig.Topics {
    }
}
