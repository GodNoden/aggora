package com.aggora.portfolio.config;

import java.math.BigDecimal;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "aggora")
public record AggoraProperties(
        String schemaRegistryUrl,
        Topics topics,
        BigDecimal marginLimit) {

    public record Topics(String executions, String portfolioUpdates) {
    }
}
