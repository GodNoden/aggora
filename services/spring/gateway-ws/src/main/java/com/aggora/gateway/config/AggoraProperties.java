package com.aggora.gateway.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "aggora")
public record AggoraProperties(String schemaRegistryUrl, Topics topics) {

    public record Topics(String ticksCanonical, String portfolioUpdates, String alerts) {
    }
}
