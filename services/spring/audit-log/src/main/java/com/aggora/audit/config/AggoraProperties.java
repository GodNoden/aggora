package com.aggora.audit.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "aggora")
public record AggoraProperties(
        String schemaRegistryUrl,
        Topics topics,
        long relayIntervalMs,
        int relayBatchSize) {

    public record Topics(
            String executions,
            String portfolioUpdates,
            String alerts,
            String auditEvents) {
    }
}
