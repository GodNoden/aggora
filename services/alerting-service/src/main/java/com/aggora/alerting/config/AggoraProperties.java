package com.aggora.alerting.config;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "aggora")
public record AggoraProperties(
        String schemaRegistryUrl,
        Topics topics,
        double priceSpikeBps,
        double priceSpikeRearmBps,
        Duration staleFeedTimeout,
        Duration staleFeedCheckInterval) {

    public record Topics(String analytics, String portfolioUpdates, String alertsRaised) {
    }
}
