package com.aggora.matching.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "aggora")
public record AggoraProperties(
        String schemaRegistryUrl,
        Topics topics,
        int failEveryNOrders) {

    public record Topics(String ordersIncoming, String executions) {
    }
}
