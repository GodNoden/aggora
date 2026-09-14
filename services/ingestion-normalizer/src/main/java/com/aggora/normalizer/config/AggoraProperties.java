package com.aggora.normalizer.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "aggora")
public record AggoraProperties(Topics topics, long processingDelayMs) {

    public record Topics(String ticksRaw) {
    }
}
