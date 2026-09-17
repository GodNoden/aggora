package com.aggora.analytics.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * CORS de SOLO LECTURA para la consulta interactiva (/analytics), porque el dashboard vive en otro
 * origen. La lista de origenes sale de {@code aggora.ui.allowed-origins} y nada de {@code *}.
 */
@Configuration
public class CorsConfig implements WebMvcConfigurer {

    private final String[] allowedOrigins;

    public CorsConfig(@Value("${aggora.ui.allowed-origins}") String[] allowedOrigins) {
        this.allowedOrigins = allowedOrigins;
    }

    @Override
    public void addCorsMappings(CorsRegistry registry) {
        registry.addMapping("/analytics")
                .allowedOrigins(allowedOrigins)
                .allowedMethods("GET");
    }
}
