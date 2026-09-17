package com.aggora.gateway.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * CORS de SOLO LECTURA para la API HTTP del gateway (el dashboard vive en otro origen).
 *
 * <p>La lista de origenes permitidos sale de {@code aggora.ui.allowed-origins} y por defecto son
 * los puertos de desarrollo. Nada de {@code *}: en cuanto esto se publica detras de TLS, el
 * comodin deja entrar a cualquiera.
 *
 * <p>El handshake del WebSocket NO se toca: {@code WebSocketConfig} sigue admitiendo cualquier
 * origen a proposito.
 */
@Configuration
public class CorsConfig implements WebMvcConfigurer {

    private final AggoraProperties props;

    public CorsConfig(AggoraProperties props) {
        this.props = props;
    }

    @Override
    public void addCorsMappings(CorsRegistry registry) {
        registry.addMapping("/api/**")
                .allowedOrigins(props.ui().allowedOrigins().toArray(String[]::new))
                .allowedMethods("GET");
    }
}
