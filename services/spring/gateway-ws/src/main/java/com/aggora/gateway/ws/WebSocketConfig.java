package com.aggora.gateway.ws;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;

/** El endpoint: {@code ws://localhost:8089/ws}. Sin STOMP ni nada encima: WebSocket a pelo. */
@Configuration
@EnableWebSocket
public class WebSocketConfig implements WebSocketConfigurer {

    private final LiveFeedHandler handler;

    public WebSocketConfig(LiveFeedHandler handler) {
        this.handler = handler;
    }

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        // El patron de origen abierto es de desarrollo: en produccion se limita a los dominios que
        // puedan abrir el WebSocket.
        registry.addHandler(handler, "/ws").setAllowedOriginPatterns("*");
    }
}
