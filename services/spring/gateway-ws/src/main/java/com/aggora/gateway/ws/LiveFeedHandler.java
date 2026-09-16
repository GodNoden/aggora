package com.aggora.gateway.ws;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

/**
 * El reparto (fan-out): todas las sesiones de WebSocket conectadas, y un metodo para mandarles un
 * evento a todas a la vez.
 *
 * <p>Este es el concepto que el spec llama "Consumer (fan-out)" y aqui se ve por que es un tipo de
 * consumidor distinto a los demas: los otros servicios escriben en otro topic (uno a uno), y este
 * **reparte lo mismo a N clientes** sin guardar nada. No necesita offsets complicados ni estado:
 * si un navegador se pierde un mensaje, el siguiente ya trae el precio nuevo.
 *
 * <p>ponytail: las sesiones viven en memoria, asi que con dos instancias cada una reparte solo a
 * SUS clientes. Para repartir a todos habria que meter un topic por instancia o un bus. Con una
 * instancia (la demo cabe de sobra) esto es lo correcto y no lo que hay que complicar.
 */
@Component
public class LiveFeedHandler extends TextWebSocketHandler {

    private static final Logger log = LoggerFactory.getLogger(LiveFeedHandler.class);

    private final Set<WebSocketSession> sesiones = ConcurrentHashMap.newKeySet();
    private final AtomicLong enviados = new AtomicLong();

    @Override
    public void afterConnectionEstablished(WebSocketSession sesion) {
        sesiones.add(sesion);
        log.info("[ws] cliente conectado ({} en total)", sesiones.size());
    }

    @Override
    public void afterConnectionClosed(WebSocketSession sesion, CloseStatus status) {
        sesiones.remove(sesion);
        log.info("[ws] cliente desconectado ({} en total)", sesiones.size());
    }

    /** Manda el JSON a todos los clientes. El que falle se cae de la lista y no arrastra al resto. */
    public void broadcast(String json) {
        if (sesiones.isEmpty()) {
            return;
        }
        TextMessage mensaje = new TextMessage(json);
        for (WebSocketSession sesion : sesiones) {
            try {
                if (sesion.isOpen()) {
                    sesion.sendMessage(mensaje);
                    enviados.incrementAndGet();
                }
            } catch (IOException ex) {
                sesiones.remove(sesion);
                log.debug("[ws] cliente caido, fuera de la lista: {}", ex.getMessage());
            }
        }
    }

    public int clientes() {
        return sesiones.size();
    }

    public long enviadosCount() {
        return enviados.get();
    }
}
