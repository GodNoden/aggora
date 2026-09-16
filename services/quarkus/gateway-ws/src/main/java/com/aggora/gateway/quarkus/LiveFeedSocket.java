package com.aggora.gateway.quarkus;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import jakarta.inject.Singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.quarkus.websockets.next.OnClose;
import io.quarkus.websockets.next.OnOpen;
import io.quarkus.websockets.next.WebSocket;
import io.quarkus.websockets.next.WebSocketConnection;

/**
 * El endpoint WebSocket: guarda las conexiones abiertas y reparte a todas lo que le llega.
 *
 * <p>Equivale al {@code LiveFeedHandler} de la version Spring, con una diferencia de fondo: en
 * Spring el envio es sincrono ({@code session.getBasicRemote().sendText}) y una sesion lenta frena
 * al hilo que reparte, asi que ahi se usa el envio asincrono a mano; en WebSockets Next el envio
 * <b>ya es asincrono</b> (devuelve {@code Uni}), que es justo lo que se quiere en un fan-out.
 *
 * <p>Un cliente que se muere no se detecta adivinando: se detecta cuando el envio falla, y
 * entonces se quita de la lista. Mientras no falle, se le sigue enviando.
 */
@WebSocket(path = "/ws")
@Singleton
public class LiveFeedSocket {

    private static final Logger log = LoggerFactory.getLogger(LiveFeedSocket.class);

    private final Set<WebSocketConnection> conexiones = ConcurrentHashMap.newKeySet();
    private final AtomicLong repartidos = new AtomicLong();
    private final AtomicInteger descartados = new AtomicInteger();

    @OnOpen
    public void abrir(WebSocketConnection conexion) {
        conexiones.add(conexion);
        log.info("[ws] cliente conectado ({} en total)", conexiones.size());
    }

    @OnClose
    public void cerrar(WebSocketConnection conexion) {
        conexiones.remove(conexion);
        log.info("[ws] cliente desconectado ({} en total)", conexiones.size());
    }

    /** Manda el mismo texto a todos los navegadores conectados. */
    public void repartir(String json) {
        for (WebSocketConnection conexion : conexiones) {
            conexion.sendText(json).subscribe().with(
                    ignorado -> {
                    },
                    fallo -> {
                        // El cliente se fue a mitad de envio: se descarta y se sigue con los demas.
                        conexiones.remove(conexion);
                        descartados.incrementAndGet();
                    });
        }
        long total = repartidos.incrementAndGet();
        if (total % 500 == 0) {
            log.info("[ws] {} eventos repartidos | clientes conectados: {} | descartados: {}",
                    total, conexiones.size(), descartados.get());
        }
    }

    public int clientes() {
        return conexiones.size();
    }

    public long repartidos() {
        return repartidos.get();
    }
}
