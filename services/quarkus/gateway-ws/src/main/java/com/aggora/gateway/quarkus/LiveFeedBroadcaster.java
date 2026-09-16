package com.aggora.gateway.quarkus;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.Map;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import com.aggora.avro.alerts.Alert;
import com.aggora.avro.canonical.CanonicalTick;
import com.aggora.avro.portfolio.PortfolioPosition;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.smallrye.common.annotation.Blocking;

/**
 * El fan-out: consume los tres topics de eventos y los manda al WebSocket como JSON pequeno,
 * pensado para la pantalla.
 *
 * <p><b>El mapeo con la version Spring:</b>
 *
 * <table>
 *   <tr><td>{@code @KafkaListener}</td><td>{@code @Incoming}</td></tr>
 *   <tr><td>un {@code @KafkaListener} por topic</td><td>un canal por topic, declarado en
 *       {@code mp.messaging.incoming.<canal>.*}</td></tr>
 *   <tr><td>{@code ConsumerRecord} con cabecera y clave</td><td>el payload directo, si no se
 *       necesitan los metadatos</td></tr>
 * </table>
 *
 * <p>Aqui no se toca el offset a mano. En un fan-out el commit automatico es lo correcto: si un
 * evento se pierde, el siguiente trae el dato nuevo y la pantalla se corrige sola. Confirmar cada
 * mensaje uno a uno solo serviria para ir mas lento. Con {@code failure-strategy=ignore} (en
 * {@code application.properties}) un mensaje que no se pueda leer se registra y se sigue, en vez
 * de parar el canal entero y dejar la pantalla congelada.
 */
@ApplicationScoped
public class LiveFeedBroadcaster {

    private static final Logger log = LoggerFactory.getLogger(LiveFeedBroadcaster.class);

    private final LiveFeedSocket socket;
    private final ObjectMapper json;

    @Inject
    public LiveFeedBroadcaster(LiveFeedSocket socket, ObjectMapper json) {
        this.socket = socket;
        this.json = json;
    }

    @Incoming("ticks-canonical")
    @Blocking
    public void onTick(CanonicalTick tick) {
        Map<String, Object> evento = new LinkedHashMap<>();
        evento.put("kind", "tick");
        evento.put("symbol", tick.getSymbol());
        evento.put("price", decimal(tick.getPrice()));
        evento.put("currency", tick.getCurrency());
        evento.put("size", tick.getSize());
        evento.put("source", tick.getSource().name());
        evento.put("at", tick.getEventTime().toString());
        repartir(evento);
    }

    @Incoming("portfolio-updates")
    @Blocking
    public void onPosition(PortfolioPosition posicion) {
        Map<String, Object> evento = new LinkedHashMap<>();
        evento.put("kind", "position");
        evento.put("account", posicion.getAccountId());
        evento.put("symbol", posicion.getSymbol());
        evento.put("quantity", posicion.getQuantity());
        evento.put("averageCost", decimal(posicion.getAverageCost()));
        evento.put("realizedPnl", decimal(posicion.getRealizedPnl()));
        evento.put("exposure", decimal(posicion.getExposure()));
        evento.put("currency", posicion.getCurrency());
        evento.put("marginBreach", posicion.getMarginBreach());
        repartir(evento);
    }

    @Incoming("alerts")
    @Blocking
    public void onAlert(Alert alerta) {
        Map<String, Object> evento = new LinkedHashMap<>();
        evento.put("kind", "alert");
        evento.put("severity", alerta.getSeverity().name());
        evento.put("type", alerta.getType().name());
        evento.put("subject", alerta.getSubject());
        evento.put("detail", alerta.getDetail());
        evento.put("value", decimal(alerta.getValue()));
        evento.put("raisedAt", alerta.getRaisedAt().toString());
        repartir(evento);
    }

    private void repartir(Map<String, Object> evento) {
        try {
            socket.repartir(json.writeValueAsString(evento));
        } catch (JsonProcessingException fallo) {
            // Aqui si hay una diferencia real entre las dos implementaciones: Spring usa Jackson 3,
            // donde la excepcion es unchecked, y Quarkus el Jackson 2 del BOM, donde es checked.
            // Con un Map de cadenas y numeros esto no puede pasar; si pasara, se pierde un evento
            // de pantalla y el siguiente trae el dato nuevo.
            log.warn("[ws] no se pudo serializar el evento {}: {}", evento.get("kind"), fallo.getMessage());
        }
    }

    private static String decimal(BigDecimal valor) {
        return valor == null ? "0" : valor.toPlainString();
    }
}
