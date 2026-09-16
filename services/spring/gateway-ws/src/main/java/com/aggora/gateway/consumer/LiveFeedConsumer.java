package com.aggora.gateway.consumer;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.aggora.avro.alerts.Alert;
import com.aggora.avro.canonical.CanonicalTick;
import com.aggora.avro.portfolio.PortfolioPosition;
import com.aggora.gateway.ws.LiveFeedHandler;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import tools.jackson.databind.ObjectMapper;

/**
 * Consume los tres topics de eventos y los reparte por WebSocket.
 *
 * <p>Traduce cada evento Avro a un JSON pequeno y pensado para la pantalla (no se manda el registro
 * entero): al navegador le interesan el simbolo, el precio y el saldo, no los offsets de origen.
 *
 * <p>Deliberadamente **no** hay commit manual ni transacciones aqui: es un fan-out. Si un evento se
 * pierde, el siguiente trae el dato nuevo y la pantalla se corrige sola; lo que no puede pasar es
 * que un cliente lento frene a los demas, y de eso se encarga el handler.
 */
@Component
public class LiveFeedConsumer {

    private static final Logger log = LoggerFactory.getLogger(LiveFeedConsumer.class);

    private final LiveFeedHandler handler;
    private final ObjectMapper json = new ObjectMapper();
    private final AtomicLong repartidos = new AtomicLong();

    public LiveFeedConsumer(LiveFeedHandler handler) {
        this.handler = handler;
    }

    @KafkaListener(id = "live-ticks", topics = "${aggora.topics.ticks-canonical}",
            groupId = "${spring.kafka.consumer.group-id}")
    public void onTick(ConsumerRecord<String, CanonicalTick> record) {
        CanonicalTick tick = record.value();
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

    @KafkaListener(id = "live-portfolio", topics = "${aggora.topics.portfolio-updates}",
            groupId = "${spring.kafka.consumer.group-id}")
    public void onPosition(ConsumerRecord<String, PortfolioPosition> record) {
        PortfolioPosition posicion = record.value();
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

    @KafkaListener(id = "live-alerts", topics = "${aggora.topics.alerts}",
            groupId = "${spring.kafka.consumer.group-id}")
    public void onAlert(ConsumerRecord<String, Alert> record) {
        Alert alerta = record.value();
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
        handler.broadcast(json.writeValueAsString(evento));
        long total = repartidos.incrementAndGet();
        if (total % 500 == 0) {
            log.info("[ws] {} eventos repartidos | clientes conectados: {}", total, handler.clientes());
        }
    }

    private static String decimal(BigDecimal valor) {
        return valor == null ? "0" : valor.toPlainString();
    }

    public long repartidosCount() {
        return repartidos.get();
    }
}
