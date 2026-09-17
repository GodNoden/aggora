package com.aggora.gateway.consumer;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.aggora.avro.alerts.Alert;
import com.aggora.avro.canonical.CanonicalTick;
import com.aggora.avro.portfolio.PortfolioPosition;
import com.aggora.gateway.config.AggoraProperties;
import com.aggora.gateway.ws.LiveFeedHandler;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import tools.jackson.databind.ObjectMapper;

/**
 * Consume los tres topics de eventos y los reparte por WebSocket.
 *
 * <p>Los ticks van AGREGADOS: de cada simbolo se guarda solo el ultimo precio y el resumen sale
 * UNA vez por segundo ({@code kind=snapshot}). Un mensaje por tick eran ~84/s de normal y hasta
 * 4.000/s en el test de estres, que funde cualquier navegador; con el resumen la pagina recibe un
 * mensaje por segundo con el ultimo precio de cada simbolo. Las posiciones y las alertas son pocas
 * y se siguen mandando AL MOMENTO, que es lo que el usuario quiere ver llegar.
 *
 * <p>Todos los mensajes llevan {@code v} (version del contrato) y {@code stack} (la
 * implementacion que los manda) para que el dashboard pueda pintar las dos a la vez.
 *
 * <p>Deliberadamente **no** hay commit manual ni transacciones aqui: es un fan-out. Si un evento se
 * pierde, el siguiente trae el dato nuevo y la pantalla se corrige sola; lo que no puede pasar es
 * que un cliente lento frene a los demas, y de eso se encarga el handler.
 */
@Component
public class LiveFeedConsumer {

    private static final Logger log = LoggerFactory.getLogger(LiveFeedConsumer.class);

    private final LiveFeedHandler handler;
    private final AggoraProperties props;
    private final ObjectMapper json = new ObjectMapper();

    /** Ultimo tick de cada simbolo: la foto que se manda cada segundo. */
    private final Map<String, Map<String, Object>> ultimoPorSimbolo = new ConcurrentHashMap<>();
    private final AtomicLong ticksIn = new AtomicLong();
    private final AtomicLong snapshots = new AtomicLong();

    /**
     * ponytail: un hilo programado a mano en vez de @Scheduled, para no depender del planificador de
     * Spring ni del de Quarkus (que ademas tiene un suelo de 1 s). Es el mismo resumen por segundo
     * en las dos implementaciones.
     */
    private final ScheduledExecutorService reloj = Executors.newSingleThreadScheduledExecutor(tarea -> {
        Thread hilo = new Thread(tarea, "snapshot-ws");
        hilo.setDaemon(true);
        return hilo;
    });

    public LiveFeedConsumer(LiveFeedHandler handler, AggoraProperties props) {
        this.handler = handler;
        this.props = props;
    }

    @PostConstruct
    void arrancarReloj() {
        reloj.scheduleAtFixedRate(this::publicarSnapshot, 1, 1, TimeUnit.SECONDS);
    }

    @PreDestroy
    void pararReloj() {
        reloj.shutdownNow();
    }

    @KafkaListener(id = "live-ticks", topics = "${aggora.topics.ticks-canonical}",
            groupId = "${spring.kafka.consumer.group-id}")
    public void onTick(ConsumerRecord<String, CanonicalTick> record) {
        CanonicalTick tick = record.value();
        Map<String, Object> precio = new LinkedHashMap<>();
        precio.put("price", decimal(tick.getPrice()));
        precio.put("currency", tick.getCurrency());
        precio.put("size", tick.getSize());
        precio.put("source", tick.getSource().name());
        precio.put("at", tick.getEventTime().toString());
        ultimoPorSimbolo.put(tick.getSymbol(), precio);
        ticksIn.incrementAndGet();
    }

    @KafkaListener(id = "live-portfolio", topics = "${aggora.topics.portfolio-updates}",
            groupId = "${spring.kafka.consumer.group-id}")
    public void onPosition(ConsumerRecord<String, PortfolioPosition> record) {
        PortfolioPosition posicion = record.value();
        Map<String, Object> evento = sobre("position");
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
        Map<String, Object> evento = sobre("alert");
        evento.put("severity", alerta.getSeverity().name());
        evento.put("type", alerta.getType().name());
        evento.put("subject", alerta.getSubject());
        evento.put("detail", alerta.getDetail());
        evento.put("value", decimal(alerta.getValue()));
        evento.put("raisedAt", alerta.getRaisedAt().toString());
        repartir(evento);
    }

    /**
     * El resumen: un mensaje por segundo con el ultimo tick de cada simbolo y las tasas.
     *
     * <p>{@code ticksIn} son los ticks consumidos en el ultimo segundo y {@code ticksOut} los
     * simbolos que salen en esta foto (la lista se queda con el ultimo de cada uno, asi que la
     * resta entre los dos es lo que el resumen se ha ahorrado).
     */
    private void publicarSnapshot() {
        try {
            Map<String, Object> simbolos = new LinkedHashMap<>(ultimoPorSimbolo);
            Map<String, Object> evento = sobre("snapshot");
            evento.put("ticksIn", ticksIn.getAndSet(0));
            evento.put("ticksOut", simbolos.size());
            evento.put("symbols", simbolos);
            repartir(evento);
            long total = snapshots.incrementAndGet();
            if (total % 60 == 0) {
                log.info("[ws] {} snapshots | simbolos: {} | clientes conectados: {}",
                        total, simbolos.size(), handler.clientes());
            }
        } catch (RuntimeException fallo) {
            // Un fallo pintando la foto no puede matar el hilo programado: el siguiente segundo
            // vuelve a intentarlo y la pantalla se corrige sola.
            log.warn("[ws] no se pudo publicar el snapshot: {}", fallo.getMessage());
        }
    }

    /** El sobre comun a todos los mensajes: version, tipo y de que implementacion salen. */
    private Map<String, Object> sobre(String kind) {
        Map<String, Object> evento = new LinkedHashMap<>();
        evento.put("v", 1);
        evento.put("kind", kind);
        evento.put("stack", props.stack());
        evento.put("ts", Instant.now().toString());
        return evento;
    }

    private void repartir(Map<String, Object> evento) {
        handler.broadcast(json.writeValueAsString(evento));
    }

    private static String decimal(BigDecimal valor) {
        return valor == null ? "0" : valor.toPlainString();
    }
}
