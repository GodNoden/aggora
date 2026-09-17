package com.aggora.gateway.quarkus;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import com.aggora.avro.alerts.Alert;
import com.aggora.avro.canonical.CanonicalTick;
import com.aggora.avro.portfolio.PortfolioPosition;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.smallrye.common.annotation.Blocking;

/**
 * El fan-out: consume los tres topics de eventos y los manda al WebSocket como JSON pequeno,
 * pensado para la pantalla.
 *
 * <p>Los ticks van AGREGADOS: de cada simbolo se guarda solo el ultimo precio y el resumen sale
 * UNA vez por segundo ({@code kind=snapshot}). Un mensaje por tick eran ~84/s de normal y hasta
 * 4.000/s en el test de estres, que funde cualquier navegador. Las posiciones y las alertas se
 * siguen mandando AL MOMENTO.
 *
 * <p>Todos los mensajes llevan {@code v} (version del contrato) y {@code stack} ("quarkus") para
 * que el dashboard pueda pintar las dos implementaciones a la vez.
 *
 * <p><b>El mapeo con la version Spring:</b>
 *
 * <table>
 *   <tr><td>{@code @KafkaListener}</td><td>{@code @Incoming}</td></tr>
 *   <tr><td>un {@code @KafkaListener} por topic</td><td>un canal por topic, declarado en
 *       {@code mp.messaging.incoming.<canal>.*}</td></tr>
 *   <tr><td>{@code ConsumerRecord} con cabecera y clave</td><td>el payload directo, si no se
 *       necesitan los metadatos</td></tr>
 *   <tr><td>{@code @Scheduled} / ScheduledExecutorService</td><td>aquí también un
 *       ScheduledExecutorService del JDK: ni dependencia nueva ni el suelo de 1 s del
 *       planificador de Quarkus (que aquí daria igual, pero una sola forma para los dos)</td></tr>
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
    @ConfigProperty(name = "aggora.stack", defaultValue = "quarkus")
    String stack;

    /** Ultimo tick de cada simbolo: la foto que se manda cada segundo. */
    private final Map<String, Map<String, Object>> ultimoPorSimbolo = new ConcurrentHashMap<>();
    private final AtomicLong ticksIn = new AtomicLong();
    private final AtomicLong snapshots = new AtomicLong();
    private final ScheduledExecutorService reloj = Executors.newSingleThreadScheduledExecutor(tarea -> {
        Thread hilo = new Thread(tarea, "snapshot-ws");
        hilo.setDaemon(true);
        return hilo;
    });

    @Inject
    public LiveFeedBroadcaster(LiveFeedSocket socket, ObjectMapper json) {
        this.socket = socket;
        this.json = json;
    }

    @PostConstruct
    void arrancarReloj() {
        reloj.scheduleAtFixedRate(this::publicarSnapshot, 1, 1, TimeUnit.SECONDS);
    }

    @PreDestroy
    void pararReloj() {
        reloj.shutdownNow();
    }

    @Incoming("ticks-canonical")
    @Blocking
    public void onTick(CanonicalTick tick) {
        Map<String, Object> precio = new LinkedHashMap<>();
        precio.put("price", decimal(tick.getPrice()));
        precio.put("currency", tick.getCurrency());
        precio.put("size", tick.getSize());
        precio.put("source", tick.getSource().name());
        precio.put("at", tick.getEventTime().toString());
        ultimoPorSimbolo.put(tick.getSymbol(), precio);
        ticksIn.incrementAndGet();
    }

    @Incoming("portfolio-updates")
    @Blocking
    public void onPosition(PortfolioPosition posicion) {
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

    @Incoming("alerts")
    @Blocking
    public void onAlert(Alert alerta) {
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
     * simbolos que salen en esta foto.
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
                        total, simbolos.size(), socket.clientes());
            }
        } catch (RuntimeException fallo) {
            // Un fallo pintando la foto no puede matar el hilo programado: el siguiente segundo
            // vuelve a intentarlo.
            log.warn("[ws] no se pudo publicar el snapshot: {}", fallo.getMessage());
        }
    }

    /** El sobre comun a todos los mensajes: version, tipo y de que implementacion salen. */
    private Map<String, Object> sobre(String kind) {
        Map<String, Object> evento = new LinkedHashMap<>();
        evento.put("v", 1);
        evento.put("kind", kind);
        evento.put("stack", stack);
        evento.put("ts", Instant.now().toString());
        return evento;
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
