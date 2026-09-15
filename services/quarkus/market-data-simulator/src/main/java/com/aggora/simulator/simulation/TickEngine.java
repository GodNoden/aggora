package com.aggora.simulator.simulation;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

import com.aggora.avro.Tick;
import com.aggora.avro.TickSource;
import com.aggora.simulator.config.AggoraConfig;
import com.aggora.simulator.pricing.PriceWalk;
import com.aggora.simulator.producer.TickEmitter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.quarkus.runtime.StartupEvent;
import io.quarkus.scheduler.Scheduled;

/**
 * Motor de ticks: mantiene un {@link PriceWalk} por instrumento y emite ticks sinteticos a
 * ritmo fijo, solo para los mercados que estan abiertos en ese instante.
 *
 * <p>Es el mismo motor que el de Spring, con el scheduler de Quarkus:
 *
 * <table>
 *   <tr><td>{@code @Scheduled(fixedRateString = "${...}")}</td><td>{@code @Scheduled(every = "${...}")}</td></tr>
 *   <tr><td>{@code spring.task.scheduling.pool.size}</td><td>los metodos programados no se solapan si lo pides ({@code SKIP})</td></tr>
 *   <tr><td>{@code @PostConstruct}</td><td>{@code @Observes StartupEvent}</td></tr>
 * </table>
 *
 * <p>ponytail: el ritmo lo marca un unico job. A ~14 instrumentos x 5 ticks/s son ~70 msg/s, de
 * sobra para dev. Si algun dia hace falta mas volumen, el cambio es repartir el trabajo en
 * varios jobs por particion de simbolos, sin tocar el resto.
 */
@ApplicationScoped
public class TickEngine {

    private static final Logger log = LoggerFactory.getLogger(TickEngine.class);

    private final AggoraConfig props;
    private final TickEmitter emitter;
    private final Random random = new Random();
    private final Map<String, PriceWalk> walks = new ConcurrentHashMap<>();
    private final Map<String, BigDecimal> references = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> sequences = new ConcurrentHashMap<>();
    private final AtomicLong ticksSent = new AtomicLong();

    @Inject
    public TickEngine(AggoraConfig props, TickEmitter emitter) {
        this.props = props;
        this.emitter = emitter;
        props.instruments().forEach(instrument ->
                references.put(instrument.symbol(), BigDecimal.valueOf(instrument.seedPrice())));
    }

    void logUniverse(@Observes StartupEvent evento) {
        Instant now = Instant.now();
        log.info("[universo] {} instrumentos configurados", props.instruments().size());
        props.instruments().forEach(instrument -> log.info("[universo] {} ({}) {} {} | {} | abierto ahora: {} | dato real: {}",
                instrument.symbol(), instrument.assetClass(), instrument.exchange(),
                instrument.currency(), instrument.exchange().hours(), instrument.exchange().isOpen(now),
                AggoraConfig.hasRealFeed(instrument) ? instrument.feed() : "no (solo semilla)"));
    }

    /**
     * Tick sintetico para cada instrumento cuyo mercado este abierto.
     *
     * <p><b>Aqui hay una diferencia real entre los dos frameworks, y se descubrio midiendo:</b> el
     * scheduler simple de Quarkus <b>no baja de un segundo</b> ("An every() value less than 1000 ms
     * is not supported") y avisa por el log. Spring si admite {@code fixedRate = 200ms}.
     *
     * <p>Para no falsear la comparacion, el port mantiene el MISMO ritmo por otra via: se programa
     * una vuelta por segundo y en cada vuelta se emiten los ticks que toquen
     * ({@code 1000 / tick-interval-ms}, o sea 5 con el valor de este proyecto). El ritmo es el
     * mismo (5 ticks/s por instrumento), pero repartido en rafagas de 5 en vez de uniforme: eso
     * cambia un poco lo que ven las ventanas de la analitica y hay que decirlo.
     *
     * <p>{@code concurrentExecution = SKIP} para que una vuelta lenta no se solape con la
     * siguiente (el comportamiento del {@code fixedRate} de Spring con un solo hilo), y
     * {@code skipExecutionIf = ApplicationNotRunning} para que no dispare antes de que la
     * aplicacion este lista: si no, la primera ejecucion se encuentra los canales de Kafka sin
     * conectar y falla con {@code SRMSG00019: Unable to connect an emitter with the channel}.
     */
    @Scheduled(every = "1s",
            concurrentExecution = Scheduled.ConcurrentExecution.SKIP,
            skipExecutionIf = Scheduled.ApplicationNotRunning.class)
    public void emitSyntheticTicks() {
        int ticksPorVuelta = Math.max(1, 1000 / props.tickIntervalMs());
        for (int vuelta = 0; vuelta < ticksPorVuelta; vuelta++) {
            emitirUnaRonda();
        }
    }

    private void emitirUnaRonda() {
        Instant now = Instant.now();
        for (AggoraConfig.Instrument instrument : props.instruments()) {
            if (!instrument.exchange().isOpen(now)) {
                continue;
            }
            double reference = references.get(instrument.symbol()).doubleValue();
            PriceWalk walk = walkOf(instrument.symbol(), reference);
            double price = walk.next(reference);
            // Gancho SOLO para el ejercicio de dead-letter topics: cada N ticks se emite uno con
            // el precio en negativo, que el normalizer rechazara por invalido.
            if (props.invalidTickEveryN() > 0 && ticksSent.incrementAndGet() % props.invalidTickEveryN() == 0) {
                log.warn("[invalido] emitido a proposito un tick con precio negativo ({})", instrument.symbol());
                price = -Math.abs(price);
            }
            emitter.send(toTick(instrument, price, TickSource.SYNTHETIC, now));
        }
    }

    /**
     * Nueva referencia real: el paseo salta al precio verdadero y se emite UN tick marcado como
     * REFERENCE (size 0: es una cotizacion, no una operacion).
     */
    public void applyReference(AggoraConfig.Instrument instrument, double price) {
        walkOf(instrument.symbol(), price).reset(price);
        references.put(instrument.symbol(), BigDecimal.valueOf(price));
        emitter.send(toTick(instrument, price, TickSource.REFERENCE, Instant.now()));
    }

    public BigDecimal referenceOf(String symbol) {
        return references.get(symbol);
    }

    private PriceWalk walkOf(String symbol, double price) {
        return walks.computeIfAbsent(symbol, s -> {
            AggoraConfig.Simulation sim = props.simulation();
            return new PriceWalk(price, sim.volatilityPerTick(), sim.meanReversion(), sim.maxDeviation(), random);
        });
    }

    /**
     * Construye el evento Avro. Ojo con los dos enums que se llaman igual: {@code
     * com.aggora.avro.Exchange} es el del CONTRATO (lo que se registra en el Schema Registry) y
     * {@code com.aggora.simulator.domain.Exchange} es el del DOMINIO (el que sabe los horarios de
     * cada mercado). Se convierten por nombre.
     */
    private Tick toTick(AggoraConfig.Instrument instrument, double price,
                        TickSource source, Instant eventTime) {
        AggoraConfig.Simulation sim = props.simulation();
        int size = source == TickSource.REFERENCE
                ? 0
                : sim.minSize() + random.nextInt(Math.max(1, sim.maxSize() - sim.minSize() + 1));
        long sequence = sequences.computeIfAbsent(instrument.symbol(), s -> new AtomicLong()).incrementAndGet();
        return Tick.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setSymbol(instrument.symbol())
                .setAssetClass(instrument.assetClass())
                .setExchange(com.aggora.avro.Exchange.valueOf(instrument.exchange().name()))
                .setCurrency(instrument.currency())
                .setPrice(BigDecimal.valueOf(price).setScale(4, RoundingMode.HALF_UP))
                .setSize(size)
                .setEventTime(eventTime)
                .setSource(source)
                .setSequence(sequence)
                .build();
    }
}
