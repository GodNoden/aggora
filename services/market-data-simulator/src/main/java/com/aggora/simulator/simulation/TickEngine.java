package com.aggora.simulator.simulation;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.aggora.simulator.config.AggoraProperties;
import com.aggora.simulator.domain.TickEvent;
import com.aggora.simulator.pricing.PriceWalk;
import com.aggora.simulator.producer.TickProducer;

import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Motor de ticks: mantiene un PriceWalk por instrumento y emite ticks sintéticos
 * a ritmo fijo, solo para los mercados que están abiertos en ese instante.
 *
 * ponytail: un único hilo de scheduler. A ~10 instrumentos x 5 ticks/s son ~50
 * msg/s, de sobra para dev. Si algún día se necesita más volumen, el cambio es
 * particionar el trabajo (un scheduler por partición de símbolos) y no tocar
 * el resto.
 */
@Component
public class TickEngine {

    private static final Logger log = LoggerFactory.getLogger(TickEngine.class);

    private final AggoraProperties props;
    private final TickProducer producer;
    private final Random random = new Random();
    private final Map<String, PriceWalk> walks = new ConcurrentHashMap<>();
    private final Map<String, BigDecimal> references = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> sequences = new ConcurrentHashMap<>();

    public TickEngine(AggoraProperties props, TickProducer producer) {
        this.props = props;
        this.producer = producer;
        props.instruments().forEach(instrument ->
                references.put(instrument.symbol(), BigDecimal.valueOf(instrument.seedPrice())));
    }

    @PostConstruct
    void logUniverse() {
        Instant now = Instant.now();
        log.info("[universo] {} instrumentos configurados", props.instruments().size());
        props.instruments().forEach(instrument -> log.info("[universo] {} ({}) {} {} | {} | abierto ahora: {} | dato real: {}",
                instrument.symbol(), instrument.assetClass(), instrument.exchange(),
                instrument.currency(), instrument.exchange().hours(), instrument.exchange().isOpen(now),
                instrument.hasRealFeed() ? instrument.feed() : "no (solo semilla)"));
    }

    /** Tick sintético para cada instrumento cuyo mercado esté abierto. */
    @Scheduled(fixedRateString = "${aggora.tick-interval-ms}")
    public void emitSyntheticTicks() {
        Instant now = Instant.now();
        for (AggoraProperties.Instrument instrument : props.instruments()) {
            if (!instrument.exchange().isOpen(now)) {
                continue;
            }
            double reference = references.get(instrument.symbol()).doubleValue();
            PriceWalk walk = walkOf(instrument.symbol(), reference);
            double price = walk.next(reference);
            producer.send(toEvent(instrument, price, TickEvent.TickSource.SYNTHETIC, now));
        }
    }

    /**
     * Nueva referencia real: el paseo salta al precio verdadero y se emite UN tick
     * marcado como REFERENCE (size 0: es una cotización, no una operación).
     */
    public void applyReference(AggoraProperties.Instrument instrument, double price) {
        walkOf(instrument.symbol(), price).reset(price);
        references.put(instrument.symbol(), BigDecimal.valueOf(price));
        producer.send(toEvent(instrument, price, TickEvent.TickSource.REFERENCE, Instant.now()));
    }

    public BigDecimal referenceOf(String symbol) {
        return references.get(symbol);
    }

    private PriceWalk walkOf(String symbol, double price) {
        return walks.computeIfAbsent(symbol, s -> {
            AggoraProperties.Simulation sim = props.simulation();
            return new PriceWalk(price, sim.volatilityPerTick(), sim.meanReversion(), sim.maxDeviation(), random);
        });
    }

    private TickEvent toEvent(AggoraProperties.Instrument instrument, double price,
                              TickEvent.TickSource source, Instant eventTime) {
        AggoraProperties.Simulation sim = props.simulation();
        int size = source == TickEvent.TickSource.REFERENCE
                ? 0
                : sim.minSize() + random.nextInt(Math.max(1, sim.maxSize() - sim.minSize() + 1));
        long sequence = sequences.computeIfAbsent(instrument.symbol(), s -> new AtomicLong()).incrementAndGet();
        return new TickEvent(
                UUID.randomUUID().toString(),
                instrument.symbol(),
                instrument.assetClass(),
                instrument.exchange(),
                instrument.currency(),
                BigDecimal.valueOf(price).setScale(4, RoundingMode.HALF_UP),
                size,
                eventTime,
                source,
                sequence);
    }
}
