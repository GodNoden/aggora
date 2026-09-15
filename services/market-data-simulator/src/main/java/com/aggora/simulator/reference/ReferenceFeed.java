package com.aggora.simulator.reference;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.aggora.simulator.config.AggoraProperties;
import com.aggora.simulator.config.AggoraProperties.FeedProvider;
import com.aggora.simulator.simulation.TickEngine;

import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Scheduler de los datos reales: cada fuente a su ritmo, y ninguna por encima de su
 * cuota.
 *
 * Es un scheduler, no un bucle ocupado (spec 3.3.1), y solo pide precios de los
 * mercados que están abiertos en ese momento: por la noche no tiene sentido
 * preguntar por Euronext.
 *
 * Los dos proveedores tienen cuotas muy distintas y por eso van por separado:
 *   - Twelve Data: 8 créditos/minuto y ~800/día, con precio por lotes. Cada 15 min.
 *   - Alpha Vantage: 1 petición/segundo y 25/día, una petición por símbolo. Cada 3 h.
 *
 * ponytail: el gasto se contabiliza por símbolos pedidos, sin mirar lo que responde
 * el servidor. Es deliberadamente conservador: mejor quedarse corto que agotar la
 * cuota y quedarse sin datos reales el resto del día.
 */
@Component
public class ReferenceFeed {

    private static final Logger log = LoggerFactory.getLogger(ReferenceFeed.class);

    private final List<ReferenceSource> sources;
    private final TickEngine engine;
    private final AggoraProperties props;
    private final Map<FeedProvider, AtomicInteger> usedToday = new EnumMap<>(FeedProvider.class);
    private final Map<FeedProvider, AtomicBoolean> budgetWarned = new EnumMap<>(FeedProvider.class);
    private volatile LocalDate budgetDay = LocalDate.now(ZoneOffset.UTC);

    public ReferenceFeed(List<ReferenceSource> sources, TickEngine engine, AggoraProperties props) {
        this.sources = sources;
        this.engine = engine;
        this.props = props;
        for (ReferenceSource source : sources) {
            usedToday.put(source.provider(), new AtomicInteger());
            budgetWarned.put(source.provider(), new AtomicBoolean());
        }
    }

    @PostConstruct
    void announceMode() {
        for (ReferenceSource source : sources) {
            long instruments = countInstruments(source.provider());
            if (source.configured()) {
                log.info("[reference] {} configurado | {} instrumentos | presupuesto diario {} | {}",
                        source.provider(), instruments, source.dailyBudget(), quotaNote(source.provider()));
            } else {
                log.warn("[reference] {} sin credenciales: sus {} instrumentos van solo con precio semilla "
                                + "(los ticks son 100% sintéticos, pero el mercado y sus horarios siguen siendo reales)",
                        source.provider(), instruments);
            }
        }
    }

    @Scheduled(initialDelayString = "${aggora.twelve-data.initial-delay-ms}",
            fixedDelayString = "${aggora.twelve-data.poll-interval-ms}")
    public void pollTwelveData() {
        poll(FeedProvider.TWELVE_DATA);
    }

    @Scheduled(initialDelayString = "${aggora.alpha-vantage.initial-delay-ms}",
            fixedDelayString = "${aggora.alpha-vantage.poll-interval-ms}")
    public void pollAlphaVantage() {
        poll(FeedProvider.ALPHA_VANTAGE);
    }

    void poll(FeedProvider provider) {
        ReferenceSource source = sources.stream()
                .filter(candidate -> candidate.provider() == provider)
                .findFirst()
                .orElse(null);
        if (source == null || !source.configured()) {
            return;
        }
        resetBudgetIfNewDay();

        AtomicInteger used = usedToday.get(provider);
        if (used.get() >= source.dailyBudget()) {
            if (budgetWarned.get(provider).compareAndSet(false, true)) {
                log.warn("[reference] {} con el presupuesto diario agotado ({}): hoy no se piden más precios reales",
                        provider, source.dailyBudget());
            }
            return;
        }

        Instant now = Instant.now();
        List<AggoraProperties.Instrument> open = props.instruments().stream()
                .filter(instrument -> instrument.feed() == provider)
                .filter(instrument -> instrument.exchange().isOpen(now))
                .toList();
        if (open.isEmpty()) {
            log.debug("[reference] {} no tiene mercados abiertos ahora mismo: no se pide nada", provider);
            return;
        }

        Map<String, Double> prices = source.fetchPrices(open);
        used.addAndGet(open.size());
        prices.forEach((symbol, price) -> props.instruments().stream()
                .filter(instrument -> instrument.symbol().equals(symbol))
                .findFirst()
                .ifPresent(instrument -> engine.applyReference(instrument, price)));

        log.info("[reference] {} | {}/{} precios actualizados | usado hoy {}/{}",
                provider, prices.size(), open.size(), used.get(), source.dailyBudget());
    }

    private long countInstruments(FeedProvider provider) {
        return props.instruments().stream().filter(instrument -> instrument.feed() == provider).count();
    }

    private String quotaNote(FeedProvider provider) {
        return provider == FeedProvider.ALPHA_VANTAGE
                ? "peticiones (1 por símbolo, 1/segundo)"
                : "créditos (1 por símbolo)";
    }

    private void resetBudgetIfNewDay() {
        LocalDate today = LocalDate.now(ZoneOffset.UTC);
        if (!today.equals(budgetDay)) {
            budgetDay = today;
            usedToday.values().forEach(counter -> counter.set(0));
            budgetWarned.values().forEach(flag -> flag.set(false));
        }
    }
}
