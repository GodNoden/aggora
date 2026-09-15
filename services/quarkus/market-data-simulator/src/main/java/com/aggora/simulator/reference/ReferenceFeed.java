package com.aggora.simulator.reference;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import com.aggora.simulator.config.AggoraConfig;
import com.aggora.simulator.config.AggoraConfig.FeedProvider;
import com.aggora.simulator.simulation.TickEngine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.quarkus.runtime.StartupEvent;
import io.quarkus.scheduler.Scheduled;

/**
 * Scheduler de los datos reales: cada fuente a su ritmo, y ninguna por encima de su cuota.
 *
 * <p>Es un scheduler, no un bucle ocupado, y solo pide precios de los mercados que estan abiertos
 * en ese momento: por la noche no tiene sentido preguntar por Euronext.
 *
 * <p>Los dos proveedores tienen cuotas muy distintas y por eso van por separado: Twelve Data cada
 * 15 minutos (precio por lotes) y Alpha Vantage cada 3 horas (una peticion por simbolo, 1/segundo).
 *
 * <p><b>Diferencia de inyeccion con Spring, que se nota al escribir esto:</b> en Spring se pide
 * {@code List<ReferenceSource>} y el contenedor mete todas las implementaciones; en CDI se pide
 * {@code Instance<ReferenceSource>} y se recorren. Misma idea, otra forma.
 *
 * <p>{@code concurrentExecution = SKIP} para que una vuelta lenta no se solape con la siguiente:
 * es el equivalente al {@code fixedDelay} de Spring.
 *
 * <p>ponytail: el gasto se contabiliza por simbolos pedidos, sin mirar lo que responde el
 * servidor. Es deliberadamente conservador: mejor quedarse corto que agotar la cuota y quedarse
 * sin datos reales el resto del dia.
 */
@ApplicationScoped
public class ReferenceFeed {

    private static final Logger log = LoggerFactory.getLogger(ReferenceFeed.class);

    private final List<ReferenceSource> sources;
    private final TickEngine engine;
    private final AggoraConfig props;
    private final Map<FeedProvider, AtomicInteger> usedToday = new EnumMap<>(FeedProvider.class);
    private final Map<FeedProvider, AtomicBoolean> budgetWarned = new EnumMap<>(FeedProvider.class);
    private volatile LocalDate budgetDay = LocalDate.now(ZoneOffset.UTC);

    @Inject
    public ReferenceFeed(Instance<ReferenceSource> sources, TickEngine engine, AggoraConfig props) {
        this.sources = sources.stream().toList();
        this.engine = engine;
        this.props = props;
        for (ReferenceSource source : this.sources) {
            usedToday.put(source.provider(), new AtomicInteger());
            budgetWarned.put(source.provider(), new AtomicBoolean());
        }
    }

    void announceMode(@Observes StartupEvent evento) {
        for (ReferenceSource source : sources) {
            long instruments = countInstruments(source.provider());
            if (source.configured()) {
                log.info("[reference] {} configurado | {} instrumentos | presupuesto diario {} | {}",
                        source.provider(), instruments, source.dailyBudget(), quotaNote(source.provider()));
            } else {
                log.warn("[reference] {} sin credenciales: sus {} instrumentos van solo con precio semilla "
                                + "(los ticks son 100% sinteticos, pero el mercado y sus horarios siguen siendo reales)",
                        source.provider(), instruments);
            }
        }
    }

    @Scheduled(every = "${aggora.twelve-data.poll-interval-ms:900000}ms", delayed = "${aggora.twelve-data.initial-delay-ms:5000}ms",
            concurrentExecution = Scheduled.ConcurrentExecution.SKIP,
            skipExecutionIf = Scheduled.ApplicationNotRunning.class)
    public void pollTwelveData() {
        poll(FeedProvider.TWELVE_DATA);
    }

    @Scheduled(every = "${aggora.alpha-vantage.poll-interval-ms:10800000}ms", delayed = "${aggora.alpha-vantage.initial-delay-ms:10000}ms",
            concurrentExecution = Scheduled.ConcurrentExecution.SKIP,
            skipExecutionIf = Scheduled.ApplicationNotRunning.class)
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
                log.warn("[reference] {} con el presupuesto diario agotado ({}): hoy no se piden mas precios reales",
                        provider, source.dailyBudget());
            }
            return;
        }

        Instant now = Instant.now();
        List<AggoraConfig.Instrument> open = props.instruments().stream()
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
                ? "peticiones (1 por simbolo, 1/segundo)"
                : "creditos (1 por simbolo)";
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
