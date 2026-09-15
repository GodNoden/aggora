package com.aggora.analytics.topology;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

import com.aggora.analytics.config.AggoraConfig;
import com.aggora.analytics.config.AvroSerdes;
import com.aggora.avro.analytics.MetricsAccumulator;
import com.aggora.avro.analytics.SymbolMetrics;
import com.aggora.avro.analytics.WindowKind;
import com.aggora.avro.canonical.CanonicalTick;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.WindowStore;

/**
 * La topologia: de ticks canonicos a metricas por ventana.
 *
 *   market.ticks.canonical --> groupByKey(simbolo) --+--> ventana FIJA (tumbling)  --+
 *                                                   |                               |--> market.analytics
 *                                                   +--> ventana MOVIL (hopping) ---+
 *
 * La misma metrica se calcula con los dos tipos de ventana a proposito: asi se ve la
 * diferencia en el propio topic. La fija (30 s, sin solape) da una foto por bloque; la
 * movil (60 s recalculada cada 15 s) da una media que se va refrescando.
 *
 * Cada agregacion se materializa en un STATE STORE: Kafka Streams guarda ahi lo que
 * necesita recordar (sumas, volumen, ultimo precio), en disco (RocksDB) y con una copia
 * en un topic de changelog, asi que el estado sobrevive a un reinicio.
 *
 * ponytail: emitimos una metrica por cada tick que entra (el record cache va
 * desactivado). Es lo mas simple y ademas didactico, porque se ve la ventana llenarse.
 * El patron "profesional" seria encadenar .suppress(untilWindowCloses(...)) para emitir
 * UNA sola vez, cuando la ventana se cierra; se anade cuando haga falta reducir trafico.
 */
public final class MetricsTopology {

    private static final Logger log = LoggerFactory.getLogger(MetricsTopology.class);
    private static final AtomicLong SAMPLED = new AtomicLong();

    /** Nombre del state store de la ventana fija: lo usa la consulta interactiva. */
    public static final String TUMBLING_STORE = "metrics-tumbling-store";

    private MetricsTopology() {
    }

    /**
     * Construye la topologia sobre el builder y devuelve el stream de metricas.
     *
     * Es estatico y sin Spring a proposito: el test lo llama directamente con una URL
     * "mock://" y comprueba la aritmetica sin broker ni registry.
     */
    public static KStream<String, SymbolMetrics> apply(StreamsBuilder builder,
                                                       AggoraConfig props,
                                                       AvroSerdes serdes) {
        KStream<String, CanonicalTick> canonical = builder.stream(
                props.topics().ticksCanonical(),
                Consumed.with(Serdes.String(), serdes.canonicalTicks()));

        TimeWindows tumbling = TimeWindows.ofSizeAndGrace(
                props.windows().tumblingSize(), props.windows().grace());

        TimeWindows hopping = TimeWindows.ofSizeAndGrace(
                        props.windows().hoppingSize(), props.windows().grace())
                .advanceBy(props.windows().hoppingAdvance());

        KStream<String, SymbolMetrics> metrics =
                windowedAverage(canonical, serdes, WindowKind.TUMBLING, tumbling)
                        .merge(windowedAverage(canonical, serdes, WindowKind.HOPPING, hopping));

        metrics
                // Log muestreado: como el topic va en Avro binario, esto es lo que
                // permite ver las metricas por la consola sin descodificar nada.
                .peek((key, value) -> {
                    if (SAMPLED.incrementAndGet() % 200 == 0) {
                        log.info("[metricas] {} {} {}->{} | ticks={} volumen={} vwap={} media={} volatilidad={}",
                                value.getSymbol(), value.getWindowKind(),
                                value.getWindowStart(), value.getWindowEnd(),
                                value.getTicks(), value.getVolume(),
                                value.getVwap(), value.getMovingAverage(), value.getVolatility());
                    }
                })
                .to(props.topics().analytics(), Produced.with(Serdes.String(), serdes.metrics()));
        return metrics;
    }

    private static KStream<String, SymbolMetrics> windowedAverage(KStream<String, CanonicalTick> canonical,
                                                                  AvroSerdes serdes,
                                                                  WindowKind kind,
                                                                  TimeWindows windows) {
        return canonical
                .groupByKey(Grouped.with(Serdes.String(), serdes.canonicalTicks()))
                .windowedBy(windows)
                .aggregate(
                        MetricsTopology::emptyAccumulator,
                        (key, tick, accumulator) -> accumulate(accumulator, tick),
                        Materialized
                                .<String, MetricsAccumulator, WindowStore<Bytes, byte[]>>as(storeName(kind))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(serdes.accumulator()))
                .toStream()
                // La clave de una ventana es Windowed<String> (simbolo + rango). En el
                // topic queremos la clave limpia: el simbolo. El rango va en el valor.
                .map((windowedKey, accumulator) -> KeyValue.pair(windowedKey.key(), toMetrics(windowedKey, accumulator, kind)));
    }

    private static MetricsAccumulator emptyAccumulator() {
        return MetricsAccumulator.newBuilder()
                .setTicks(0L)
                .setVolume(0L)
                .setSumPrice(0.0)
                .setSumPriceSize(0.0)
                .setSumSqPrice(0.0)
                .setLastPrice(0.0)
                .setCurrency("")
                .build();
    }

    private static MetricsAccumulator accumulate(MetricsAccumulator accumulator, CanonicalTick tick) {
        double price = tick.getPrice().doubleValue();
        long size = tick.getSize();
        return MetricsAccumulator.newBuilder(accumulator)
                .setTicks(accumulator.getTicks() + 1)
                .setVolume(accumulator.getVolume() + size)
                .setSumPrice(accumulator.getSumPrice() + price)
                .setSumPriceSize(accumulator.getSumPriceSize() + price * size)
                .setSumSqPrice(accumulator.getSumSqPrice() + price * price)
                .setLastPrice(price)
                .setCurrency(tick.getCurrency())
                .build();
    }

    /** Nombre del state store de cada tipo de ventana. */
    public static String storeName(WindowKind kind) {
        return "metrics-" + kind.name().toLowerCase() + "-store";
    }

    private static SymbolMetrics toMetrics(Windowed<String> windowedKey,
                                           MetricsAccumulator accumulator,
                                           WindowKind kind) {
        return toMetrics(windowedKey.key(), windowedKey.window().start(), windowedKey.window().end(), kind, accumulator);
    }

    /**
     * Las mismas cuentas que hace la topologia, pero a partir de un acumulador suelto.
     * Lo usa la consulta interactiva: lee el state store y aplica esta conversion, en
     * vez de duplicar las formulas.
     */
    public static SymbolMetrics toMetrics(String symbol,
                                          long windowStart,
                                          long windowEnd,
                                          WindowKind kind,
                                          MetricsAccumulator accumulator) {
        long ticks = accumulator.getTicks();
        double mean = ticks == 0 ? 0.0 : accumulator.getSumPrice() / ticks;
        double variance = ticks == 0 ? 0.0 : accumulator.getSumSqPrice() / ticks - mean * mean;
        double vwap = accumulator.getVolume() == 0 ? 0.0 : accumulator.getSumPriceSize() / accumulator.getVolume();

        return SymbolMetrics.newBuilder()
                .setSymbol(symbol)
                .setCurrency(accumulator.getCurrency())
                .setWindowKind(kind)
                .setWindowStart(Instant.ofEpochMilli(windowStart))
                .setWindowEnd(Instant.ofEpochMilli(windowEnd))
                .setTicks(ticks)
                .setVolume(accumulator.getVolume())
                .setVwap(decimal(vwap))
                .setMovingAverage(decimal(mean))
                // max(0, ...) evita un NaN si la aritmetica en coma flotante deja la
                // varianza en un numero negativo minusculo.
                .setVolatility(decimal(Math.sqrt(Math.max(0.0, variance))))
                .setLastPrice(decimal(accumulator.getLastPrice()))
                .build();
    }

    private static BigDecimal decimal(double value) {
        return BigDecimal.valueOf(value).setScale(4, RoundingMode.HALF_UP);
    }
}
