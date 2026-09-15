package com.aggora.analytics.topology;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

import com.aggora.analytics.config.AggoraProperties;
import com.aggora.analytics.config.AvroSerdes;
import com.aggora.avro.analytics.ArbitrageSpread;
import com.aggora.avro.canonical.CanonicalTick;
import com.aggora.avro.reference.FxRate;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * El spread entre las dos cotizaciones de la misma empresa (arbitraje).
 *
 * Aqui se practican los DOS joins que pide el spec:
 *
 *  1) STREAM-TABLE, en su variante GLOBAL: el precio europeo se convierte a dolares
 *     con el ultimo tipo de cambio. Una GlobalKTable es una copia ENTERA de la tabla
 *     en cada instancia (aqui son dos pares de divisas, cabe de sobra), asi que no
 *     hace falta que la clave del stream coincida con la de la tabla: la clave de
 *     busqueda se calcula del propio registro.
 *
 *  2) STREAM-STREAM: se cruzan las dos cotizaciones que caen dentro de la misma
 *     ventana de tiempo. Los dos precios no llegan en el mismo milisegundo, asi que
 *     la ventana es lo que permite emparejarlos. Para que el join funcione, las dos
 *     patas tienen que estar en la MISMA particion: se re-clavan por el simbolo raiz
 *     ("ASML") y Kafka Streams inserta el topic de reparticion que haga falta.
 */
public final class ArbitrageTopology {

    private static final Logger log = LoggerFactory.getLogger(ArbitrageTopology.class);
    private static final AtomicLong SAMPLED = new AtomicLong();

    private ArbitrageTopology() {
    }

    public static void apply(StreamsBuilder builder, AggoraProperties props, AvroSerdes serdes) {
        AggoraProperties.Arbitrage config = props.arbitrage();

        GlobalKTable<String, FxRate> fxRates = builder.globalTable(
                props.topics().fxReference(),
                Consumed.with(Serdes.String(), serdes.fxRates()));

        KStream<String, CanonicalTick> canonical = builder.stream(
                props.topics().ticksCanonical(),
                Consumed.with(Serdes.String(), serdes.canonicalTicks()));

        KStream<String, CanonicalTick> european = canonical.filter(
                (key, tick) -> config.europeanSymbol().equals(tick.getSymbol()));
        KStream<String, CanonicalTick> american = canonical.filter(
                (key, tick) -> config.americanSymbol().equals(tick.getSymbol()));

        // 1) El precio europeo, pasado a dolares con el tipo de cambio de referencia.
        KStream<String, CanonicalTick> europeanInUsd = european.join(
                fxRates,
                (key, tick) -> tick.getCurrency() + "/" + config.quoteCurrency(),
                (tick, rate) -> convertedToUsd(tick, rate, config.quoteCurrency()));

        // 2) Las dos patas, con la misma clave, cruzadas dentro de la ventana.
        KStream<String, ArbitrageSpread> spreads = europeanInUsd
                .selectKey((key, tick) -> config.rootSymbol())
                .join(american.selectKey((key, tick) -> config.rootSymbol()),
                        (europeanTick, americanTick) -> spread(config, europeanTick, americanTick),
                        JoinWindows.ofTimeDifferenceWithNoGrace(config.joinWindow()),
                        // El join guarda la pata izquierda en un state store con ventana,
                        // asi que hay que decirle con que serdes serializarla.
                        StreamJoined.with(Serdes.String(), serdes.canonicalTicks(), serdes.canonicalTicks()));

        spreads
                .peek((key, value) -> {
                    if (SAMPLED.incrementAndGet() % 100 == 0) {
                        log.info("[arbitraje] {} | EU {} USD vs US {} USD | spread {} USD ({} bps)",
                                value.getRootSymbol(), value.getEuropeanPriceUsd(),
                                value.getAmericanPriceUsd(), value.getSpreadUsd(), value.getSpreadBps());
                    }
                })
                .to(props.topics().arbitrage(), Produced.with(Serdes.String(), serdes.spreads()));
    }

    /** El mismo tick, pero con el precio expresado en la divisa de cotizacion. */
    private static CanonicalTick convertedToUsd(CanonicalTick tick, FxRate rate, String quoteCurrency) {
        return CanonicalTick.newBuilder(tick)
                .setPrice(tick.getPrice().multiply(rate.getRate()).setScale(4, RoundingMode.HALF_UP))
                .setCurrency(quoteCurrency)
                .build();
    }

    private static ArbitrageSpread spread(AggoraProperties.Arbitrage config,
                                          CanonicalTick europeanInUsd,
                                          CanonicalTick american) {
        BigDecimal europeanPrice = europeanInUsd.getPrice();
        BigDecimal americanPrice = american.getPrice();
        BigDecimal spreadUsd = americanPrice.subtract(europeanPrice);
        double spreadBps = europeanPrice.signum() == 0
                ? 0.0
                : spreadUsd.doubleValue() / europeanPrice.doubleValue() * 10_000.0;

        return ArbitrageSpread.newBuilder()
                .setRootSymbol(config.rootSymbol())
                .setEuropeanSymbol(europeanInUsd.getSymbol())
                .setAmericanSymbol(american.getSymbol())
                .setEuropeanPriceUsd(europeanPrice)
                .setAmericanPriceUsd(americanPrice)
                .setSpreadUsd(spreadUsd)
                .setSpreadBps(spreadBps)
                .setEuropeanTime(europeanInUsd.getEventTime())
                .setAmericanTime(american.getEventTime())
                .setDetectedAt(Instant.now())
                .build();
    }

    /** La ventana de tiempo del join, para poder usarla en descripciones y tests. */
    static Duration joinWindow(AggoraProperties props) {
        return props.arbitrage().joinWindow();
    }
}
