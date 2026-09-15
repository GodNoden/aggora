package com.aggora.analytics.topology;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import com.aggora.analytics.config.AggoraProperties;
import com.aggora.analytics.config.AvroSerdes;
import com.aggora.avro.analytics.ArbitrageSpread;
import com.aggora.avro.canonical.AssetClass;
import com.aggora.avro.canonical.CanonicalTick;
import com.aggora.avro.canonical.Exchange;
import com.aggora.avro.canonical.TickSource;
import com.aggora.avro.reference.FxRate;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * El spread de arbitraje: la conversion de divisa (join con la tabla global de tipos de
 * cambio) y el cruce de las dos cotizaciones dentro de una ventana (join stream-stream).
 *
 * Los dos joins se prueban sin broker ni registry, con los dos topics como entrada.
 */
class ArbitrageTopologyTest {

    private static final String CANONICAL = "market.ticks.canonical";
    private static final String ARBITRAGE = "market.arbitrage";
    private static final String FX = "market.fx.reference";

    private static final Instant T0 = Instant.parse("2026-09-15T10:00:00Z");

    private final AggoraProperties props = new AggoraProperties(
            "mock://aggora-test",
            new AggoraProperties.Topics(CANONICAL, "market.analytics", ARBITRAGE, FX),
            new AggoraProperties.Windows(
                    Duration.ofSeconds(30), Duration.ofSeconds(60), Duration.ofSeconds(15), Duration.ofSeconds(5)),
            new AggoraProperties.Arbitrage("ASML", "ASML.AMS", "ASML", "USD", Duration.ofSeconds(5)));

    private final AvroSerdes serdes = new AvroSerdes("mock://aggora-test");

    @Test
    @DisplayName("Convierte el precio europeo a dolares y calcula el spread")
    void convierte_y_calcula_el_spread() {
        try (TopologyTestDriver driver = driver()) {
            // Un euro vale 1,25 dolares.
            fx(driver).pipeInput("EUR/USD", fxRate("EUR/USD", 1.25), T0.minusSeconds(1));

            // ASML en Amsterdam a 100 EUR -> 125 USD; en NASDAQ a 130 USD.
            tick(driver, "ASML.AMS", "EUR", 100.0, T0);
            tick(driver, "ASML", "USD", 130.0, T0.plusMillis(500));

            List<ArbitrageSpread> spreads = spreads(driver);

            assertThat(spreads).hasSize(1);
            ArbitrageSpread spread = spreads.getFirst();
            assertThat(spread.getRootSymbol()).isEqualTo("ASML");
            assertThat(spread.getEuropeanSymbol()).isEqualTo("ASML.AMS");
            assertThat(spread.getAmericanSymbol()).isEqualTo("ASML");
            assertThat(spread.getEuropeanPriceUsd()).isEqualByComparingTo(new BigDecimal("125.0000"));
            assertThat(spread.getAmericanPriceUsd()).isEqualByComparingTo(new BigDecimal("130.0000"));
            assertThat(spread.getSpreadUsd()).isEqualByComparingTo(new BigDecimal("5.0000"));
            // 5 sobre 125 = 4% = 400 puntos basicos
            assertThat(spread.getSpreadBps()).isEqualTo(400.0);
        }
    }

    @Test
    @DisplayName("Si los dos precios se separan mas que la ventana, no hay spread")
    void fuera_de_la_ventana_no_hay_spread() {
        try (TopologyTestDriver driver = driver()) {
            fx(driver).pipeInput("EUR/USD", fxRate("EUR/USD", 1.25), T0.minusSeconds(1));

            tick(driver, "ASML.AMS", "EUR", 100.0, T0);
            // 30 segundos despues: la ventana del join son 5 segundos.
            tick(driver, "ASML", "USD", 130.0, T0.plusSeconds(30));

            assertThat(spreads(driver)).isEmpty();
        }
    }

    @Test
    @DisplayName("Sin tipo de cambio de referencia no se inventa nada: no hay spread")
    void sin_tipo_de_cambio_no_hay_spread() {
        try (TopologyTestDriver driver = driver()) {
            // No se publica ningun EUR/USD.
            tick(driver, "ASML.AMS", "EUR", 100.0, T0);
            tick(driver, "ASML", "USD", 130.0, T0.plusMillis(500));

            assertThat(spreads(driver)).isEmpty();
        }
    }

    private TopologyTestDriver driver() {
        StreamsBuilder builder = new StreamsBuilder();
        ArbitrageTopology.apply(builder, props, serdes);

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "arbitrage-test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        config.put(StreamsConfig.STATE_DIR_CONFIG, tempStateDir());
        return new TopologyTestDriver(builder.build(), config);
    }

    private TestInputTopic<String, FxRate> fx(TopologyTestDriver driver) {
        return driver.createInputTopic(FX, new StringSerializer(), serdes.fxRates().serializer());
    }

    private void tick(TopologyTestDriver driver, String symbol, String currency, double price, Instant when) {
        driver.createInputTopic(CANONICAL, new StringSerializer(), serdes.canonicalTicks().serializer())
                .pipeInput(symbol, tick(symbol, currency, price, when), when);
    }

    private List<ArbitrageSpread> spreads(TopologyTestDriver driver) {
        TestOutputTopic<String, ArbitrageSpread> output =
                driver.createOutputTopic(ARBITRAGE, new StringDeserializer(), serdes.spreads().deserializer());
        return output.readValuesToList();
    }

    /** Un directorio de estado nuevo por driver: si no, dos drivers del mismo test se pisan. */
    private static String tempStateDir() {
        try {
            return java.nio.file.Files.createTempDirectory("aggora-streams-test").toString();
        } catch (java.io.IOException ex) {
            throw new IllegalStateException("no se pudo crear el directorio de estado", ex);
        }
    }

    private static FxRate fxRate(String pair, double rate) {
        return FxRate.newBuilder()
                .setPair(pair)
                .setRate(BigDecimal.valueOf(rate).setScale(4, RoundingMode.HALF_UP))
                .setEventTime(T0.minusSeconds(1))
                .build();
    }

    private static CanonicalTick tick(String symbol, String currency, double price, Instant when) {
        return CanonicalTick.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setSymbol(symbol)
                .setAssetClass(AssetClass.EQUITY)
                .setExchange(Exchange.NASDAQ)
                .setCurrency(currency)
                .setPrice(BigDecimal.valueOf(price).setScale(4, RoundingMode.HALF_UP))
                .setSize(10)
                .setEventTime(when)
                .setSource(TickSource.SYNTHETIC)
                .setSequence(1L)
                .setNormalizedAt(when)
                .setOriginPartition(0)
                .setOriginOffset(0L)
                .build();
    }
}
