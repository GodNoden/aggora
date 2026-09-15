package com.aggora.analytics.topology;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import com.aggora.analytics.config.AggoraConfig;
import com.aggora.analytics.config.ConfigDePrueba;
import com.aggora.analytics.config.AvroSerdes;
import com.aggora.avro.analytics.SymbolMetrics;
import com.aggora.avro.analytics.WindowKind;
import com.aggora.avro.canonical.AssetClass;
import com.aggora.avro.canonical.CanonicalTick;
import com.aggora.avro.canonical.Exchange;
import com.aggora.avro.canonical.TickSource;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * La aritmetica de las metricas, probada con TopologyTestDriver: sin broker, sin
 * Schema Registry (la URL "mock://" hace que el serde use un registry en memoria) y
 * sin esperar ventanas de verdad.
 */
class MetricsTopologyTest {

    private static final String CANONICAL = "market.ticks.canonical";
    private static final String ANALYTICS = "market.analytics";

    private static final Instant T0 = Instant.parse("2026-09-15T10:00:00Z");

    private final AggoraConfig props = ConfigDePrueba.de("mock://aggora-test", CANONICAL, ANALYTICS, "market.arbitrage", "market.fx.reference");

    private final AvroSerdes serdes = new AvroSerdes("mock://aggora-test");

    @Test
    @DisplayName("VWAP, media y volatilidad de dos ticks en la misma ventana")
    void calcula_las_tres_metricas() {
        try (TopologyTestDriver driver = driver()) {
            TestInputTopic<String, CanonicalTick> input = input(driver);

            // 10 unidades a 100 y 10 a 110 -> VWAP = 2100/20 = 105, media = 105
            // volatilidad = raiz((10000 + 12100)/2 - 105^2) = raiz(25) = 5
            input.pipeInput("AAPL", tick("AAPL", 100.0, 10, T0), T0);
            input.pipeInput("AAPL", tick("AAPL", 110.0, 10, T0.plusMillis(1)), T0.plusMillis(1));

            SymbolMetrics metrics = lastOf(read(driver), WindowKind.TUMBLING);

            assertEquals("AAPL", metrics.getSymbol());
            assertEquals("USD", metrics.getCurrency());
            assertEquals(2, metrics.getTicks());
            assertEquals(20, metrics.getVolume());
            assertDecimal("105.0000", metrics.getVwap());
            assertDecimal("105.0000", metrics.getMovingAverage());
            assertDecimal("5.0000", metrics.getVolatility());
            assertDecimal("110.0000", metrics.getLastPrice());
            assertEquals(T0, metrics.getWindowStart());
            assertEquals(T0.plusSeconds(30), metrics.getWindowEnd());
        }
    }

    @Test
    @DisplayName("Un tick de REFERENCE (size 0) entra en la media pero no en el VWAP ni en el volumen")
    void los_ticks_de_referencia_no_anaden_volumen() {
        try (TopologyTestDriver driver = driver()) {
            TestInputTopic<String, CanonicalTick> input = input(driver);

            input.pipeInput("AAPL", tick("AAPL", 100.0, 10, T0), T0);
            input.pipeInput("AAPL", tick("AAPL", 110.0, 10, T0.plusMillis(1)), T0.plusMillis(1));
            // Cotizacion de referencia: cuenta como precio, no como operacion.
            input.pipeInput("AAPL", tick("AAPL", 999.0, 0, T0.plusMillis(2)), T0.plusMillis(2));

            SymbolMetrics metrics = lastOf(read(driver), WindowKind.TUMBLING);

            assertEquals(3, metrics.getTicks());
            assertEquals(20, metrics.getVolume());
            assertDecimal("105.0000", metrics.getVwap());
            assertDecimal("403.0000", metrics.getMovingAverage());
        }
    }

    @Test
    @DisplayName("La ventana movil se solapa (varias ventanas) y la fija no (una sola)")
    void la_ventana_movil_solapa() {
        try (TopologyTestDriver driver = driver()) {
            TestInputTopic<String, CanonicalTick> input = input(driver);

            input.pipeInput("AAPL", tick("AAPL", 100.0, 10, T0), T0);
            input.pipeInput("AAPL", tick("AAPL", 110.0, 10, T0.plusSeconds(20)), T0.plusSeconds(20));

            List<SymbolMetrics> all = read(driver).stream().map(record -> record.value).toList();

            long tumblingWindows = all.stream()
                    .filter(metrics -> metrics.getWindowKind() == WindowKind.TUMBLING)
                    .map(SymbolMetrics::getWindowStart)
                    .distinct()
                    .count();
            long hoppingWindows = all.stream()
                    .filter(metrics -> metrics.getWindowKind() == WindowKind.HOPPING)
                    .map(SymbolMetrics::getWindowStart)
                    .distinct()
                    .count();

            // Los dos ticks caen en el mismo bloque fijo (10:00:00-10:00:30)...
            assertEquals(1, tumblingWindows);
            // ...pero la ventana movil de 60 s recalculada cada 15 s los reparte en varias.
            assertTrue(hoppingWindows > 1);
        }
    }

    /** Compara dos decimales por valor, no por escala (que es lo que hacia isEqualByComparingTo). */
    private static void assertDecimal(String esperado, java.math.BigDecimal real) {
        assertEquals(0, real.compareTo(new BigDecimal(esperado)),
                "esperado " + esperado + " y era " + real);
    }

    private TopologyTestDriver driver() {
        StreamsBuilder builder = new StreamsBuilder();
        MetricsTopology.apply(builder, props, serdes);

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "analytics-streams-test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        config.put(StreamsConfig.STATE_DIR_CONFIG, tempStateDir());
        return new TopologyTestDriver(builder.build(), config);
    }

    private TestInputTopic<String, CanonicalTick> input(TopologyTestDriver driver) {
        return driver.createInputTopic(CANONICAL, new StringSerializer(), serdes.canonicalTicks().serializer());
    }

    private List<KeyValue<String, SymbolMetrics>> read(TopologyTestDriver driver) {
        TestOutputTopic<String, SymbolMetrics> output =
                driver.createOutputTopic(ANALYTICS, new StringDeserializer(), serdes.metrics().deserializer());
        return output.readKeyValuesToList();
    }

    private static SymbolMetrics lastOf(List<KeyValue<String, SymbolMetrics>> records, WindowKind kind) {
        return records.stream()
                .map(record -> record.value)
                .filter(metrics -> metrics.getWindowKind() == kind)
                .reduce((first, second) -> second)
                .orElseThrow(() -> new AssertionError("no hay metricas de tipo " + kind));
    }

    /** Un directorio de estado nuevo por driver: si no, dos drivers del mismo test se pisan. */
    private static String tempStateDir() {
        try {
            return java.nio.file.Files.createTempDirectory("aggora-streams-test").toString();
        } catch (java.io.IOException ex) {
            throw new IllegalStateException("no se pudo crear el directorio de estado", ex);
        }
    }

    private static CanonicalTick tick(String symbol, double price, int size, Instant when) {
        return CanonicalTick.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setSymbol(symbol)
                .setAssetClass(AssetClass.EQUITY)
                .setExchange(Exchange.NASDAQ)
                .setCurrency("USD")
                .setPrice(BigDecimal.valueOf(price).setScale(4, RoundingMode.HALF_UP))
                .setSize(size)
                .setEventTime(when)
                .setSource(size == 0 ? TickSource.REFERENCE : TickSource.SYNTHETIC)
                .setSequence(1L)
                .setNormalizedAt(when)
                .setOriginPartition(0)
                .setOriginOffset(0L)
                .build();
    }
}
