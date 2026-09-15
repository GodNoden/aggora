package com.aggora.alerting.topology;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import com.aggora.alerting.config.AggoraProperties;
import com.aggora.alerting.config.AvroSerdes;
import com.aggora.avro.alerts.Alert;
import com.aggora.avro.alerts.AlertType;
import com.aggora.avro.analytics.SymbolMetrics;
import com.aggora.avro.analytics.WindowKind;
import com.aggora.avro.portfolio.PortfolioPosition;

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
 * Las tres reglas de alerta. La del feed parado se prueba avanzando el reloj a mano
 * (advanceWallClockTime), que es justo lo que hace el driver con los punctuators.
 */
class AlertingTopologyTest {

    private static final String ANALYTICS = "market.analytics";
    private static final String PORTFOLIO = "portfolio.updates";
    private static final String ALERTS = "alerts.raised";
    private static final Instant T0 = Instant.parse("2026-09-15T10:00:00Z");

    private final AggoraProperties props = new AggoraProperties(
            "mock://aggora-test",
            new AggoraProperties.Topics(ANALYTICS, PORTFOLIO, ALERTS),
            40.0,
            20.0,
            Duration.ofSeconds(30),
            Duration.ofSeconds(10));

    private final AvroSerdes serdes = new AvroSerdes("mock://aggora-test");

    @Test
    @DisplayName("Pico de precio: avisa cuando el ultimo precio se aleja de la media")
    void pico_de_precio() {
        try (TopologyTestDriver driver = driver()) {
            metrics(driver).pipeInput("AAPL", metrics("AAPL", "100.00", "100.00"), T0);
            assertThat(alerts(driver)).isEmpty();

            // 102 contra una media de 100 son 200 puntos basicos: muy por encima de 40.
            metrics(driver).pipeInput("AAPL", metrics("AAPL", "102.00", "100.00"), T0.plusSeconds(1));

            List<Alert> raised = alerts(driver);
            assertThat(raised).hasSize(1);
            assertThat(raised.getFirst().getType()).isEqualTo(AlertType.PRICE_SPIKE);
            assertThat(raised.getFirst().getSubject()).isEqualTo("AAPL");
            assertThat(raised.getFirst().getValue()).isEqualByComparingTo(new BigDecimal("200.0000"));

            // Y no repite mientras el pico siga ahi: un aviso por episodio, no uno por
            // cada actualizacion de la ventana (que es lo que pasaba en la vida real).
            metrics(driver).pipeInput("AAPL", metrics("AAPL", "103.00", "100.00"), T0.plusSeconds(2));
            assertThat(alerts(driver)).isEmpty();

            // Se normaliza y vuelve a dispararse: nueva alerta.
            metrics(driver).pipeInput("AAPL", metrics("AAPL", "100.10", "100.00"), T0.plusSeconds(3));
            metrics(driver).pipeInput("AAPL", metrics("AAPL", "105.00", "100.00"), T0.plusSeconds(4));
            assertThat(alerts(driver)).hasSize(1);
        }
    }

    @Test
    @DisplayName("Margen superado: la alerta la dispara la marca que pone portfolio-risk")
    void margen_superado() {
        try (TopologyTestDriver driver = driver()) {
            TestInputTopic<String, PortfolioPosition> input = driver.createInputTopic(
                    PORTFOLIO, new StringSerializer(), serdes.positions().serializer());

            input.pipeInput("ACC-01", position("ACC-01", false, "1000.00", "500000.00"), T0);
            assertThat(alerts(driver)).isEmpty();

            input.pipeInput("ACC-01", position("ACC-01", true, "600000.00", "500000.00"), T0.plusSeconds(1));

            List<Alert> raised = alerts(driver);
            assertThat(raised).hasSize(1);
            assertThat(raised.getFirst().getType()).isEqualTo(AlertType.MARGIN_BREACH);
            assertThat(raised.getFirst().getSubject()).isEqualTo("ACC-01");
            assertThat(raised.getFirst().getValue()).isEqualByComparingTo(new BigDecimal("600000.0000"));
        }
    }

    @Test
    @DisplayName("Feed parado: avisa UNA vez por episodio y se rearma cuando vuelven los datos")
    void feed_parado() {
        try (TopologyTestDriver driver = driver()) {
            metrics(driver).pipeInput("AAPL", metrics("AAPL", "100.00", "100.00"), T0);

            // Pasa el tiempo sin datos: a los 40 s (limite 30 s) salta la alerta.
            driver.advanceWallClockTime(Duration.ofSeconds(40));
            List<Alert> first = alerts(driver);
            assertThat(first).hasSize(1);
            assertThat(first.getFirst().getType()).isEqualTo(AlertType.STALE_FEED);

            // Y no se repite en cada vuelta del punctuator: un episodio, una alerta.
            driver.advanceWallClockTime(Duration.ofSeconds(60));
            assertThat(alerts(driver)).isEmpty();

            // Vuelven los datos y se vuelve a quedar callado: nueva alerta.
            metrics(driver).pipeInput("AAPL", metrics("AAPL", "100.00", "100.00"), T0.plusSeconds(120));
            driver.advanceWallClockTime(Duration.ofSeconds(40));
            assertThat(alerts(driver)).hasSize(1);
        }
    }

    private TopologyTestDriver driver() {
        StreamsBuilder builder = new StreamsBuilder();
        AlertingTopology.apply(builder, props, serdes);

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "alerting-test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        config.put(StreamsConfig.STATE_DIR_CONFIG, tempStateDir());
        return new TopologyTestDriver(builder.build(), config);
    }

    private TestInputTopic<String, SymbolMetrics> metrics(TopologyTestDriver driver) {
        return driver.createInputTopic(ANALYTICS, new StringSerializer(), serdes.metrics().serializer());
    }

    private List<Alert> alerts(TopologyTestDriver driver) {
        TestOutputTopic<String, Alert> output =
                driver.createOutputTopic(ALERTS, new StringDeserializer(), serdes.alerts().deserializer());
        return output.readKeyValuesToList().stream().map(record -> record.value).toList();
    }

    private static SymbolMetrics metrics(String symbol, String lastPrice, String movingAverage) {
        return SymbolMetrics.newBuilder()
                .setSymbol(symbol)
                .setCurrency("USD")
                .setWindowKind(WindowKind.HOPPING)
                .setWindowStart(T0)
                .setWindowEnd(T0.plusSeconds(60))
                .setTicks(10L)
                .setVolume(100L)
                .setVwap(new BigDecimal(lastPrice).setScale(4, RoundingMode.HALF_UP))
                .setMovingAverage(new BigDecimal(movingAverage).setScale(4, RoundingMode.HALF_UP))
                .setVolatility(new BigDecimal("1.0000"))
                .setLastPrice(new BigDecimal(lastPrice).setScale(4, RoundingMode.HALF_UP))
                .build();
    }

    private static PortfolioPosition position(String accountId, boolean breach, String exposure, String limit) {
        return PortfolioPosition.newBuilder()
                .setAccountId(accountId)
                .setSymbol("AAPL")
                .setCurrency("USD")
                .setQuantity(100L)
                .setAverageCost(new BigDecimal("100.0000"))
                .setRealizedPnl(new BigDecimal("0.0000"))
                .setExposure(new BigDecimal(exposure).setScale(4, RoundingMode.HALF_UP))
                .setMarginLimit(new BigDecimal(limit).setScale(4, RoundingMode.HALF_UP))
                .setMarginBreach(breach)
                .setExecutions(Long.parseLong("1"))
                .setLastUpdated(T0)
                .build();
    }

    private static String tempStateDir() {
        try {
            return java.nio.file.Files.createTempDirectory("aggora-alerting-test").toString();
        } catch (java.io.IOException ex) {
            throw new IllegalStateException("no se pudo crear el directorio de estado", ex);
        }
    }
}
