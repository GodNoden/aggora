package com.aggora.portfolio.topology;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import com.aggora.avro.orders.Execution;
import com.aggora.avro.portfolio.PortfolioPosition;
import com.aggora.portfolio.config.AggoraProperties;
import com.aggora.portfolio.config.AvroSerdes;

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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * La aritmetica de la cartera: coste medio, resultado realizado y vuelta de posicion.
 * Con TopologyTestDriver y registry "mock://": sin broker ni Schema Registry.
 */
class PortfolioTopologyTest {

    private static final String EXECUTIONS = "orders.executions";
    private static final String UPDATES = "portfolio.updates";
    private static final Instant T0 = Instant.parse("2026-09-15T10:00:00Z");

    private final AggoraProperties props = new AggoraProperties(
            "mock://aggora-test",
            new AggoraProperties.Topics(EXECUTIONS, UPDATES),
            new BigDecimal("500000"));

    private final AvroSerdes serdes = new AvroSerdes("mock://aggora-test");

    @Test
    @DisplayName("Dos compras: coste medio ponderado")
    void coste_medio_ponderado() {
        try (TopologyTestDriver driver = driver()) {
            // ACC-01 compra 100 a 10 y 100 a 12 -> 200 al coste medio de 11.
            execution(driver, "ACC-01", "ACC-02", "100.00", 100, T0);
            execution(driver, "ACC-01", "ACC-02", "120.00", 100, T0.plusSeconds(1));

            PortfolioPosition position = lastOf(positions(driver), "ACC-01");

            assertThat(position.getAccountId()).isEqualTo("ACC-01");
            assertThat(position.getSymbol()).isEqualTo("AAPL");
            assertThat(position.getQuantity()).isEqualTo(200);
            assertThat(position.getAverageCost()).isEqualByComparingTo(new BigDecimal("110.0000"));
            assertThat(position.getRealizedPnl()).isEqualByComparingTo(new BigDecimal("0.0000"));
            assertThat(position.getExposure()).isEqualByComparingTo(new BigDecimal("22000.0000"));
            assertThat(position.getMarginBreach()).isFalse();
            assertThat(position.getExecutions()).isEqualTo(2);
        }
    }

    @Test
    @DisplayName("Al cerrar parte se materializa el resultado y el coste medio no cambia")
    void cerrar_parte_materializa_resultado() {
        try (TopologyTestDriver driver = driver()) {
            execution(driver, "ACC-01", "ACC-02", "100.00", 100, T0);
            execution(driver, "ACC-01", "ACC-02", "120.00", 100, T0.plusSeconds(1));
            // Vende 50 a 150: gana (150 - 110) * 50 = 2000, y le quedan 150 a coste 110.
            Execution venta = execution(driver, "ACC-03", "ACC-01", "150.00", 50, T0.plusSeconds(2));

            List<PortfolioPosition> all = positions(driver);
            PortfolioPosition acc01 = lastOf(all, "ACC-01");

            assertThat(acc01.getAccountId()).isEqualTo("ACC-01");
            assertThat(acc01.getQuantity()).isEqualTo(150);
            assertThat(acc01.getAverageCost()).isEqualByComparingTo(new BigDecimal("110.0000"));
            assertThat(acc01.getRealizedPnl()).isEqualByComparingTo(new BigDecimal("2000.0000"));

            // Y la contraparte (ACC-03) tiene su propia posicion: compro 50, asi que
            // queda larga. Cada ejecucion mueve DOS cuentas, no una.
            assertThat(lastOf(all, "ACC-03").getQuantity()).isEqualTo(50);
            assertThat(venta).isNotNull();
        }
    }

    @Test
    @DisplayName("Si la operacion da la vuelta a la posicion, el resto abre al precio nuevo")
    void dar_la_vuelta() {
        try (TopologyTestDriver driver = driver()) {
            // ACC-01 compra 100 a 100.
            execution(driver, "ACC-01", "ACC-02", "100.00", 100, T0);
            // Vende 150 a 120: cierra 100 con resultado (120-100)*100 = 2000 y se queda
            // corto 50, con coste medio 120 (el precio de esta operacion).
            execution(driver, "ACC-03", "ACC-01", "120.00", 150, T0.plusSeconds(1));

            List<PortfolioPosition> all = positions(driver);
            PortfolioPosition acc01 = lastOf(all, "ACC-01");

            assertThat(acc01.getAccountId()).isEqualTo("ACC-01");
            assertThat(acc01.getQuantity()).isEqualTo(-50);
            assertThat(acc01.getAverageCost()).isEqualByComparingTo(new BigDecimal("120.0000"));
            assertThat(acc01.getRealizedPnl()).isEqualByComparingTo(new BigDecimal("2000.0000"));
        }
    }

    private TopologyTestDriver driver() {
        StreamsBuilder builder = new StreamsBuilder();
        PortfolioTopology.apply(builder, props, serdes);

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "portfolio-test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        config.put(StreamsConfig.STATE_DIR_CONFIG, tempStateDir());
        return new TopologyTestDriver(builder.build(), config);
    }

    private Execution execution(TopologyTestDriver driver, String buyer, String seller,
                                String price, int quantity, Instant when) {
        Execution execution = Execution.newBuilder()
                .setExecutionId(UUID.randomUUID().toString())
                .setSymbol("AAPL")
                .setPrice(new BigDecimal(price).setScale(4, RoundingMode.HALF_UP))
                .setQuantity(quantity)
                .setCurrency("USD")
                .setBuyOrderId(UUID.randomUUID().toString())
                .setSellOrderId(UUID.randomUUID().toString())
                .setBuyAccountId(buyer)
                .setSellAccountId(seller)
                .setExecutedAt(when)
                .build();
        TestInputTopic<String, Execution> input =
                driver.createInputTopic(EXECUTIONS, new StringSerializer(), serdes.executions().serializer());
        input.pipeInput("AAPL", execution, when);
        return execution;
    }

    private List<PortfolioPosition> positions(TopologyTestDriver driver) {
        TestOutputTopic<String, PortfolioPosition> output =
                driver.createOutputTopic(UPDATES, new StringDeserializer(), serdes.positions().deserializer());
        return output.readKeyValuesToList().stream().map(record -> record.value).toList();
    }

    /**
     * La ultima posicion DE UNA CUENTA dentro de lo ya leido. Hace falta filtrar porque
     * cada ejecucion genera dos posiciones (la del comprador y la del vendedor) y el topic
     * trae las de todos. Se filtra sobre la lista ya leida: el lector del topic se agota
     * al leerlo, asi que no se puede leer dos veces.
     */
    private static PortfolioPosition lastOf(List<PortfolioPosition> positions, String accountId) {
        return positions.stream()
                .filter(position -> accountId.equals(position.getAccountId()))
                .reduce((first, second) -> second)
                .orElseThrow(() -> new AssertionError("sin posiciones para " + accountId));
    }

    private static String tempStateDir() {
        try {
            return java.nio.file.Files.createTempDirectory("aggora-portfolio-test").toString();
        } catch (java.io.IOException ex) {
            throw new IllegalStateException("no se pudo crear el directorio de estado", ex);
        }
    }
}
