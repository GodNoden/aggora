package com.aggora.analytics.query;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import com.aggora.analytics.config.AggoraProperties;
import com.aggora.analytics.topology.MetricsTopology;
import com.aggora.avro.analytics.MetricsAccumulator;
import com.aggora.avro.analytics.SymbolMetrics;
import com.aggora.avro.analytics.WindowKind;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

/**
 * Consultas interactivas: preguntarle a la aplicacion EN MARCHA por el estado que ya
 * tiene en su state store local, sin pasar por Kafka.
 *
 * Es la gracia de que Kafka Streams guarde el estado en disco: el VWAP que se acaba de
 * calcular esta ahi, disponible al instante, en lugar de releer el topic de metricas y
 * volver a agregar. La contrapartida: cada instancia solo conoce SUS particiones, asi
 * que con varias instancias habria que preguntar a la que tiene la clave (para eso esta
 * la metadata del stream) o consultarlas todas.
 */
@RestController
public class AnalyticsQueryController {

    private static final Logger log = LoggerFactory.getLogger(AnalyticsQueryController.class);

    private final StreamsBuilderFactoryBean streamsBuilderFactoryBean;
    private final AggoraProperties props;

    public AnalyticsQueryController(StreamsBuilderFactoryBean streamsBuilderFactoryBean, AggoraProperties props) {
        this.streamsBuilderFactoryBean = streamsBuilderFactoryBean;
        this.props = props;
    }

    /**
     * Ultimas ventanas de un simbolo.
     * Ejemplo: GET /analytics?symbol=AAPL&minutes=5
     *
     * El simbolo va como parametro y no en la ruta porque los pares de divisas llevan
     * una barra (EUR/USD) y la barra parte la URL en dos segmentos.
     */
    @GetMapping("/analytics")
    public List<WindowMetrics> metrics(@RequestParam String symbol,
                                       @RequestParam(defaultValue = "5") int minutes) {
        ReadOnlyWindowStore<String, MetricsAccumulator> store = windowStore();
        Instant to = Instant.now();
        Instant from = to.minus(Duration.ofMinutes(minutes));
        long windowMillis = props.windows().tumblingSize().toMillis();

        List<WindowMetrics> found = new ArrayList<>();
        try (WindowStoreIterator<MetricsAccumulator> windows = store.fetch(symbol, from, to)) {
            while (windows.hasNext()) {
                KeyValue<Long, MetricsAccumulator> window = windows.next();
                // La misma conversion que usa la topologia: nada de duplicar formulas.
                found.add(WindowMetrics.from(MetricsTopology.toMetrics(
                        symbol, window.key, window.key + windowMillis,
                        WindowKind.TUMBLING, window.value)));
            }
        }
        log.info("[consulta] {} ventanas de {} en los ultimos {} min", found.size(), symbol, minutes);
        return found;
    }

    private ReadOnlyWindowStore<String, MetricsAccumulator> windowStore() {
        KafkaStreams streams = streamsBuilderFactoryBean.getKafkaStreams();
        if (streams == null) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Kafka Streams todavia no esta en marcha");
        }
        try {
            return streams.store(StoreQueryParameters.fromNameAndType(
                    MetricsTopology.TUMBLING_STORE, QueryableStoreTypes.windowStore()));
        } catch (InvalidStateStoreException ex) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE,
                    "El state store no esta disponible (rebalanceando?): " + ex.getMessage());
        }
    }

    /**
     * La respuesta del endpoint. No se devuelve la clase Avro directamente por dos
     * motivos: Jackson intenta serializar tambien su getSchema() y revienta, y ademas
     * la API HTTP no deberia quedar atada al contrato de Kafka (que evoluciona aparte).
     */
    public record WindowMetrics(
            String symbol,
            String currency,
            String windowKind,
            Instant windowStart,
            Instant windowEnd,
            long ticks,
            long volume,
            java.math.BigDecimal vwap,
            java.math.BigDecimal movingAverage,
            java.math.BigDecimal volatility,
            java.math.BigDecimal lastPrice) {

        static WindowMetrics from(SymbolMetrics metrics) {
            return new WindowMetrics(
                    metrics.getSymbol(), metrics.getCurrency(), metrics.getWindowKind().name(),
                    metrics.getWindowStart(), metrics.getWindowEnd(),
                    metrics.getTicks(), metrics.getVolume(),
                    metrics.getVwap(), metrics.getMovingAverage(), metrics.getVolatility(), metrics.getLastPrice());
        }
    }
}
