package com.aggora.analytics.query;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import com.aggora.analytics.config.AggoraConfig;
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

/**
 * Consultas interactivas: preguntarle a la aplicacion EN MARCHA por el estado que ya tiene en su
 * state store local, sin pasar por Kafka.
 *
 * <p>Es la gracia de que Kafka Streams guarde el estado en disco: el VWAP que se acaba de calcular
 * esta ahi, disponible al instante, en lugar de releer el topic de metricas y volver a agregar. La
 * contrapartida: cada instancia solo conoce SUS particiones, asi que con varias instancias habria
 * que preguntar a la que tiene la clave (para eso esta la metadata del stream) o consultarlas
 * todas.
 *
 * <p><b>Diferencia con Spring:</b> alli el motor se saca del {@code StreamsBuilderFactoryBean}
 * ({@code getKafkaStreams()}); aqui la extension de Quarkus lo expone como bean y se inyecta
 * directamente. Y el endpoint es JAX-RS en vez de un {@code @RestController}.
 */
@ApplicationScoped
@Path("/analytics")
public class AnalyticsResource {

    private static final Logger log = LoggerFactory.getLogger(AnalyticsResource.class);

    private final KafkaStreams streams;
    private final AggoraConfig props;

    @Inject
    public AnalyticsResource(KafkaStreams streams, AggoraConfig props) {
        this.streams = streams;
        this.props = props;
    }

    /**
     * Ultimas ventanas de un simbolo.
     * Ejemplo: {@code GET /analytics?symbol=AAPL&minutes=5}
     *
     * <p>El simbolo va como parametro y no en la ruta porque los pares de divisas llevan una barra
     * (EUR/USD) y la barra partiria la URL en dos segmentos.
     */
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response metrics(@QueryParam("symbol") String symbol,
                            @QueryParam("minutes") @DefaultValue("5") int minutes) {
        if (streams.state() == KafkaStreams.State.ERROR) {
            // Estado ERROR: el proceso vive pero el motor no. Es un fallo silencioso y hay que
            // decirlo claro en vez de devolver un 500 generico.
            return error(503, "Kafka Streams esta en estado ERROR: mira su log, el servicio no esta procesando");
        }
        try {
            return Response.ok(consultar(symbol, minutes)).build();
        } catch (InvalidStateStoreException ex) {
            // Pasa en cada despliegue: el hilo de Streams esta STARTING o REBALANCING mientras
            // reconstruye el estado, y durante ese rato el store existe pero no se puede leer. No
            // es un fallo del servicio, es que todavia no esta listo: eso es un 503 con su motivo,
            // no un 500 generico.
            return error(503, "El state store no esta listo todavia (arrancando o rebalanceando): " + ex.getMessage());
        }
    }

    private List<WindowMetrics> consultar(String symbol, int minutes) {
        ReadOnlyWindowStore<String, MetricsAccumulator> store = streams.store(
                StoreQueryParameters.fromNameAndType(
                        MetricsTopology.TUMBLING_STORE, QueryableStoreTypes.windowStore()));
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

    private Response error(int status, String motivo) {
        return Response.status(status).entity(new ErrorResponse(status, motivo)).build();
    }

    /** El cuerpo del error, para que un 503 se pueda leer sin mirar el log. */
    public record ErrorResponse(int status, String motivo) {
    }

    /**
     * La respuesta del endpoint. No se devuelve la clase Avro directamente por dos motivos: el
     * serializador intentaria sacar tambien su getSchema(), y ademas la API HTTP no deberia quedar
     * atada al contrato de Kafka (que evoluciona aparte).
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
