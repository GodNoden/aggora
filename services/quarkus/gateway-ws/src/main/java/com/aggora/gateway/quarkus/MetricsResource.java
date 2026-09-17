package com.aggora.gateway.quarkus;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * Proxy de Prometheus con CATALOGO CERRADO: {@code GET /api/metrics?panel=<nombre>}.
 *
 * <p>Es el gemelo del {@code MetricsController} de Spring. No se acepta PromQL libre (nada de
 * {@code ?query=}): el catalogo vive en el codigo, asi que la pagina no puede pedirle a Prometheus
 * cualquier cosa. Solo lectura.
 *
 * <p>El cliente HTTP es el del JDK ({@code java.net.http.HttpClient}): no hace falta el
 * rest-client de Quarkus para una llamada GET que devuelve JSON, y asi no se anade una pieza mas.
 *
 * <p>Cada panel devuelve {@code {panel, ts, stack, series:[{label, points:[[ts, valor]]}]}}. Si un
 * panel no tiene datos en este entorno, {@code series} va vacio y se explica el por que en
 * {@code nota}.
 */
@Path("/api/metrics")
public class MetricsResource {

    private record Consulta(String prefijo, String promql) {
    }

    private static final List<String> CLAVES_ETIQUETA =
            List.of("consumergroup", "stack", "service", "topic", "partition", "job", "instance");

    private static final String NOTA_TRANSACCIONES =
            "Prometheus no publica confirmadas frente a abortadas: el exporter de Kafka solo da "
            + "offsets, lag y grupos, y las metricas kafka_producer_txn_*_time_ns_total del cliente "
            + "son TIEMPOS (aqui valen 0), no un recuento. La semantica exactly-once se demuestra "
            + "con scripts/ExactlyOnceRaceCheck.java y se vigila por el lag de orders.executions.";

    private static final Map<String, List<Consulta>> PANELES = paneles();
    private static final Map<String, List<Consulta>> COMPARATIVA = comparativa();

    @Inject
    ObjectMapper json;

    @ConfigProperty(name = "aggora.stack", defaultValue = "quarkus")
    String stack;

    @ConfigProperty(name = "aggora.prometheus-url", defaultValue = "http://prometheus:9090")
    String prometheusUrl;

    private final HttpClient http = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response metrics(@QueryParam("panel") String panel, @QueryParam("de") String de) {
        if (panel == null || panel.isBlank()) {
            return error(400, "falta el parametro panel", catalogo());
        }

        List<Consulta> consultas;
        String comparado = null;
        if ("comparativa".equals(panel)) {
            comparado = (de == null || de.isBlank()) ? "pulso" : de;
            consultas = COMPARATIVA.get(comparado);
            if (consultas == null) {
                return error(400, "comparativa no soporta de=" + comparado, catalogo());
            }
        } else {
            consultas = PANELES.get(panel);
            if (consultas == null) {
                return error(400, "panel desconocido: " + panel, catalogo());
            }
        }

        List<Map<String, Object>> series = new ArrayList<>();
        try {
            for (Consulta consulta : consultas) {
                series.addAll(consultar(consulta));
            }
        } catch (Exception fallo) {
            return error(502, "prometheus no responde", String.valueOf(fallo.getMessage()));
        }

        Map<String, Object> cuerpo = new LinkedHashMap<>();
        cuerpo.put("panel", panel);
        cuerpo.put("ts", Instant.now().toString());
        if (comparado != null) {
            cuerpo.put("de", comparado);
        }
        cuerpo.put("stack", stack);
        cuerpo.put("series", series);
        if (series.isEmpty()) {
            boolean sonTransacciones = "transacciones".equals(panel) || "transacciones".equals(comparado);
            cuerpo.put("nota", sonTransacciones ? NOTA_TRANSACCIONES : "sin datos en este entorno");
        }
        return Response.ok(cuerpo).build();
    }

    /** Ejecuta una consulta instantanea y la convierte a la forma del contrato. */
    private List<Map<String, Object>> consultar(Consulta consulta) throws Exception {
        JsonNode raiz = pedir(consulta.promql());
        long ahora = Instant.now().getEpochSecond();
        List<Map<String, Object>> series = new ArrayList<>();
        for (JsonNode resultado : raiz.path("data").path("result")) {
            JsonNode muestra = resultado.path("value");
            double numero = muestra.path(1).asDouble(Double.NaN);
            List<Object> punto = new ArrayList<>(2);
            punto.add(muestra.path(0).isMissingNode() ? ahora : (long) muestra.path(0).asDouble(ahora));
            punto.add(Double.isFinite(numero) ? numero : null);
            Map<String, Object> serie = new LinkedHashMap<>();
            serie.put("label", etiqueta(consulta.prefijo(), resultado.path("metric")));
            serie.put("points", List.of(punto));
            series.add(serie);
        }
        return series;
    }

    private JsonNode pedir(String promql) throws Exception {
        // El PromQL va codificado a mano: las llaves de los selectores no son plantillas de URI.
        URI uri = URI.create(prometheusUrl + "/api/v1/query?query="
                + URLEncoder.encode(promql, StandardCharsets.UTF_8));
        HttpRequest peticion = HttpRequest.newBuilder(uri)
                .timeout(Duration.ofSeconds(5))
                .GET()
                .build();
        HttpResponse<String> respuesta = http.send(peticion, HttpResponse.BodyHandlers.ofString());
        if (respuesta.statusCode() != 200) {
            throw new IllegalStateException("prometheus contesto HTTP " + respuesta.statusCode());
        }
        JsonNode raiz = json.readTree(respuesta.body());
        if (raiz == null || !"success".equals(raiz.path("status").asText())) {
            throw new IllegalStateException("prometheus contesto "
                    + (raiz == null ? "vacio" : raiz.path("error").asText()));
        }
        return raiz;
    }

    /** La etiqueta legible de una serie: el prefijo del catalogo y las etiquetas que la identifican. */
    private static String etiqueta(String prefijo, JsonNode metric) {
        List<String> partes = new ArrayList<>();
        if (prefijo != null && !prefijo.isBlank()) {
            partes.add(prefijo);
        }
        for (String clave : CLAVES_ETIQUETA) {
            if (metric.has(clave)) {
                partes.add(metric.get(clave).asText());
            }
        }
        return String.join("/", partes);
    }

    private Response error(int estado, String mensaje, Object detalle) {
        Map<String, Object> cuerpo = new LinkedHashMap<>();
        cuerpo.put("error", mensaje);
        if (detalle != null) {
            cuerpo.put("detalle", detalle);
        }
        return Response.status(estado).entity(cuerpo).build();
    }

    private static List<String> catalogo() {
        List<String> nombres = new ArrayList<>(PANELES.keySet());
        nombres.add("comparativa");
        return nombres;
    }

    /** Las consultas exactas de cada panel, las MISMAS que el gateway de Spring. */
    private static Map<String, List<Consulta>> paneles() {
        Map<String, List<Consulta>> mapa = new LinkedHashMap<>();
        mapa.put("pulso", List.of(
                new Consulta("entrada", "sum(rate(kafka_topic_partition_current_offset{topic=\"market.ticks.raw\"}[1m]))"),
                new Consulta("salida-spring", "sum(rate(kafka_topic_partition_current_offset{topic=\"market.ticks.canonical\"}[1m]))"),
                new Consulta("salida-quarkus", "sum(rate(kafka_topic_partition_current_offset{topic=\"market.ticks.canonical.q\"}[1m]))")));
        mapa.put("lag", List.of(
                new Consulta("", "sum by (consumergroup) (kafka_consumergroup_lag{consumergroup=~\"ingestion-normalizer|ingestion-normalizer-q|analytics-streams|analytics-streams-q\"})")));
        mapa.put("particiones", List.of(
                new Consulta("", "sum by (topic, partition) (kafka_topic_partition_current_offset{topic=~\"market.ticks.raw|market.ticks.canonical|market.ticks.canonical.q|market.analytics|market.analytics.q\"})")));
        mapa.put("transacciones", List.of());
        mapa.put("descartes", List.of(
                new Consulta("", "sum by (topic, partition) (kafka_topic_partition_current_offset{topic=~\"market.ticks.raw.DLT|market.ticks.raw.DLT.q|.*\\\\.retry-.*\"})")));
        mapa.put("salud", List.of(
                new Consulta("", "up"),
                new Consulta("", "sum by (stack, service) (aggora_kafka_streams_running)"),
                new Consulta("under-replicated", "sum by (topic) (kafka_topic_partition_under_replicated_partition)")));
        return mapa;
    }

    /** El panel que se pida (de), con su serie de Spring y la de Quarkus al lado. */
    private static Map<String, List<Consulta>> comparativa() {
        Map<String, List<Consulta>> mapa = new LinkedHashMap<>();
        mapa.put("pulso", List.of(
                new Consulta("spring", "sum(rate(kafka_topic_partition_current_offset{topic=\"market.ticks.canonical\"}[1m]))"),
                new Consulta("quarkus", "sum(rate(kafka_topic_partition_current_offset{topic=\"market.ticks.canonical.q\"}[1m]))")));
        mapa.put("lag", List.of(
                new Consulta("spring/ingestion-normalizer", "sum(kafka_consumergroup_lag{consumergroup=\"ingestion-normalizer\"})"),
                new Consulta("quarkus/ingestion-normalizer-q", "sum(kafka_consumergroup_lag{consumergroup=\"ingestion-normalizer-q\"})"),
                new Consulta("spring/analytics-streams", "sum(kafka_consumergroup_lag{consumergroup=\"analytics-streams\"})"),
                new Consulta("quarkus/analytics-streams-q", "sum(kafka_consumergroup_lag{consumergroup=\"analytics-streams-q\"})")));
        mapa.put("particiones", List.of(
                new Consulta("spring/market.ticks.canonical", "sum(kafka_topic_partition_current_offset{topic=\"market.ticks.canonical\"})"),
                new Consulta("quarkus/market.ticks.canonical.q", "sum(kafka_topic_partition_current_offset{topic=\"market.ticks.canonical.q\"})"),
                new Consulta("spring/market.analytics", "sum(kafka_topic_partition_current_offset{topic=\"market.analytics\"})"),
                new Consulta("quarkus/market.analytics.q", "sum(kafka_topic_partition_current_offset{topic=\"market.analytics.q\"})")));
        mapa.put("descartes", List.of(
                new Consulta("spring/market.ticks.raw.DLT", "sum(kafka_topic_partition_current_offset{topic=\"market.ticks.raw.DLT\"})"),
                new Consulta("quarkus/market.ticks.raw.DLT.q", "sum(kafka_topic_partition_current_offset{topic=\"market.ticks.raw.DLT.q\"})")));
        mapa.put("salud", List.of(
                new Consulta("", "sum by (stack, service) (aggora_kafka_streams_running)")));
        mapa.put("transacciones", List.of());
        return mapa;
    }
}
