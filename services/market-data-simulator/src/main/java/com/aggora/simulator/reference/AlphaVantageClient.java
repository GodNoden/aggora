package com.aggora.simulator.reference;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.aggora.simulator.config.AggoraProperties;
import com.aggora.simulator.config.AggoraProperties.FeedProvider;
import com.fasterxml.jackson.databind.JsonNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

/**
 * Alpha Vantage: la fuente secundaria, solo para lo que Twelve Data no da gratis
 * (Euronext París y Shanghai).
 *
 * Dos diferencias importantes con Twelve Data:
 *
 * 1. No hay endpoint por lotes: es UNA petición por símbolo. Y el plan gratuito
 *    admite 1 petición por SEGUNDO, así que las llamadas van espaciadas. Por eso
 *    esta fuente se consulta cada 3 horas y no cada 15 minutos.
 * 2. El símbolo lleva el mercado dentro: MC.PAR (París), 600519.SHH (Shanghai).
 *    Eso está en el YAML (feed-symbol), no en el código.
 *
 * Cuando se agota la cuota, Alpha Vantage no devuelve un error HTTP: devuelve 200
 * con {"Information": "..."}. Hay que mirar el cuerpo, no solo el código de estado.
 */
@Component
public class AlphaVantageClient implements ReferenceSource {

    private static final Logger log = LoggerFactory.getLogger(AlphaVantageClient.class);

    /** Clave del precio dentro de la respuesta de GLOBAL_QUOTE. */
    private static final String QUOTE_PRICE_FIELD = "05. price";

    /** El plan gratis admite 1 petición/segundo: dejamos margen. */
    private static final long SPACING_MS = 1200;

    private final AggoraProperties props;
    private final RestClient http;

    public AlphaVantageClient(AggoraProperties props) {
        this.props = props;
        this.http = RestClient.builder().baseUrl(props.alphaVantage().baseUrl()).build();
    }

    @Override
    public FeedProvider provider() {
        return FeedProvider.ALPHA_VANTAGE;
    }

    @Override
    public boolean configured() {
        String apiKey = props.alphaVantage().apiKey();
        return apiKey != null && !apiKey.isBlank();
    }

    @Override
    public int dailyBudget() {
        return props.alphaVantage().dailyRequestBudget();
    }

    @Override
    public Map<String, Double> fetchPrices(List<AggoraProperties.Instrument> instruments) {
        Map<String, Double> prices = new LinkedHashMap<>();
        for (int i = 0; i < instruments.size(); i++) {
            AggoraProperties.Instrument instrument = instruments.get(i);
            fetchOne(instrument).ifPresent(price -> prices.put(instrument.symbol(), price));
            if (i < instruments.size() - 1) {
                spaceOutRequests();
            }
        }
        return prices;
    }

    private Optional<Double> fetchOne(AggoraProperties.Instrument instrument) {
        try {
            JsonNode body = http.get()
                    .uri(builder -> builder.path("/query")
                            .queryParam("function", "GLOBAL_QUOTE")
                            .queryParam("symbol", instrument.feedSymbol())
                            .queryParam("apikey", props.alphaVantage().apiKey())
                            .build())
                    .retrieve()
                    .body(JsonNode.class);
            return parseQuote(body, instrument.feedSymbol());
        } catch (RuntimeException ex) {
            log.warn("[alpha-vantage] petición fallida para {}: {}", instrument.feedSymbol(), ex.getMessage());
            return Optional.empty();
        }
    }

    /**
     * Traduce la respuesta de GLOBAL_QUOTE a un precio.
     *
     * Paquete-privado y sin HTTP a propósito: aquí está el caso que muerde (la cuota
     * agotada llega como HTTP 200 con un campo "Information"), y se prueba con test.
     */
    static Optional<Double> parseQuote(JsonNode body, String feedSymbol) {
        if (body == null) {
            return Optional.empty();
        }
        for (String aviso : List.of("Information", "Note", "Error Message")) {
            if (body.hasNonNull(aviso)) {
                log.warn("[alpha-vantage] {}: {}", feedSymbol, body.get(aviso).asText());
                return Optional.empty();
            }
        }
        JsonNode price = body.path("Global Quote").path(QUOTE_PRICE_FIELD);
        if (price.isMissingNode() || price.asText().isBlank()) {
            log.warn("[alpha-vantage] {} sin precio en la respuesta", feedSymbol);
            return Optional.empty();
        }
        return Optional.of(price.asDouble());
    }

    private void spaceOutRequests() {
        try {
            Thread.sleep(SPACING_MS);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }
}
