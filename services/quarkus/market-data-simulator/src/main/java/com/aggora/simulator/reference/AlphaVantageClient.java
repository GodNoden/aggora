package com.aggora.simulator.reference;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import com.aggora.simulator.config.AggoraConfig;
import com.aggora.simulator.config.AggoraConfig.FeedProvider;
import com.fasterxml.jackson.databind.JsonNode;

import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Alpha Vantage: la fuente secundaria, solo para lo que Twelve Data no da gratis (Euronext Paris
 * y Shanghai).
 *
 * <p>Cuando se agota la cuota, Alpha Vantage no devuelve un error HTTP: devuelve 200 con
 * {@code {"Information": "..."}}. Hay que mirar el cuerpo, no solo el codigo de estado.
 */
@ApplicationScoped
public class AlphaVantageClient implements ReferenceSource {

    private static final Logger log = LoggerFactory.getLogger(AlphaVantageClient.class);

    /** Clave del precio dentro de la respuesta de GLOBAL_QUOTE. */
    private static final String QUOTE_PRICE_FIELD = "05. price";

    /** El plan gratis admite 1 peticion/segundo: dejamos margen. */
    private static final long SPACING_MS = 1200;

    private final AggoraConfig props;
    private final AlphaVantageApi http;

    @Inject
    public AlphaVantageClient(AggoraConfig props, @RestClient AlphaVantageApi http) {
        this.props = props;
        this.http = http;
    }

    @Override
    public FeedProvider provider() {
        return FeedProvider.ALPHA_VANTAGE;
    }

    @Override
    public boolean configured() {
        // Una key vacia (variable de entorno sin definir) llega como Optional vacio.
        return props.alphaVantage().apiKey().filter(key -> !key.isBlank()).isPresent();
    }

    @Override
    public int dailyBudget() {
        return props.alphaVantage().dailyRequestBudget();
    }

    @Override
    public Map<String, Double> fetchPrices(List<AggoraConfig.Instrument> instruments) {
        Map<String, Double> prices = new LinkedHashMap<>();
        for (int i = 0; i < instruments.size(); i++) {
            AggoraConfig.Instrument instrument = instruments.get(i);
            fetchOne(instrument).ifPresent(price -> prices.put(instrument.symbol(), price));
            if (i < instruments.size() - 1) {
                spaceOutRequests();
            }
        }
        return prices;
    }

    private Optional<Double> fetchOne(AggoraConfig.Instrument instrument) {
        try {
            JsonNode body = http.query("GLOBAL_QUOTE", instrument.feedSymbol(), props.alphaVantage().apiKey().orElse(""));
            return parseQuote(body, instrument.feedSymbol());
        } catch (RuntimeException ex) {
            log.warn("[alpha-vantage] peticion fallida para {}: {}", instrument.feedSymbol(), ex.getMessage());
            return Optional.empty();
        }
    }

    /**
     * Traduce la respuesta de GLOBAL_QUOTE a un precio.
     *
     * <p>Paquete-privado y sin HTTP a proposito: aqui esta el caso que muerde (la cuota agotada
     * llega como HTTP 200 con un campo {@code Information}) y se prueba con test.
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
