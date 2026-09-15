package com.aggora.simulator.reference;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.aggora.simulator.config.AggoraProperties;
import com.aggora.simulator.config.AggoraProperties.FeedProvider;
import tools.jackson.databind.JsonNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

/**
 * Twelve Data: la fuente principal (acciones de EEUU, forex, oro spot y ETFs).
 *
 * Un solo endpoint /price admite varios símbolos separados por coma, así que se
 * agrupa por exchange y se hace UNA petición por exchange: el exchange es parámetro
 * de la petición, no del símbolo.
 *
 * Cuidado con el coste: cada SÍMBOLO consume un crédito de la cuota diaria, no cada
 * petición. Por eso quien decide cuándo llamar es ReferenceFeed, con presupuesto.
 */
@Component
public class TwelveDataClient implements ReferenceSource {

    private static final Logger log = LoggerFactory.getLogger(TwelveDataClient.class);

    private final AggoraProperties props;
    private final RestClient http;

    public TwelveDataClient(AggoraProperties props) {
        this.props = props;
        this.http = RestClient.builder().baseUrl(props.twelveData().baseUrl()).build();
    }

    @Override
    public FeedProvider provider() {
        return FeedProvider.TWELVE_DATA;
    }

    @Override
    public boolean configured() {
        String apiKey = props.twelveData().apiKey();
        return apiKey != null && !apiKey.isBlank();
    }

    @Override
    public int dailyBudget() {
        return props.twelveData().dailyCreditBudget();
    }

    @Override
    public Map<String, Double> fetchPrices(List<AggoraProperties.Instrument> instruments) {
        Map<String, List<AggoraProperties.Instrument>> byExchange = new LinkedHashMap<>();
        for (AggoraProperties.Instrument instrument : instruments) {
            String exchange = instrument.feedExchange() == null ? "" : instrument.feedExchange();
            byExchange.computeIfAbsent(exchange, key -> new ArrayList<>()).add(instrument);
        }

        Map<String, Double> prices = new LinkedHashMap<>();
        byExchange.forEach((exchange, group) -> fetchGroup(exchange, group, prices));
        return prices;
    }

    private void fetchGroup(String exchange, List<AggoraProperties.Instrument> group, Map<String, Double> prices) {
        String symbols = String.join(",", group.stream().map(AggoraProperties.Instrument::feedSymbol).toList());
        try {
            JsonNode body = http.get()
                    .uri(builder -> {
                        var uri = builder.path("/price")
                                .queryParam("symbol", symbols)
                                .queryParam("apikey", props.twelveData().apiKey());
                        if (!exchange.isBlank()) {
                            uri.queryParam("exchange", exchange);
                        }
                        return uri.build();
                    })
                    .retrieve()
                    .body(JsonNode.class);
            parsePriceBody(body, group).forEach(prices::put);
        } catch (RuntimeException ex) {
            log.warn("[twelve-data] petición fallida ({}): {}",
                    exchange.isBlank() ? "sin exchange" : exchange, ex.getMessage());
        }
    }

    /**
     * Traduce la respuesta de /price a símbolo interno -> precio.
     *
     * Paquete-privado y sin HTTP a propósito: esta parte tiene casos raros (dos
     * formatos de respuesta distintos) y se prueba con un test.
     */
    static Map<String, Double> parsePriceBody(JsonNode body, List<AggoraProperties.Instrument> group) {
        Map<String, Double> prices = new LinkedHashMap<>();
        if (body == null) {
            return prices;
        }
        // Error global: {"code":401,"message":"...","status":"error"}
        if (body.hasNonNull("status") && "error".equals(body.get("status").asText())) {
            log.warn("[twelve-data] error {}: {}", body.path("code").asText(), body.path("message").asText());
            return prices;
        }

        // /price responde en DOS formatos según cuántos símbolos pidas:
        //   un símbolo -> {"price":"333.47"}
        //   varios     -> {"AAPL":{"price":"..."},"JPM":{"price":"..."}}
        // Confundirlos hace que "price" parezca un símbolo: de ahí el caso especial.
        JsonNode price = body.get("price");
        if (price != null && price.isValueNode() && group.size() == 1) {
            prices.put(group.get(0).symbol(), price.asDouble());
            return prices;
        }

        Map<String, String> symbolByFeedSymbol = new LinkedHashMap<>();
        group.forEach(instrument -> symbolByFeedSymbol.put(instrument.feedSymbol(), instrument.symbol()));

        for (Map.Entry<String, JsonNode> entry : body.properties()) {
            if ("meta".equals(entry.getKey())) {
                continue; // en los lotes con error, "meta" no es un símbolo
            }
            JsonNode node = entry.getValue();
            if (node.hasNonNull("price")) {
                prices.put(symbolByFeedSymbol.getOrDefault(entry.getKey(), entry.getKey()),
                        node.get("price").asDouble());
            } else {
                log.warn("[twelve-data] {} sin precio: {} {}",
                        entry.getKey(), node.path("code").asText(), node.path("message").asText());
            }
        }
        return prices;
    }
}
