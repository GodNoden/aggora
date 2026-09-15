package com.aggora.simulator.reference;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import com.aggora.simulator.config.AggoraConfig;
import com.aggora.simulator.config.AggoraConfig.FeedProvider;
import com.fasterxml.jackson.databind.JsonNode;

import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Twelve Data: la fuente principal (acciones de EEUU, forex, oro spot y ETFs).
 *
 * <p>Se agrupa por exchange y se hace una peticion por grupo, porque el endpoint admite varios
 * simbolos separados por coma. Quien decide CUANDO llamar es {@link ReferenceFeed}, con
 * presupuesto: cada simbolo consume un credito de la cuota diaria, no cada peticion.
 *
 * <p><b>Jackson:</b> el parser es el de Jackson 2 ({@code com.fasterxml.jackson.databind}), que es
 * el que trae Quarkus, mientras que la version Spring usa Jackson 3 ({@code tools.jackson}) porque
 * es lo que trae Boot 4. La logica de traduccion es la misma, pero los nombres cambian
 * ({@code asText()} en vez de {@code asString()}, {@code fields()} en vez de {@code properties()}).
 * Es exactamente la misma friccion que dio la migracion a Boot 4, en la direccion contraria.
 */
@ApplicationScoped
public class TwelveDataClient implements ReferenceSource {

    private static final Logger log = LoggerFactory.getLogger(TwelveDataClient.class);

    private final AggoraConfig props;
    private final TwelveDataApi http;

    @Inject
    public TwelveDataClient(AggoraConfig props, @RestClient TwelveDataApi http) {
        this.props = props;
        this.http = http;
    }

    @Override
    public FeedProvider provider() {
        return FeedProvider.TWELVE_DATA;
    }

    @Override
    public boolean configured() {
        // Una key vacia (variable de entorno sin definir) llega como Optional vacio.
        return props.twelveData().apiKey().filter(key -> !key.isBlank()).isPresent();
    }

    @Override
    public int dailyBudget() {
        return props.twelveData().dailyCreditBudget();
    }

    @Override
    public Map<String, Double> fetchPrices(List<AggoraConfig.Instrument> instruments) {
        Map<String, List<AggoraConfig.Instrument>> byExchange = new LinkedHashMap<>();
        for (AggoraConfig.Instrument instrument : instruments) {
            String exchange = instrument.feedExchange().orElse("");
            byExchange.computeIfAbsent(exchange, key -> new ArrayList<>()).add(instrument);
        }

        Map<String, Double> prices = new LinkedHashMap<>();
        byExchange.forEach((exchange, group) -> fetchGroup(exchange, group, prices));
        return prices;
    }

    private void fetchGroup(String exchange, List<AggoraConfig.Instrument> group, Map<String, Double> prices) {
        String symbols = String.join(",", group.stream().map(AggoraConfig.Instrument::feedSymbol).toList());
        try {
            // El exchange en blanco se manda como null para que el cliente no lo incluya en la URL
            // (en Spring se omitia condicionalmente construyendo la peticion; aqui se consigue
            // pasando null, que el cliente REST no serializa).
            JsonNode body = http.price(symbols, props.twelveData().apiKey().orElse(""),
                    exchange.isBlank() ? null : exchange);
            parsePriceBody(body, group).forEach(prices::put);
        } catch (RuntimeException ex) {
            log.warn("[twelve-data] peticion fallida ({}): {}",
                    exchange.isBlank() ? "sin exchange" : exchange, ex.getMessage());
        }
    }

    /**
     * Traduce la respuesta de {@code /price} a simbolo interno -> precio.
     *
     * <p>Paquete-privado y sin HTTP a proposito: aqui estan los dos formatos de respuesta que ya
     * costaron un bug real, y se prueban con test.
     */
    static Map<String, Double> parsePriceBody(JsonNode body, List<AggoraConfig.Instrument> group) {
        Map<String, Double> prices = new LinkedHashMap<>();
        if (body == null) {
            return prices;
        }
        // Error global: {"code":401,"message":"...","status":"error"}
        if (body.hasNonNull("status") && "error".equals(body.get("status").asText())) {
            log.warn("[twelve-data] error {}: {}", body.path("code").asText(), body.path("message").asText());
            return prices;
        }

        // /price responde en DOS formatos segun cuantos simbolos pidas:
        //   un simbolo -> {"price":"333.47"}
        //   varios     -> {"AAPL":{"price":"..."},"JPM":{"price":"..."}}
        // Confundirlos hace que "price" parezca un simbolo: de ahi el caso especial.
        JsonNode price = body.get("price");
        if (price != null && price.isValueNode() && group.size() == 1) {
            prices.put(group.get(0).symbol(), price.asDouble());
            return prices;
        }

        Map<String, String> symbolByFeedSymbol = new LinkedHashMap<>();
        group.forEach(instrument -> symbolByFeedSymbol.put(instrument.feedSymbol(), instrument.symbol()));

        Iterator<Entry<String, JsonNode>> campos = body.fields();
        while (campos.hasNext()) {
            Entry<String, JsonNode> entry = campos.next();
            if ("meta".equals(entry.getKey())) {
                continue; // en los lotes con error, "meta" no es un simbolo
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
