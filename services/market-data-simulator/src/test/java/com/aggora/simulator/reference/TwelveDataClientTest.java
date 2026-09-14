package com.aggora.simulator.reference;

import java.util.List;
import java.util.Map;

import com.aggora.simulator.config.AggoraProperties;
import com.aggora.simulator.config.AggoraProperties.FeedProvider;
import com.aggora.simulator.domain.Exchange;
import com.aggora.simulator.domain.TickEvent.AssetClass;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * La traducción de la respuesta de /price tiene dos formatos distintos, y
 * confundirlos ya nos costó un bug real (AAPL se perdía porque su grupo tenía un
 * solo símbolo y la respuesta venía sin envolver). Este test lo fija.
 */
class TwelveDataClientTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static AggoraProperties.Instrument instrument(String symbol, String feedSymbol) {
        return new AggoraProperties.Instrument(symbol, AssetClass.EQUITY, Exchange.NASDAQ, "USD",
                1.0, FeedProvider.TWELVE_DATA, feedSymbol, "NASDAQ");
    }

    private static JsonNode json(String raw) throws JsonProcessingException {
        return MAPPER.readTree(raw);
    }

    @Test
    @DisplayName("Un solo símbolo: la respuesta viene SIN envolver, como {\"price\":\"...\"}")
    void formato_de_un_solo_simbolo() throws Exception {
        Map<String, Double> prices = TwelveDataClient.parsePriceBody(
                json("{\"price\":\"333.47000\"}"),
                List.of(instrument("AAPL", "AAPL")));

        assertThat(prices).containsExactly(Map.entry("AAPL", 333.47));
    }

    @Test
    @DisplayName("Varios símbolos: la respuesta viene indexada por símbolo")
    void formato_por_lotes() throws Exception {
        Map<String, Double> prices = TwelveDataClient.parsePriceBody(
                json("{\"AAPL\":{\"price\":\"333.47000\"},\"JPM\":{\"price\":\"285.40000\"}}"),
                List.of(instrument("AAPL", "AAPL"), instrument("JPM", "JPM")));

        assertThat(prices).containsOnly(
                Map.entry("AAPL", 333.47),
                Map.entry("JPM", 285.40));
    }

    @Test
    @DisplayName("El símbolo del proveedor se traduce al símbolo interno de Aggora")
    void traduce_el_simbolo_del_proveedor() throws Exception {
        Map<String, Double> prices = TwelveDataClient.parsePriceBody(
                json("{\"AAPL\":{\"price\":\"333.47000\"}}"),
                List.of(instrument("ACCION-AAPL", "AAPL")));

        assertThat(prices).containsExactly(Map.entry("ACCION-AAPL", 333.47));
    }

    @Test
    @DisplayName("Error global (cuota, key inválida): no devuelve precios")
    void error_global_no_devuelve_precios() throws Exception {
        Map<String, Double> prices = TwelveDataClient.parsePriceBody(
                json("{\"code\":429,\"message\":\"You have run out of API credits for the current minute.\",\"status\":\"error\"}"),
                List.of(instrument("AAPL", "AAPL")));

        assertThat(prices).isEmpty();
    }

    @Test
    @DisplayName("En un lote con error, la clave \"meta\" no se confunde con un símbolo")
    void ignora_la_clave_meta() throws Exception {
        Map<String, Double> prices = TwelveDataClient.parsePriceBody(
                json("{\"AAPL\":{\"price\":\"333.47000\"},\"meta\":{\"symbol\":\"AAPL,JPM\",\"exchange\":\"NASDAQ\"}}"),
                List.of(instrument("AAPL", "AAPL"), instrument("JPM", "JPM")));

        assertThat(prices).containsExactly(Map.entry("AAPL", 333.47));
    }
}
