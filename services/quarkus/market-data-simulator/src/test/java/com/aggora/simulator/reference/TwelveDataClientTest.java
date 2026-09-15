package com.aggora.simulator.reference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.aggora.avro.AssetClass;
import com.aggora.simulator.config.AggoraConfig;
import com.aggora.simulator.config.AggoraConfig.FeedProvider;
import com.aggora.simulator.domain.Exchange;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * La traduccion de la respuesta de {@code /price} tiene DOS formatos distintos, y confundirlos ya
 * costo un bug real (AAPL se perdia porque su grupo tenia un solo simbolo y la respuesta venia
 * sin envolver). Este test lo fija, igual que en la version Spring.
 *
 * <p>Nota del port: el parser es el de Jackson 2 ({@code com.fasterxml.jackson}), que es el que
 * trae Quarkus; el de Spring es Jackson 3. La logica es identica.
 */
class TwelveDataClientTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * Un instrumento de mentira para el test. En Spring es un record que se puede construir; aqui
     * {@code AggoraConfig.Instrument} es una interfaz (la rellena Quarkus desde el yaml), asi que
     * el test trae su propia implementacion: un record con los mismos accesores.
     */
    private record FakeInstrument(String symbol, AssetClass assetClass, Exchange exchange, String currency,
                                  double seedPrice, FeedProvider feed, String feedSymbol, Optional<String> feedExchange)
            implements AggoraConfig.Instrument {
    }

    private static AggoraConfig.Instrument instrument(String symbol, String feedSymbol) {
        return new FakeInstrument(symbol, AssetClass.EQUITY, Exchange.NASDAQ, "USD",
                1.0, FeedProvider.TWELVE_DATA, feedSymbol, Optional.of("NASDAQ"));
    }

    private static JsonNode json(String raw) throws Exception {
        return MAPPER.readTree(raw);
    }

    @Test
    @DisplayName("Un solo simbolo: la respuesta viene SIN envolver, como {\"price\":\"...\"}")
    void formato_de_un_solo_simbolo() throws Exception {
        Map<String, Double> prices = TwelveDataClient.parsePriceBody(
                json("{\"price\":\"333.47000\"}"),
                List.of(instrument("AAPL", "AAPL")));

        assertEquals(Map.of("AAPL", 333.47), prices);
    }

    @Test
    @DisplayName("Varios simbolos: la respuesta viene indexada por simbolo")
    void formato_por_lotes() throws Exception {
        Map<String, Double> prices = TwelveDataClient.parsePriceBody(
                json("{\"AAPL\":{\"price\":\"333.47000\"},\"JPM\":{\"price\":\"285.40000\"}}"),
                List.of(instrument("AAPL", "AAPL"), instrument("JPM", "JPM")));

        assertEquals(Map.of("AAPL", 333.47, "JPM", 285.40), prices);
    }

    @Test
    @DisplayName("El simbolo del proveedor se traduce al simbolo interno de Aggora")
    void traduce_el_simbolo_del_proveedor() throws Exception {
        Map<String, Double> prices = TwelveDataClient.parsePriceBody(
                json("{\"AAPL\":{\"price\":\"333.47000\"}}"),
                List.of(instrument("ACCION-AAPL", "AAPL")));

        assertEquals(Map.of("ACCION-AAPL", 333.47), prices);
    }

    @Test
    @DisplayName("Error global (cuota, key invalida): no devuelve precios")
    void error_global_no_devuelve_precios() throws Exception {
        Map<String, Double> prices = TwelveDataClient.parsePriceBody(
                json("{\"code\":429,\"message\":\"You have run out of API credits for the current minute.\",\"status\":\"error\"}"),
                List.of(instrument("AAPL", "AAPL")));

        assertTrue(prices.isEmpty());
    }

    @Test
    @DisplayName("En un lote con error, la clave \"meta\" no se confunde con un simbolo")
    void ignora_la_clave_meta() throws Exception {
        Map<String, Double> prices = TwelveDataClient.parsePriceBody(
                json("{\"AAPL\":{\"price\":\"333.47000\"},\"meta\":{\"symbol\":\"AAPL,JPM\",\"exchange\":\"NASDAQ\"}}"),
                List.of(instrument("AAPL", "AAPL"), instrument("JPM", "JPM")));

        assertEquals(Map.of("AAPL", 333.47), prices);
    }
}
