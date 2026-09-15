package com.aggora.simulator.reference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Alpha Vantage no usa codigos HTTP para decir "te has pasado de cuota": responde 200 con un campo
 * {@code Information}. Si no se mira el cuerpo, un mensaje de cuota agotada se colaria como si
 * fuera un precio. Este test lo fija, igual que en la version Spring.
 */
class AlphaVantageClientTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static JsonNode json(String raw) throws Exception {
        return MAPPER.readTree(raw);
    }

    @Test
    @DisplayName("Respuesta normal: el precio esta en Global Quote -> \"05. price\"")
    void lee_el_precio_del_global_quote() throws Exception {
        Optional<Double> price = AlphaVantageClient.parseQuote(json("""
                {"Global Quote": {
                    "01. symbol": "MC.PAR",
                    "04. low": "407.8500",
                    "05. price": "417.4500",
                    "07. latest trading day": "2026-09-14"
                }}"""), "MC.PAR");

        assertEquals(Optional.of(417.45), price);
    }

    @Test
    @DisplayName("Cuota diaria agotada: viene en \"Information\", no es un precio")
    void cuota_agotada_no_es_un_precio() throws Exception {
        Optional<Double> price = AlphaVantageClient.parseQuote(json("""
                {"Information": "Thank you for using Alpha Vantage! ... 25 requests per day ..."}"""),
                "MC.PAR");

        assertTrue(price.isEmpty());
    }

    @Test
    @DisplayName("Demasiadas peticiones por segundo: viene en \"Note\"")
    void limite_por_segundo_no_es_un_precio() throws Exception {
        Optional<Double> price = AlphaVantageClient.parseQuote(
                json("{\"Note\": \"Thank you for using Alpha Vantage! Please consider spreading out your free API requests.\"}"),
                "MC.PAR");

        assertTrue(price.isEmpty());
    }

    @Test
    @DisplayName("Simbolo desconocido: \"Error Message\"")
    void error_message_no_es_un_precio() throws Exception {
        Optional<Double> price = AlphaVantageClient.parseQuote(
                json("{\"Error Message\": \"Invalid API call. Please retry or visit the documentation.\"}"),
                "XXX.YYY");

        assertTrue(price.isEmpty());
    }

    @Test
    @DisplayName("Respuesta sin Global Quote: no revienta, devuelve vacio")
    void respuesta_vacia() throws Exception {
        assertTrue(AlphaVantageClient.parseQuote(json("{}"), "MC.PAR").isEmpty());
        assertTrue(AlphaVantageClient.parseQuote(null, "MC.PAR").isEmpty());
    }
}
