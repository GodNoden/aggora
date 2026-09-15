package com.aggora.simulator.reference;

import java.util.Optional;


import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Alpha Vantage no usa códigos HTTP para decir "te has pasado de cuota": responde
 * 200 con un campo "Information". Si no se mira el cuerpo, un mensaje de cuota
 * agotada se colaría como si fuera un precio. Este test lo fija.
 */
class AlphaVantageClientTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static JsonNode json(String raw) {
        return MAPPER.readTree(raw);
    }

    @Test
    @DisplayName("Respuesta normal: el precio está en Global Quote -> \"05. price\"")
    void lee_el_precio_del_global_quote() throws Exception {
        Optional<Double> price = AlphaVantageClient.parseQuote(json("""
                {"Global Quote": {
                    "01. symbol": "MC.PAR",
                    "04. low": "407.8500",
                    "05. price": "417.4500",
                    "07. latest trading day": "2026-09-14"
                }}"""), "MC.PAR");

        assertThat(price).contains(417.45);
    }

    @Test
    @DisplayName("Cuota diaria agotada: viene en \"Information\", no es un precio")
    void cuota_agotada_no_es_un_precio() throws Exception {
        Optional<Double> price = AlphaVantageClient.parseQuote(json("""
                {"Information": "Thank you for using Alpha Vantage! ... 25 requests per day ..."}"""),
                "MC.PAR");

        assertThat(price).isEmpty();
    }

    @Test
    @DisplayName("Demasiadas peticiones por segundo: viene en \"Note\"")
    void limite_por_segundo_no_es_un_precio() throws Exception {
        Optional<Double> price = AlphaVantageClient.parseQuote(
                json("{\"Note\": \"Thank you for using Alpha Vantage! Please consider spreading out your free API requests.\"}"),
                "MC.PAR");

        assertThat(price).isEmpty();
    }

    @Test
    @DisplayName("Símbolo desconocido: \"Error Message\"")
    void error_message_no_es_un_precio() throws Exception {
        Optional<Double> price = AlphaVantageClient.parseQuote(
                json("{\"Error Message\": \"Invalid API call. Please retry or visit the documentation.\"}"),
                "XXX.YYY");

        assertThat(price).isEmpty();
    }

    @Test
    @DisplayName("Respuesta sin Global Quote: no revienta, devuelve vacío")
    void respuesta_vacia() throws Exception {
        assertThat(AlphaVantageClient.parseQuote(json("{}"), "MC.PAR")).isEmpty();
        assertThat(AlphaVantageClient.parseQuote(null, "MC.PAR")).isEmpty();
    }
}
