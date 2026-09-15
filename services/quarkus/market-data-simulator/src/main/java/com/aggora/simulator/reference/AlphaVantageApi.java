package com.aggora.simulator.reference;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.QueryParam;

import com.fasterxml.jackson.databind.JsonNode;

import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

/**
 * La llamada HTTP a Alpha Vantage, descrita como interfaz.
 *
 * <p>Dos diferencias con Twelve Data que estan en la configuracion, no aqui: no hay endpoint por
 * lotes (es UNA peticion por simbolo, y el plan gratuito admite 1 por segundo, asi que las
 * llamadas van espaciadas), y el simbolo lleva el mercado dentro ({@code MC.PAR},
 * {@code 600519.SHH}), que es lo que dice {@code feed-symbol} en el yaml.
 *
 * <p>Cuando se agota la cuota, Alpha Vantage <b>no</b> devuelve un error HTTP: devuelve 200 con
 * un campo {@code Information}. Hay que mirar el cuerpo, y eso se hace en
 * {@link AlphaVantageClient#parseQuote}.
 */
@RegisterRestClient(configKey = "alpha-vantage")
public interface AlphaVantageApi {

    @GET
    @Path("/query")
    JsonNode query(@QueryParam("function") String function,
                   @QueryParam("symbol") String symbol,
                   @QueryParam("apikey") String apiKey);
}
