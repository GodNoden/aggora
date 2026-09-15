package com.aggora.simulator.reference;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.QueryParam;

import com.fasterxml.jackson.databind.JsonNode;

import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

/**
 * La llamada HTTP a Twelve Data, descrita como interfaz.
 *
 * <p>Aqui esta la diferencia mas grande del port con la version Spring: alli el cliente se
 * construye a mano con un {@code RestClient} imperativo (URL, query params y lectura del cuerpo,
 * todo en el metodo), y aqui se <b>describe</b> la llamada y la implementacion la genera Quarkus.
 * Menos codigo y menos sitios donde equivocarse con un parametro; a cambio se pierde el control
 * fino del builder (no se puede montar la URL a trozos ni condicionar parametros desde el
 * propio metodo, y por eso el exchange vacio se manda como cadena vacia en lugar de omitirse).
 *
 * <p>Un solo endpoint {@code /price} admite varios simbolos separados por coma, asi que se agrupa
 * por exchange y se hace UNA peticion por exchange: el exchange es parametro de la peticion, no
 * del simbolo. Ojo con el coste: cada SIMBOLO consume un credito, no cada peticion.
 */
@RegisterRestClient(configKey = "twelve-data")
public interface TwelveDataApi {

    @GET
    @Path("/price")
    JsonNode price(@QueryParam("symbol") String symbols,
                   @QueryParam("apikey") String apiKey,
                   @QueryParam("exchange") String exchange);
}
