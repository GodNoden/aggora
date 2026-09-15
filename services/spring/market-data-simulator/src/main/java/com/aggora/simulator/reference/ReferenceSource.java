package com.aggora.simulator.reference;

import java.util.List;
import java.util.Map;

import com.aggora.simulator.config.AggoraProperties;

/**
 * Una fuente de precios reales (de referencia).
 *
 * Hay dos implementaciones porque hacen falta dos proveedores: Twelve Data cubre
 * EEUU, forex y oro en su plan gratuito, y Alpha Vantage cubre Euronext y Shanghai.
 * Cada una tiene su propia cuota, su propio formato de respuesta y su propio ritmo
 * de consulta, y por eso el feed trata con la interfaz y no con cada cliente.
 */
public interface ReferenceSource {

    AggoraProperties.FeedProvider provider();

    /** ¿Hay credenciales configuradas? Si no, esta fuente se salta sin ruido. */
    boolean configured();

    /** Cuota diaria que NO queremos agotar (créditos/peticiones según el proveedor). */
    int dailyBudget();

    /**
     * Precios reales de los instrumentos indicados, indexados por el símbolo
     * interno de Aggora. Devuelve solo los que llegaron bien: un símbolo que falle
     * se registra y se omite, nunca tumba al resto.
     */
    Map<String, Double> fetchPrices(List<AggoraProperties.Instrument> instruments);
}
