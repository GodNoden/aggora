package com.aggora.simulator.config;

import java.util.List;

import com.aggora.simulator.domain.Exchange;
import com.aggora.simulator.domain.TickEvent.AssetClass;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Toda la configuración de Aggora en un solo sitio (application.yml -> aggora.*).
 *
 * Las propiedades que solo alimentan placeholders de @Scheduled (tick-interval-ms,
 * poll-interval-ms, initial-delay-ms) no aparecen aquí a propósito: las lee Spring
 * del Environment directamente.
 */
@ConfigurationProperties(prefix = "aggora")
public record AggoraProperties(
        Topics topics,
        Simulation simulation,
        TwelveData twelveData,
        AlphaVantage alphaVantage,
        List<Instrument> instruments) {

    public record Topics(String ticksRaw) {
    }

    /** Parámetros del modelo estocástico (ver PriceWalk). */
    public record Simulation(
            double volatilityPerTick,
            double meanReversion,
            double maxDeviation,
            int minSize,
            int maxSize) {
    }

    /**
     * Twelve Data: en su plan gratuito cubre acciones de EEUU, forex, oro spot y
     * ETFs de EEUU. Un crédito = un símbolo pedido (no una petición).
     */
    public record TwelveData(String baseUrl, String apiKey, int dailyCreditBudget) {
    }

    /**
     * Alpha Vantage: se usa SOLO para lo que Twelve Data no da gratis (Euronext y
     * Shanghai). Su plan gratuito son 25 peticiones/día y 1 petición/segundo, así
     * que se consulta cada 3 horas; la interpolación sintética rellena el hueco.
     */
    public record AlphaVantage(String baseUrl, String apiKey, int dailyRequestBudget) {
    }

    /** De dónde sale el precio de referencia de un instrumento. */
    public enum FeedProvider { NONE, TWELVE_DATA, ALPHA_VANTAGE }

    /**
     * Un instrumento operable. El symbol es también el key del mensaje Kafka.
     *
     * @param feed         proveedor del precio real, o NONE si va solo con semilla.
     * @param feedSymbol   símbolo tal y como lo espera ESE proveedor (Alpha Vantage
     *                     usa sufijos de mercado: MC.PAR, 600519.SHH).
     * @param feedExchange exchange que espera Twelve Data (null = sin filtro);
     *                     Alpha Vantage lo ignora porque va dentro del símbolo.
     */
    public record Instrument(
            String symbol,
            AssetClass assetClass,
            Exchange exchange,
            String currency,
            double seedPrice,
            FeedProvider feed,
            String feedSymbol,
            String feedExchange) {

        public boolean hasRealFeed() {
            return feed != FeedProvider.NONE;
        }
    }
}
