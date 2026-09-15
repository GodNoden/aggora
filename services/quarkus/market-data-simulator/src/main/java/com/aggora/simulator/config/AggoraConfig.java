package com.aggora.simulator.config;

import java.util.List;
import java.util.Optional;

import com.aggora.avro.AssetClass;
import com.aggora.simulator.domain.Exchange;

import io.smallrye.config.ConfigMapping;

/**
 * Toda la configuracion de Aggora en un solo sitio: el bloque {@code aggora:} del
 * {@code application.yaml}, que es el MISMO fichero que usa la version Spring.
 *
 * <p>Es el equivalente del {@code @ConfigurationProperties} de Spring, con una diferencia de
 * fondo que se nota al escribir codigo: aqui es una <b>interfaz</b> con metodos de acceso y la
 * implementacion la genera Quarkus en el build, no un record que Spring rellena en el arranque.
 * Los nombres van en camelCase y el YAML en kebab-case: la conversion la hace SmallRye Config.
 *
 * <p>Los tipos anidados son interfaces por el mismo motivo. {@code hasRealFeed()} es estatico a
 * proposito: un metodo de instancia con cuerpo en una interfaz de configuracion lo tomaria el
 * mapeador por una propiedad mas.
 */
@ConfigMapping(prefix = "aggora")
public interface AggoraConfig {

    Topics topics();

    int invalidTickEveryN();

    /**
     * Cada cuanto se emite un tick por instrumento. Lo lee el motor para calcular cuantos ticks
     * emitir por vuelta: el scheduler de Quarkus no baja de un segundo (ver TickEngine).
     */
    int tickIntervalMs();

    Simulation simulation();

    TwelveData twelveData();

    AlphaVantage alphaVantage();

    List<Instrument> instruments();

    /** ¿Este instrumento tiene proveedor de precio real, o va solo con el precio semilla? */
    static boolean hasRealFeed(Instrument instrument) {
        return instrument.feed() != FeedProvider.NONE;
    }

    interface Topics {
        String ticksRaw();

        String ordersIncoming();
    }

    /** Parametros del modelo estocastico (ver PriceWalk). */
    interface Simulation {
        double volatilityPerTick();

        double meanReversion();

        double maxDeviation();

        int minSize();

        int maxSize();
    }

    /**
     * Twelve Data: en su plan gratuito cubre acciones de EEUU, forex, oro spot y ETFs de EEUU.
     * Un credito = un simbolo pedido (no una peticion).
     */
    interface TwelveData {
        String baseUrl();

        /**
         * Sin key configurada, esta fuente se salta y los ticks van con precio semilla.
         *
         * <p>Es {@code Optional} y no {@code String} por una diferencia real con Spring: alli
         * {@code ${TWELVEDATA_API_KEY:}} deja una cadena vacia y el codigo comprueba
         * {@code isBlank()}; aqui SmallRye considera que una propiedad vacia <b>no esta</b>, y si
         * el miembro es un {@code String} obligatorio el arranque falla con
         * "defined as the empty String which the Converter considered to be null".
         */
        Optional<String> apiKey();

        int dailyCreditBudget();
    }

    /**
     * Alpha Vantage: solo para lo que Twelve Data no da gratis (Euronext y Shanghai). Su plan
     * gratuito son 25 peticiones/dia y 1 peticion/segundo, asi que se consulta cada 3 horas.
     */
    interface AlphaVantage {
        String baseUrl();

        /** Igual que en Twelve Data: sin key, esta fuente no se consulta. */
        Optional<String> apiKey();

        int dailyRequestBudget();
    }

    /** De donde sale el precio de referencia de un instrumento. */
    enum FeedProvider { NONE, TWELVE_DATA, ALPHA_VANTAGE }

    /**
     * Un instrumento operable. El symbol es tambien el key del mensaje Kafka.
     *
     * @param feed         proveedor del precio real, o NONE si va solo con semilla
     * @param feedSymbol   simbolo tal y como lo espera ESE proveedor (Alpha Vantage lleva el
     *                     mercado dentro: MC.PAR, 600519.SHH)
     * @param feedExchange exchange que espera Twelve Data (null = sin filtro); Alpha Vantage lo
     *                     ignora porque va dentro del simbolo
     */
    interface Instrument {
        String symbol();

        AssetClass assetClass();

        Exchange exchange();

        String currency();

        double seedPrice();

        FeedProvider feed();

        String feedSymbol();

        /**
         * Exchange que espera Twelve Data. Puede faltar (los instrumentos de Alpha Vantage no lo
         * llevan), y en Quarkus "puede faltar" se dice con {@code Optional}: un valor vacio se
         * considera ausente, y un {@code String} obligatorio con valor vacio no arranca
         * ("defined as the empty String which the Converter considered to be null"). En Spring
         * bastaba con que el record admitiera el campo ausente y quedara a null.
         */
        Optional<String> feedExchange();
    }
}
