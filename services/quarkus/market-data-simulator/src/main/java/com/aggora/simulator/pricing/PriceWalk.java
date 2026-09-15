package com.aggora.simulator.pricing;

import java.util.Random;

/**
 * Paseo aleatorio con reversión a la media y banda dura.
 *
 * En cada paso el precio recibe un shock gaussiano proporcional a su tamaño y, a la
 * vez, es tirado hacia la referencia real (mean reversion). El resultado se recorta
 * dentro de [referencia*(1-d), referencia*(1+d)], de modo que el tick sintético
 * nunca se despega del dato real que le sirve de ancla (spec 3.3.3).
 *
 * Es la pieza con lógica de verdad del simulador, por eso tiene test.
 */
public final class PriceWalk {

    private final double volatilityPerTick;
    private final double meanReversion;
    private final double maxDeviation;
    private final Random random;
    private double price;

    public PriceWalk(double initialPrice,
                     double volatilityPerTick,
                     double meanReversion,
                     double maxDeviation,
                     Random random) {
        this.price = initialPrice;
        this.volatilityPerTick = volatilityPerTick;
        this.meanReversion = meanReversion;
        this.maxDeviation = maxDeviation;
        this.random = random;
    }

    public synchronized double price() {
        return price;
    }

    /** Salta a un precio nuevo (por ejemplo al llegar una referencia real). */
    public synchronized void reset(double newPrice) {
        this.price = newPrice;
    }

    /**
     * Avanza un tick y devuelve el precio nuevo, siempre dentro de la banda.
     *
     * synchronized porque lo tocan dos schedulers: el que emite ticks sintéticos y
     * el que aplica referencias reales (spring.task.scheduling.pool.size=2). La
     * contención es de un lock por símbolo y dos hilos como mucho.
     */
    public synchronized double next(double reference) {
        double pull = (reference - price) * meanReversion;
        double shock = random.nextGaussian() * price * volatilityPerTick;
        double next = price + pull + shock;
        double low = reference * (1 - maxDeviation);
        double high = reference * (1 + maxDeviation);
        price = Math.max(low, Math.min(high, next));
        return price;
    }
}
