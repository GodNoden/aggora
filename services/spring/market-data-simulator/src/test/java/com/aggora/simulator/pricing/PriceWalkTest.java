package com.aggora.simulator.pricing;

import java.util.Random;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * El sentido del paseo es que el tick sintético NO se despegue del precio real:
 * si esto falla, el dato simulado deja de ser creíble.
 */
class PriceWalkTest {

    @Test
    @DisplayName("100.000 ticks sin salirse de la banda de la referencia")
    void se_mantiene_dentro_de_la_banda() {
        double reference = 187.45;
        double maxDeviation = 0.015;
        PriceWalk walk = new PriceWalk(reference, 0.0006, 0.02, maxDeviation, new Random(42));

        for (int i = 0; i < 100_000; i++) {
            double price = walk.next(reference);
            assertThat(price)
                    .as("tick %d", i)
                    .isBetween(reference * (1 - maxDeviation), reference * (1 + maxDeviation));
        }
    }

    @Test
    @DisplayName("Si la referencia real salta un 10%, el siguiente tick ya está dentro de la banda nueva")
    void se_reajusta_cuando_cambia_la_referencia() {
        PriceWalk walk = new PriceWalk(100.0, 0.0006, 0.05, 0.01, new Random(7));

        double price = walk.next(90.0);

        assertThat(price).isBetween(90.0 * 0.99, 90.0 * 1.01);
    }

    @Test
    @DisplayName("La media de muchos ticks con referencia fija se queda cerca de la referencia")
    void no_se_va_a_la_deriva() {
        double reference = 50.0;
        PriceWalk walk = new PriceWalk(reference, 0.001, 0.02, 0.02, new Random(2024));

        double sum = 0;
        int steps = 50_000;
        for (int i = 0; i < steps; i++) {
            sum += walk.next(reference);
        }

        assertThat(sum / steps).isCloseTo(reference, org.assertj.core.data.Offset.offset(reference * 0.005));
    }
}
