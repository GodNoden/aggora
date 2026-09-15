package com.aggora.simulator.domain;

import java.time.Instant;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * El calendario de mercado es fácil de romper sin darse cuenta (zonas horarias,
 * horario de verano, descanso de mediodía), así que tiene test.
 */
class ExchangeTest {

    // Miércoles 15 de enero de 2025, horario de invierno en Europa/EEUU.
    private static final Instant WEDNESDAY = Instant.parse("2025-01-15T12:00:00Z");
    // Sábado 18 de enero de 2025.
    private static final Instant SATURDAY = Instant.parse("2025-01-18T12:00:00Z");

    @Test
    @DisplayName("NYSE abre a las 09:30 ET: a las 14:30Z está abierto y a las 13:00Z cerrado")
    void nyse_respeta_horario_de_nueva_york() {
        assertThat(Exchange.NYSE.isOpen(Instant.parse("2025-01-15T14:30:00Z"))).isTrue();
        assertThat(Exchange.NYSE.isOpen(Instant.parse("2025-01-15T13:00:00Z"))).isFalse();
        // 16:00 ET = 21:00Z -> el cierre no incluye el instante exacto de cierre
        assertThat(Exchange.NYSE.isOpen(Instant.parse("2025-01-15T21:00:00Z"))).isFalse();
    }

    @Test
    @DisplayName("Euronext Paris usa CET en invierno (09:00 = 08:00Z) y CEST en verano (07:00Z)")
    void euronext_respeta_horario_de_paris_y_el_cambio_de_hora() {
        assertThat(Exchange.EURONEXT.isOpen(Instant.parse("2025-01-15T08:00:00Z"))).isTrue();
        assertThat(Exchange.EURONEXT.isOpen(Instant.parse("2025-01-15T07:00:00Z"))).isFalse();
        // En julio París va dos horas por delante de UTC.
        assertThat(Exchange.EURONEXT.isOpen(Instant.parse("2025-07-15T07:00:00Z"))).isTrue();
    }

    @Test
    @DisplayName("Shanghai cierra al mediodía: 12:00 CST está cerrado, 12:00Z (que son las 20:00 CST) también")
    void sse_tiene_descanso_de_mediodia() {
        // 01:30-03:30Z = 09:30-11:30 CST (mañana), 05:00-07:00Z = 13:00-15:00 CST (tarde)
        assertThat(Exchange.SSE.isOpen(Instant.parse("2025-01-15T02:00:00Z"))).isTrue();
        assertThat(Exchange.SSE.isOpen(Instant.parse("2025-01-15T04:00:00Z"))).isFalse();
        assertThat(Exchange.SSE.isOpen(Instant.parse("2025-01-15T06:00:00Z"))).isTrue();
        assertThat(Exchange.SSE.isOpen(Instant.parse("2025-01-15T08:00:00Z"))).isFalse();
    }

    @Test
    @DisplayName("Fin de semana no hay mercado, ni siquiera en FX 24x5")
    void fin_de_semana_cerrado_en_todos_los_mercados() {
        for (Exchange exchange : Exchange.values()) {
            assertThat(exchange.isOpen(SATURDAY))
                    .as("mercado %s en sábado", exchange)
                    .isFalse();
        }
        assertThat(Exchange.FX.isOpen(WEDNESDAY)).isTrue();
        assertThat(Exchange.COMMODITY.isOpen(WEDNESDAY)).isTrue();
    }
}
