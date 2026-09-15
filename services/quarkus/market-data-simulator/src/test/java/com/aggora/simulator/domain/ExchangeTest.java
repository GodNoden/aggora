package com.aggora.simulator.domain;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * El calendario de mercado es facil de romper sin darse cuenta (zonas horarias, horario de
 * verano, descanso de mediodia), asi que tiene test. Es el mismo que el de la version Spring,
 * palabra por palabra: la logica de dominio se copia, lo que cambia es el framework.
 */
class ExchangeTest {

    // Miercoles 15 de enero de 2025, horario de invierno en Europa/EEUU.
    private static final Instant WEDNESDAY = Instant.parse("2025-01-15T12:00:00Z");
    // Sabado 18 de enero de 2025.
    private static final Instant SATURDAY = Instant.parse("2025-01-18T12:00:00Z");

    @Test
    @DisplayName("NYSE abre a las 09:30 ET: a las 14:30Z esta abierto y a las 13:00Z cerrado")
    void nyse_respeta_horario_de_nueva_york() {
        assertTrue(Exchange.NYSE.isOpen(Instant.parse("2025-01-15T14:30:00Z")));
        assertFalse(Exchange.NYSE.isOpen(Instant.parse("2025-01-15T13:00:00Z")));
        // 16:00 ET = 21:00Z -> el cierre no incluye el instante exacto de cierre
        assertFalse(Exchange.NYSE.isOpen(Instant.parse("2025-01-15T21:00:00Z")));
    }

    @Test
    @DisplayName("Euronext Paris usa CET en invierno (09:00 = 08:00Z) y CEST en verano (07:00Z)")
    void euronext_respeta_horario_de_paris_y_el_cambio_de_hora() {
        assertTrue(Exchange.EURONEXT.isOpen(Instant.parse("2025-01-15T08:00:00Z")));
        assertFalse(Exchange.EURONEXT.isOpen(Instant.parse("2025-01-15T07:00:00Z")));
        // En julio Paris va dos horas por delante de UTC.
        assertTrue(Exchange.EURONEXT.isOpen(Instant.parse("2025-07-15T07:00:00Z")));
    }

    @Test
    @DisplayName("Shanghai cierra al mediodia: 12:00 CST esta cerrado, 12:00Z (que son las 20:00 CST) tambien")
    void sse_tiene_descanso_de_mediodia() {
        // 01:30-03:30Z = 09:30-11:30 CST (manana), 05:00-07:00Z = 13:00-15:00 CST (tarde)
        assertTrue(Exchange.SSE.isOpen(Instant.parse("2025-01-15T02:00:00Z")));
        assertFalse(Exchange.SSE.isOpen(Instant.parse("2025-01-15T04:00:00Z")));
        assertTrue(Exchange.SSE.isOpen(Instant.parse("2025-01-15T06:00:00Z")));
        assertFalse(Exchange.SSE.isOpen(Instant.parse("2025-01-15T08:00:00Z")));
    }

    @Test
    @DisplayName("Fin de semana no hay mercado, ni siquiera en FX 24x5")
    void fin_de_semana_cerrado_en_todos_los_mercados() {
        for (Exchange exchange : Exchange.values()) {
            assertFalse(exchange.isOpen(SATURDAY), "mercado " + exchange + " en sabado");
        }
        assertTrue(Exchange.FX.isOpen(WEDNESDAY));
        assertTrue(Exchange.COMMODITY.isOpen(WEDNESDAY));
    }
}
