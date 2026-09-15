package com.aggora.normalizer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.time.Instant;

import com.aggora.avro.AssetClass;
import com.aggora.avro.Exchange;
import com.aggora.avro.Tick;
import com.aggora.avro.TickSource;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Las reglas de validacion, que son la unica logica no trivial del port.
 *
 * <p>Son las mismas reglas que las de la version Spring y se prueban una a una. Si algun dia
 * las dos implementaciones se separan, este test es el que lo dice.
 */
class TickValidatorTest {

    private final TickValidator validator = new TickValidator();

    @Test
    @DisplayName("Un tick correcto pasa la validacion")
    void un_tick_correcto_pasa() {
        assertNull(validator.validate("EUR/USD", tick("EUR/USD")));
    }

    @Test
    @DisplayName("Un payload nulo no se puede procesar")
    void payload_nulo() {
        assertEquals("payload nulo", validator.validate("EUR/USD", null));
    }

    @Test
    @DisplayName("Sin simbolo no hay nada que normalizar")
    void sin_simbolo() {
        Tick tick = tick("EUR/USD");
        tick.setSymbol("  ");
        assertEquals("symbol vacio", validator.validate("EUR/USD", tick));
    }

    @Test
    @DisplayName("Si la key no coincide con el symbol, el reparto por particion seria mentira")
    void key_que_no_cuadra() {
        String motivo = validator.validate("AAPL", tick("EUR/USD"));
        assertTrue(motivo.startsWith("la key (AAPL) no coincide"), motivo);
    }

    @Test
    @DisplayName("Un precio que no es positivo no vale")
    void precio_no_positivo() {
        Tick tick = tick("EUR/USD");
        tick.setPrice(BigDecimal.ZERO);
        assertEquals("precio ausente o no positivo", validator.validate("EUR/USD", tick));
    }

    @Test
    @DisplayName("Un tick con fecha muy en el futuro es un reloj mal puesto, no un dato")
    void timestamp_en_el_futuro() {
        Tick tick = tick("EUR/USD");
        tick.setEventTime(Instant.now().plusSeconds(3600));
        assertTrue(validator.validate("EUR/USD", tick).startsWith("timestamp en el futuro"), "deberia rechazarlo");
    }

    @Test
    @DisplayName("Una divisa que no esta en ISO-4217 se descarta")
    void divisa_inventada() {
        Tick tick = tick("EUR/USD");
        // Ojo: "XXX" NO vale como ejemplo de divisa inventada, es el codigo ISO-4217 de
        // "sin divisa" y Currency.getInstance lo acepta. Se usa uno que no existe.
        tick.setCurrency("ZZZ");
        assertEquals("divisa no ISO-4217: ZZZ", validator.validate("EUR/USD", tick));
    }

    /** Un tick valido, del que cada test cambia solo lo que quiere probar. */
    private static Tick tick(String symbol) {
        return Tick.newBuilder()
                .setEventId("evt-0001")
                .setSymbol(symbol)
                .setAssetClass(AssetClass.FX)
                .setExchange(Exchange.FX)
                .setCurrency("USD")
                .setPrice(new BigDecimal("1.1462"))
                .setSize(1000)
                .setEventTime(Instant.now())
                .setSource(TickSource.REFERENCE)
                .setSequence(7L)
                .build();
    }
}
