package com.aggora.matching.book;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import com.aggora.avro.orders.Execution;
import com.aggora.avro.orders.Order;
import com.aggora.avro.orders.Side;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * La logica del libro: cruces, precio pasivo, prioridad precio-tiempo y lo que se queda
 * esperando. Es la parte con logica de verdad del motor, asi que tiene test.
 */
class OrderBookTest {

    private final OrderBook book = new OrderBook("AAPL");

    @Test
    @DisplayName("Una compra que cruza con una venta se ejecuta al precio de la que ya estaba")
    void cruza_al_precio_de_la_orden_pasiva() {
        // La venta entra primero (queda en el libro) a 100.
        assertTrue(book.match(order(Side.SELL, "100.00", 50, "ACC-SELL")).isEmpty());
        assertEquals(1, book.restingOrders());

        // La compra llega dispuesta a pagar 105: se ejecuta a 100, el precio de la venta.
        List<Execution> executions = book.match(order(Side.BUY, "105.00", 50, "ACC-BUY"));

        assertEquals(1, executions.size());
        Execution execution = executions.getFirst();
        assertDecimal("100.00", execution.getPrice());
        assertEquals(50, execution.getQuantity());
        assertEquals("ACC-BUY", execution.getBuyAccountId());
        assertEquals("ACC-SELL", execution.getSellAccountId());
        // Los dos lados quedan vacios.
        assertEquals(0, book.restingOrders());
    }

    @Test
    @DisplayName("Si la cantidad no cuadra, se ejecuta lo que se puede y el resto espera")
    void ejecucion_parcial() {
        book.match(order(Side.SELL, "100.00", 100, "ACC-SELL"));

        List<Execution> executions = book.match(order(Side.BUY, "100.00", 40, "ACC-BUY"));

        assertEquals(1, executions.size());
        assertEquals(40, executions.getFirst().getQuantity());
        // La venta sigue en el libro con lo que le queda: 60.
        assertEquals(1, book.restingOrders());

        // Y otra compra de 60 la remata.
        List<Execution> rest = book.match(order(Side.BUY, "100.00", 60, "ACC-BUY-2"));
        assertEquals(1, rest.size());
        assertEquals(60, rest.getFirst().getQuantity());
        assertEquals(0, book.restingOrders());
    }

    @Test
    @DisplayName("Si los precios no se cruzan, la orden se queda esperando")
    void sin_cruce_no_hay_ejecucion() {
        book.match(order(Side.SELL, "110.00", 50, "ACC-SELL"));

        // Compra a 100 con la venta mas barata a 110: no hay trato.
        assertTrue(book.match(order(Side.BUY, "100.00", 50, "ACC-BUY")).isEmpty());
        assertEquals(2, book.restingOrders());
    }

    @Test
    @DisplayName("A igual precio, se atiende antes al que llego antes (precio-tiempo)")
    void prioridad_por_tiempo() {
        book.match(order(Side.SELL, "100.00", 10, "ACC-PRIMERO"));
        book.match(order(Side.SELL, "100.00", 10, "ACC-SEGUNDO"));

        List<Execution> executions = book.match(order(Side.BUY, "100.00", 10, "ACC-BUY"));

        assertEquals(1, executions.size());
        assertEquals("ACC-PRIMERO", executions.getFirst().getSellAccountId());
        assertEquals(1, book.restingOrders());
    }

    /** Compara decimales por valor, no por escala (lo que hacia isEqualByComparingTo). */
    private static void assertDecimal(String esperado, BigDecimal real) {
        assertEquals(0, real.compareTo(new BigDecimal(esperado)),
                "esperado " + esperado + " y era " + real);
    }

    private static Order order(Side side, String price, int quantity, String account) {
        return Order.newBuilder()
                .setOrderId(UUID.randomUUID().toString())
                .setAccountId(account)
                .setSymbol("AAPL")
                .setSide(side)
                .setPrice(new BigDecimal(price).setScale(4, RoundingMode.HALF_UP))
                .setQuantity(quantity)
                .setPlacedAt(Instant.parse("2026-09-15T10:00:00Z"))
                .build();
    }
}
