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

import static org.assertj.core.api.Assertions.assertThat;

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
        assertThat(book.match(order(Side.SELL, "100.00", 50, "ACC-SELL"))).isEmpty();
        assertThat(book.restingOrders()).isEqualTo(1);

        // La compra llega dispuesta a pagar 105: se ejecuta a 100, el precio de la venta.
        List<Execution> executions = book.match(order(Side.BUY, "105.00", 50, "ACC-BUY"));

        assertThat(executions).hasSize(1);
        Execution execution = executions.getFirst();
        assertThat(execution.getPrice()).isEqualByComparingTo(new BigDecimal("100.00"));
        assertThat(execution.getQuantity()).isEqualTo(50);
        assertThat(execution.getBuyAccountId()).isEqualTo("ACC-BUY");
        assertThat(execution.getSellAccountId()).isEqualTo("ACC-SELL");
        // Los dos lados quedan vacios.
        assertThat(book.restingOrders()).isZero();
    }

    @Test
    @DisplayName("Si la cantidad no cuadra, se ejecuta lo que se puede y el resto espera")
    void ejecucion_parcial() {
        book.match(order(Side.SELL, "100.00", 100, "ACC-SELL"));

        List<Execution> executions = book.match(order(Side.BUY, "100.00", 40, "ACC-BUY"));

        assertThat(executions).hasSize(1);
        assertThat(executions.getFirst().getQuantity()).isEqualTo(40);
        // La venta sigue en el libro con lo que le queda: 60.
        assertThat(book.restingOrders()).isEqualTo(1);

        // Y otra compra de 60 la remata.
        List<Execution> rest = book.match(order(Side.BUY, "100.00", 60, "ACC-BUY-2"));
        assertThat(rest).hasSize(1);
        assertThat(rest.getFirst().getQuantity()).isEqualTo(60);
        assertThat(book.restingOrders()).isZero();
    }

    @Test
    @DisplayName("Si los precios no se cruzan, la orden se queda esperando")
    void sin_cruce_no_hay_ejecucion() {
        book.match(order(Side.SELL, "110.00", 50, "ACC-SELL"));

        // Compra a 100 con la venta mas barata a 110: no hay trato.
        assertThat(book.match(order(Side.BUY, "100.00", 50, "ACC-BUY"))).isEmpty();
        assertThat(book.restingOrders()).isEqualTo(2);
    }

    @Test
    @DisplayName("A igual precio, se atiende antes al que llego antes (precio-tiempo)")
    void prioridad_por_tiempo() {
        book.match(order(Side.SELL, "100.00", 10, "ACC-PRIMERO"));
        book.match(order(Side.SELL, "100.00", 10, "ACC-SEGUNDO"));

        List<Execution> executions = book.match(order(Side.BUY, "100.00", 10, "ACC-BUY"));

        assertThat(executions).hasSize(1);
        assertThat(executions.getFirst().getSellAccountId()).isEqualTo("ACC-PRIMERO");
        assertThat(book.restingOrders()).isEqualTo(1);
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
