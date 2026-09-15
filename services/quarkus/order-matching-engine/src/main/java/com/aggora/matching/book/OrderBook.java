package com.aggora.matching.book;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.UUID;

import com.aggora.avro.orders.Execution;
import com.aggora.avro.orders.Order;
import com.aggora.avro.orders.Side;

/**
 * El libro de ordenes de UN instrumento.
 *
 * Dos listas ordenadas por precio: las compras (bids) de mayor a menor y las ventas
 * (asks) de menor a mayor, para que la mejor orden de cada lado este siempre la
 * primera. Dentro de un mismo precio, la cola respeta el orden de llegada: eso es la
 * prioridad precio-tiempo, que es como funcionan los mercados de verdad.
 *
 * Solo hay ordenes LIMITADAS (con precio). Las de mercado quedan fuera del alcance.
 *
 * El estado vive en memoria: si el servicio se cae, el libro se pierde y se reconstruye
 * con las ordenes que vuelvan a entrar. Para que fuese persistente habria que guardarlo
 * en un state store (como hace Kafka Streams) o en la base de datos del outbox.
 */
public class OrderBook {

    private final String symbol;
    private final NavigableMap<BigDecimal, Deque<Order>> bids = new TreeMap<>(Comparator.reverseOrder());
    private final NavigableMap<BigDecimal, Deque<Order>> asks = new TreeMap<>();

    public OrderBook(String symbol) {
        this.symbol = symbol;
    }

    public String symbol() {
        return symbol;
    }

    /**
     * Cruza la orden que llega con lo que haya en el lado contrario y devuelve las
     * ejecuciones. Si sobra cantidad, la orden se queda en el libro esperando.
     */
    public List<Execution> match(Order incoming) {
        List<Execution> executions = new ArrayList<>();
        NavigableMap<BigDecimal, Deque<Order>> opposite = incoming.getSide() == Side.BUY ? asks : bids;
        int remaining = incoming.getQuantity();

        while (remaining > 0 && !opposite.isEmpty()) {
            Map.Entry<BigDecimal, Deque<Order>> best = opposite.firstEntry();
            if (!crosses(incoming, best.getKey())) {
                break;
            }
            Deque<Order> queue = best.getValue();
            Order resting = queue.peekFirst();
            int traded = Math.min(remaining, resting.getQuantity());

            // El precio lo pone la orden que ya estaba: la parte pasiva marca el precio.
            executions.add(executionOf(incoming, resting, best.getKey(), traded));

            remaining -= traded;
            resting.setQuantity(resting.getQuantity() - traded);
            if (resting.getQuantity() == 0) {
                queue.pollFirst();
            }
            if (queue.isEmpty()) {
                opposite.remove(best.getKey());
            }
        }

        if (remaining > 0) {
            rest(incoming, remaining);
        }
        return executions;
    }

    /** Cuantas ordenes siguen esperando en el libro (para el log y los tests). */
    public int restingOrders() {
        return bids.values().stream().mapToInt(Deque::size).sum()
                + asks.values().stream().mapToInt(Deque::size).sum();
    }

    private boolean crosses(Order incoming, BigDecimal bestOppositePrice) {
        return incoming.getSide() == Side.BUY
                ? incoming.getPrice().compareTo(bestOppositePrice) >= 0
                : incoming.getPrice().compareTo(bestOppositePrice) <= 0;
    }

    private void rest(Order order, int quantity) {
        Order resting = Order.newBuilder(order).setQuantity(quantity).build();
        side(order.getSide()).computeIfAbsent(order.getPrice(), price -> new ArrayDeque<>()).addLast(resting);
    }

    private NavigableMap<BigDecimal, Deque<Order>> side(Side side) {
        return side == Side.BUY ? bids : asks;
    }

    private Execution executionOf(Order incoming, Order resting, BigDecimal price, int quantity) {
        Order buy = incoming.getSide() == Side.BUY ? incoming : resting;
        Order sell = incoming.getSide() == Side.SELL ? incoming : resting;
        return Execution.newBuilder()
                .setExecutionId(UUID.randomUUID().toString())
                .setSymbol(symbol)
                .setPrice(price)
                .setQuantity(quantity)
                .setCurrency(buy.getCurrency())
                .setBuyOrderId(buy.getOrderId())
                .setSellOrderId(sell.getOrderId())
                .setBuyAccountId(buy.getAccountId())
                .setSellAccountId(sell.getAccountId())
                .setExecutedAt(Instant.now())
                .build();
    }
}
