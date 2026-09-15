package com.aggora.matching.book;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.aggora.avro.orders.Execution;
import com.aggora.avro.orders.Order;

import jakarta.enterprise.context.ApplicationScoped;

/**
 * Un libro por instrumento.
 *
 * Los libros se crean a demanda y se quedan para siempre (un instrumento sin ordenes
 * solo ocupa un objeto vacio). El mapa es concurrente porque el contenedor de escucha
 * puede tener varios hilos si se sube la concurrencia.
 */
@ApplicationScoped
public class OrderBooks {

    private final Map<String, OrderBook> books = new ConcurrentHashMap<>();

    public List<Execution> match(Order order) {
        return books.computeIfAbsent(order.getSymbol(), OrderBook::new).match(order);
    }

    public int booksOpen() {
        return books.size();
    }

    public int restingOrders() {
        return books.values().stream().mapToInt(OrderBook::restingOrders).sum();
    }
}
