package com.aggora.matching.consumer;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import com.aggora.avro.orders.Execution;
import com.aggora.avro.orders.Order;
import com.aggora.matching.book.OrderBooks;
import com.aggora.matching.config.AggoraProperties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Escucha las ordenes, las cruza contra el libro y publica las ejecuciones.
 *
 * No hay acknowledge manual: con la transaccion enganchada al contenedor, el offset de
 * la orden entra en el MISMO commit que las ejecuciones publicadas. Por eso tampoco hay
 * que preocuparse por el orden de "publicar y confirmar": no son dos pasos.
 */
@Component
public class OrderMatcher {

    private static final Logger log = LoggerFactory.getLogger(OrderMatcher.class);

    private final OrderBooks books;
    private final KafkaTemplate<String, Execution> executionsTemplate;
    private final AggoraProperties props;
    private final AtomicLong received = new AtomicLong();
    private final AtomicLong executed = new AtomicLong();
    private final AtomicLong injectedFailures = new AtomicLong();

    public OrderMatcher(OrderBooks books,
                        KafkaTemplate<String, Execution> executionsTemplate,
                        AggoraProperties props) {
        this.books = books;
        this.executionsTemplate = executionsTemplate;
        this.props = props;
    }

    @KafkaListener(id = "order-matcher", topics = "${aggora.topics.orders-incoming}")
    public void onOrder(ConsumerRecord<String, Order> record) {
        Order order = record.value();
        long count = received.incrementAndGet();

        List<Execution> matches = books.match(order);
        matches.forEach(execution -> {
            executionsTemplate.send(props.topics().executions(), execution.getSymbol(), execution);
            executed.incrementAndGet();
        });

        if (count <= 5 || count % 25 == 0) {
            log.info("[matching] {} {} {} x{} @ {} | cruces={} | ordenes en libros={} | ejecutadas={}",
                    order.getAccountId(), order.getSide(), order.getSymbol(), order.getQuantity(),
                    order.getPrice(), matches.size(), books.restingOrders(), executed.get());
        }

        // Experimento de exactly-once (ver README): se lanza DESPUES de publicar, para
        // que se vea que la transaccion deshace lo publicado y no avanza el offset.
        if (props.failEveryNOrders() > 0 && count % props.failEveryNOrders() == 0) {
            injectedFailures.incrementAndGet();
            throw new IllegalStateException("fallo inyectado tras publicar: la transaccion se deshace");
        }
    }

    public long receivedCount() {
        return received.get();
    }

    public long executedCount() {
        return executed.get();
    }

    public long injectedFailures() {
        return injectedFailures.get();
    }
}
