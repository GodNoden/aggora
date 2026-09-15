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

        // Gancho para el ejercicio de dead-letter topics. Ojo con el detalle, que costo un
        // rato entenderlo: el fallo NO puede depender de un contador. Si se inyecta "cada
        // 20 ordenes", al reintentar el contador avanza, el fallo desaparece y el mensaje
        // se procesa bien: nunca llega al DLT. Un DLT sirve para mensajes VENENOSOS, es
        // decir, fallos que pertenecen al mensaje y no al momento. Por eso aqui se decide
        // por el propio orderId: o esa orden es venenosa siempre, o no lo es nunca.
        if (props.failEveryNOrders() > 0
                && Math.abs(order.getOrderId().hashCode()) % props.failEveryNOrders() == 0) {
            injectedFailures.incrementAndGet();
            throw new IllegalStateException("orden venenosa inyectada: esta orden no se puede procesar");
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
