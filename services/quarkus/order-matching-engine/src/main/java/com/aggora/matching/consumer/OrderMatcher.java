package com.aggora.matching.consumer;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import com.aggora.avro.orders.Execution;
import com.aggora.avro.orders.Order;
import com.aggora.matching.book.OrderBooks;
import com.aggora.matching.config.AggoraConfig;

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.smallrye.reactive.messaging.kafka.KafkaRecord;
import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;
import io.smallrye.reactive.messaging.kafka.transactions.KafkaTransactions;

/**
 * Escucha las ordenes, las cruza contra el libro y publica las ejecuciones. Con EXACTLY-ONCE.
 *
 * <p><b>El montaje de exactly-once, que es de lo que va este servicio, cambia bastante de un
 * framework a otro:</b>
 *
 * <ul>
 *   <li>En Spring se cuelga un {@code KafkaTransactionManager} del contenedor de escucha: Spring
 *       abre una transaccion por poll, el listener publica dentro de ella y, al terminar, los
 *       offsets del consumidor se confirman DENTRO de la misma transaccion. Casi todo es
 *       configuracion (los beans de {@code KafkaTransactionConfig}).</li>
 *   <li>En SmallRye la transaccion se pide <b>en el codigo</b>: se inyecta un
 *       {@code KafkaTransactions} del canal de salida y se envuelve el procesamiento en
 *       {@code withTransactionAndAck}, que ademas mete los offsets del mensaje consumido en la
 *       transaccion. Es mas explicito (se ve donde empieza y acaba) y menos magico.</li>
 * </ul>
 *
 * <p>Lo que NO cambia: el productor es idempotente y transaccional, y el consumidor lee con
 * {@code isolation.level=read_committed}, asi que las ejecuciones abortadas no las ve nadie.
 *
 * <p>Cuando la orden es venenosa se publica al topic de descartes y se confirma el offset: la
 * particion sigue avanzando en vez de quedarse atascada (que era el problema de la Fase 4).
 */
@ApplicationScoped
public class OrderMatcher {

    private static final Logger log = LoggerFactory.getLogger(OrderMatcher.class);

    /** Cabecera donde viaja el motivo del descarte, igual que en el normalizer. */
    static final String DLT_REASON_HEADER = "x-dlt-reason";

    private final OrderBooks books;
    private final KafkaTransactions<Execution> transactions;
    private final MutinyEmitter<Order> deadLetterEmitter;
    private final AggoraConfig props;
    private final AtomicLong received = new AtomicLong();
    private final AtomicLong executed = new AtomicLong();
    private final AtomicLong injectedFailures = new AtomicLong();

    @Inject
    public OrderMatcher(OrderBooks books,
                        @Channel("executions") KafkaTransactions<Execution> transactions,
                        @Channel("orders-incoming-dlt") MutinyEmitter<Order> deadLetterEmitter,
                        AggoraConfig props) {
        this.books = books;
        this.transactions = transactions;
        this.deadLetterEmitter = deadLetterEmitter;
        this.props = props;
    }

    @Incoming("orders-incoming")
    @Blocking
    public Uni<Void> onOrder(KafkaRecord<String, Order> record) {
        Order order = record.getPayload();
        long count = received.incrementAndGet();
        List<Execution> matches = books.match(order);

        return transactions.withTransactionAndAck(record, emitter -> {
            // Todo lo que se publica aqui dentro entra en la MISMA transaccion que el offset del
            // mensaje consumido: o se ve la ejecucion Y avanza el offset, o no pasa ninguna de
            // las dos cosas.
            matches.forEach(execution -> emitter.send(KafkaRecord.of(execution.getSymbol(), execution)));
            executed.addAndGet(matches.size());

            if (count <= 5 || count % 25 == 0) {
                log.info("[matching] {} {} {} x{} @ {} | cruces={} | ordenes en libros={} | ejecutadas={}",
                        order.getAccountId(), order.getSide(), order.getSymbol(), order.getQuantity(),
                        order.getPrice(), matches.size(), books.restingOrders(), executed.get());
            }

            // Gancho para los ejercicios de exactly-once y de DLT. El fallo NO puede depender de un
            // contador (al reintentar el contador avanza, el fallo desaparece y el mensaje nunca
            // llega al DLT): se decide por el propio orderId, o sea que la orden es venenosa
            // siempre o nunca.
            //
            // Y se hace DESPUES de publicar, a proposito: asi la transaccion se aborta con las
            // ejecuciones ya dentro, que es justo lo que demuestra el exactly-once (el topic tiene
            // mensajes ABORTADOS que solo ve quien lee con read_uncommitted).
            if (esVenenosa(order)) {
                injectedFailures.incrementAndGet();
                log.warn("[matching] orden venenosa inyectada {} ({}): se aborta la transaccion y va al DLT",
                        order.getOrderId(), order.getSymbol());
                emitter.markForAbort();
                return alDescarte(record, "orden venenosa inyectada: esta orden no se puede procesar");
            }
            return Uni.createFrom().voidItem();
        });
    }

    /** Una orden que no se puede procesar va al topic de descartes con el motivo, y se confirma. */
    private Uni<Void> alDescarte(KafkaRecord<String, Order> record, String motivo) {
        OutgoingKafkaRecordMetadata<String> metadata = OutgoingKafkaRecordMetadata.<String>builder()
                .withKey(record.getKey())
                .withHeaders(new RecordHeaders().add(DLT_REASON_HEADER, motivo.getBytes(StandardCharsets.UTF_8)))
                .build();
        return deadLetterEmitter.sendMessage(
                        org.eclipse.microprofile.reactive.messaging.Message.of(record.getPayload()).addMetadata(metadata))
                .chain(() -> Uni.createFrom().completionStage(record.ack()));
    }

    private boolean esVenenosa(Order order) {
        return props.failEveryNOrders() > 0
                && Math.abs(order.getOrderId().hashCode()) % props.failEveryNOrders() == 0;
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
