package com.aggora.simulator.simulation;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import com.aggora.avro.orders.Order;
import com.aggora.avro.orders.Side;
import com.aggora.simulator.config.AggoraProperties;
import com.aggora.simulator.domain.Exchange;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Genera ordenes de "traders" simulados hacia orders.incoming.
 *
 * Vive en el simulador porque este servicio ya es el generador de trafico del proyecto
 * y ya tiene los precios de referencia, que es lo que hace falta para poner precios
 * limite creibles. Un modulo aparte solo para esto seria un despliegue mas sin nada
 * nuevo que ensenar.
 *
 * El precio se pone alrededor de la referencia (+-0,5%) y los lados son aleatorios, asi
 * que unas ordenes cruzan con las que ya estaban en el libro y otras se quedan
 * esperando. Esa mezcla es justo lo que hace que el motor de matching tenga trabajo.
 */
@Component
public class OrderFlowGenerator {

    private static final Logger log = LoggerFactory.getLogger(OrderFlowGenerator.class);

    private static final List<String> ACCOUNTS =
            List.of("ACC-01", "ACC-02", "ACC-03", "ACC-04", "ACC-05");

    private final AggoraProperties props;
    private final TickEngine engine;
    private final KafkaTemplate<String, Order> ordersTemplate;
    private final Random random = new Random();
    private final AtomicLong sent = new AtomicLong();

    public OrderFlowGenerator(AggoraProperties props, TickEngine engine, KafkaTemplate<String, Order> ordersTemplate) {
        this.props = props;
        this.engine = engine;
        this.ordersTemplate = ordersTemplate;
    }

    @Scheduled(initialDelayString = "${aggora.order-initial-delay-ms}",
            fixedRateString = "${aggora.order-interval-ms}")
    public void emitOrders() {
        Instant now = Instant.now();
        List<AggoraProperties.Instrument> open = props.instruments().stream()
                .filter(instrument -> instrument.exchange().isOpen(now))
                .toList();
        if (open.isEmpty()) {
            return;
        }

        AggoraProperties.Instrument instrument = open.get(random.nextInt(open.size()));
        BigDecimal reference = engine.referenceOf(instrument.symbol());
        if (reference == null || reference.signum() <= 0) {
            return;
        }

        double factor = 1.0 + (random.nextDouble() - 0.5) * 0.01;
        BigDecimal price = reference.multiply(BigDecimal.valueOf(factor)).setScale(4, RoundingMode.HALF_UP);
        int quantity = 10 + random.nextInt(190);

        Order order = Order.newBuilder()
                .setOrderId(UUID.randomUUID().toString())
                .setAccountId(ACCOUNTS.get(random.nextInt(ACCOUNTS.size())))
                .setSymbol(instrument.symbol())
                .setSide(random.nextBoolean() ? Side.BUY : Side.SELL)
                .setPrice(price)
                .setQuantity(quantity)
                .setCurrency(instrument.currency())
                .setPlacedAt(now)
                .build();

        // La key es el SIMBOLO, no la cuenta: el motor de matching mantiene un libro por
        // instrumento, y para que un libro este completo sus ordenes tienen que caer
        // todas en la misma particion. Con la cuenta como key habria que reparticionar.
        ordersTemplate.send(props.topics().ordersIncoming(), instrument.symbol(), order);

        long total = sent.incrementAndGet();
        if (total % 20 == 0) {
            log.info("[ordenes] {} enviadas | ultima: {} {} {} x{} @ {} ({})",
                    total, order.getAccountId(), order.getSide(), order.getSymbol(),
                    quantity, price, instrument.exchange());
        }
    }
}
