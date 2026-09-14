package com.aggora.simulator.producer;

import java.util.concurrent.atomic.AtomicLong;

import com.aggora.simulator.config.AggoraProperties;
import com.aggora.simulator.domain.TickEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Envío de ticks a Kafka.
 *
 * Async con callback (no sync): el productor manda y sigue generando ticks; el
 * callback registra el offset real o el error. Bloquear en cada send limitaría el
 * throughput a un round-trip por mensaje.
 *
 * Fiabilidad: el productor es idempotente (enable.idempotence=true en el yml) y
 * acks=all, de modo que un reintento por fallo transitorio NO duplica el mensaje.
 */
@Component
public class TickProducer {

    private static final Logger log = LoggerFactory.getLogger(TickProducer.class);

    private final KafkaTemplate<String, TickEvent> kafkaTemplate;
    private final String topic;
    private final AtomicLong sent = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();

    public TickProducer(KafkaTemplate<String, TickEvent> kafkaTemplate, AggoraProperties props) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = props.topics().ticksRaw();
    }

    public void send(TickEvent tick) {
        kafkaTemplate.send(topic, tick.symbol(), tick).whenComplete((result, ex) -> {
            if (ex != null) {
                failed.incrementAndGet();
                log.error("[produce] fallo enviando {}: {}", tick.symbol(), ex.getMessage());
                return;
            }
            long total = sent.incrementAndGet();
            if (total % 1000 == 0) {
                log.info("[produce] {} ticks enviados | último: {} part={} offset={} | fallos={}",
                        total, tick.symbol(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset(),
                        failed.get());
            }
        });
    }

    public long sentCount() {
        return sent.get();
    }

    public long failedCount() {
        return failed.get();
    }
}
