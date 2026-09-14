package com.aggora.normalizer.consumer;

import java.time.Duration;
import java.time.Instant;
import java.util.Currency;
import java.util.concurrent.atomic.AtomicLong;

import com.aggora.normalizer.config.AggoraProperties;
import com.aggora.normalizer.domain.TickEvent;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Consumidor de market.ticks.raw con COMMIT MANUAL.
 *
 * Diferencia con el commit automático: aquí el offset se confirma cuando el mensaje
 * ya está procesado, no cuando el cliente lo trajo del broker. Si el proceso muere a
 * media faena, al reiniciar se relee desde el último offset confirmado: eso es
 * at-least-once (puede repetirse, no puede perderse).
 *
 * Los mensajes inválidos se descartan con un warning y se confirman: en la Fase 6
 * esto se convierte en un topic de descartes (*.DLT) en lugar de un log.
 */
@Component
public class TickConsumer {

    private static final Logger log = LoggerFactory.getLogger(TickConsumer.class);

    private final long processingDelayMs;
    private final AtomicLong received = new AtomicLong();
    private final AtomicLong discarded = new AtomicLong();

    public TickConsumer(AggoraProperties props) {
        this.processingDelayMs = props.processingDelayMs();
    }

    /**
     * El groupId se declara explícitamente: si se omite, Spring Kafka usa el "id" del
     * listener como group.id y el de application.yml quedaría ignorado.
     */
    @KafkaListener(id = "tick-normalizer",
            groupId = "${spring.kafka.consumer.group-id}",
            topics = "${aggora.topics.ticks-raw}")
    public void onTick(ConsumerRecord<String, TickEvent> record, Acknowledgment ack) {
        long count = received.incrementAndGet();
        String problem = validate(record);

        if (problem != null) {
            discarded.incrementAndGet();
            log.warn("[descarta] part={} offset={} key={} motivo={}",
                    record.partition(), record.offset(), record.key(), problem);
        } else {
            logCanonical(record, count);
        }

        simulateSlowProcessing();

        // El commit manual: a partir de aquí el mensaje no se repetirá aunque el
        // proceso se caiga justo después.
        ack.acknowledge();
    }

    public long receivedCount() {
        return received.get();
    }

    public long discardedCount() {
        return discarded.get();
    }

    /**
     * Normalización de la Fase 1: validar forma y dejar el evento en forma canónica
     * (timestamp ya en UTC, divisa ISO-4217 verificada). El evento normalizado se
     * publicará a market.ticks.canonical en la Fase 2, con su esquema Avro.
     */
    private void logCanonical(ConsumerRecord<String, TickEvent> record, long count) {
        TickEvent tick = record.value();
        if (count <= 5 || count % 500 == 0) {
            log.info("[canónico] {} {} {} {} precio={} size={} seq={} fuente={} t={} | part={} offset={} | recibidos={} descartados={}",
                    tick.symbol(), tick.assetClass(), tick.exchange(), tick.currency(),
                    tick.price(), tick.size(), tick.sequence(), tick.source(), tick.eventTime(),
                    record.partition(), record.offset(), count, discarded.get());
        }
    }

    /** Devuelve el motivo por el que el mensaje no es válido, o null si lo es. */
    private String validate(ConsumerRecord<String, TickEvent> record) {
        TickEvent tick = record.value();
        if (tick == null) {
            return "payload nulo";
        }
        if (tick.symbol() == null || tick.symbol().isBlank()) {
            return "symbol vacío";
        }
        if (record.key() != null && !record.key().equals(tick.symbol())) {
            return "la key (" + record.key() + ") no coincide con el symbol (" + tick.symbol() + ")";
        }
        if (tick.price() == null || tick.price().signum() <= 0) {
            return "precio ausente o no positivo";
        }
        if (tick.eventTime() == null) {
            return "sin timestamp";
        }
        if (tick.eventTime().isAfter(Instant.now().plus(Duration.ofMinutes(5)))) {
            return "timestamp en el futuro: " + tick.eventTime();
        }
        if (tick.assetClass() == null || tick.exchange() == null || tick.source() == null) {
            return "falta assetClass, exchange o source";
        }
        try {
            Currency.getInstance(tick.currency());
        } catch (IllegalArgumentException ex) {
            return "divisa no ISO-4217: " + tick.currency();
        }
        return null;
    }

    /** Ver "aggora.processing-delay-ms": sirve para poder matar el proceso a media faena. */
    private void simulateSlowProcessing() {
        if (processingDelayMs <= 0) {
            return;
        }
        try {
            Thread.sleep(processingDelayMs);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }
}
