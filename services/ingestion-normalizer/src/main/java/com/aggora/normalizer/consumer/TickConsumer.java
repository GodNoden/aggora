package com.aggora.normalizer.consumer;

import java.time.Duration;
import java.time.Instant;
import java.util.Currency;
import java.util.concurrent.atomic.AtomicLong;

import com.aggora.avro.Tick;
import com.aggora.normalizer.config.AggoraProperties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Consumidor de market.ticks.raw con COMMIT MANUAL, y ahora tambien productor del
 * evento canonico en market.ticks.canonical.
 *
 * Diferencia con el commit automatico: aqui el offset se confirma cuando el mensaje
 * ya esta procesado, no cuando el cliente lo trajo del broker. Si el proceso muere a
 * media faena, al reiniciar se relee desde el ultimo offset confirmado: eso es
 * at-least-once (puede repetirse, no puede perderse).
 *
 * Los mensajes invalidos se descartan con un warning y se confirman: en la Fase 6
 * esto se convierte en un topic de descartes (*.DLT) en lugar de un log.
 */
@Component
public class TickConsumer {

    private static final Logger log = LoggerFactory.getLogger(TickConsumer.class);

    private final AggoraProperties props;
    private final KafkaTemplate<String, com.aggora.avro.canonical.CanonicalTick> canonicalTemplate;
    private final AtomicLong received = new AtomicLong();
    private final AtomicLong discarded = new AtomicLong();
    private final AtomicLong published = new AtomicLong();

    public TickConsumer(AggoraProperties props,
                        KafkaTemplate<String, com.aggora.avro.canonical.CanonicalTick> canonicalTemplate) {
        this.props = props;
        this.canonicalTemplate = canonicalTemplate;
    }

    /**
     * El groupId se declara explicitamente: si se omite, Spring Kafka usa el "id" del
     * listener como group.id y el de application.yml quedaria ignorado.
     */
    @KafkaListener(id = "tick-normalizer",
            groupId = "${spring.kafka.consumer.group-id}",
            topics = "${aggora.topics.ticks-raw}")
    public void onTick(ConsumerRecord<String, Tick> record, Acknowledgment ack) {
        long count = received.incrementAndGet();
        String problem = validate(record);

        if (problem != null) {
            discarded.incrementAndGet();
            log.warn("[descarta] part={} offset={} key={} motivo={}",
                    record.partition(), record.offset(), record.key(), problem);
        } else {
            com.aggora.avro.canonical.CanonicalTick canonical = toCanonical(record);
            publish(canonical);
            logCanonical(record, canonical, count);
        }

        simulateSlowProcessing();

        // El commit manual: a partir de aqui el mensaje no se repetira aunque el
        // proceso se caiga justo despues.
        ack.acknowledge();
    }

    /**
     * Normalizacion de la Fase 2: mismo dato, pero como evento canonico con su propio
     * esquema en market.ticks.canonical. Ademas de los campos del tick, anade cuando
     * se normalizo y de que particion/offset del crudo salio (trazabilidad).
     *
     * Se construye un objeto NUEVO en vez de reutilizar el del crudo porque los dos
     * subjects evolucionan por separado: el canonico puede ganar campos sin tocar el
     * contrato del crudo.
     */
    private com.aggora.avro.canonical.CanonicalTick toCanonical(ConsumerRecord<String, Tick> record) {
        Tick tick = record.value();
        return com.aggora.avro.canonical.CanonicalTick.newBuilder()
                .setEventId(tick.getEventId())
                .setSymbol(tick.getSymbol())
                .setAssetClass(com.aggora.avro.canonical.AssetClass.valueOf(tick.getAssetClass().name()))
                .setExchange(com.aggora.avro.canonical.Exchange.valueOf(tick.getExchange().name()))
                .setCurrency(tick.getCurrency())
                .setPrice(tick.getPrice())
                .setSize(tick.getSize())
                .setEventTime(tick.getEventTime())
                .setSource(com.aggora.avro.canonical.TickSource.valueOf(tick.getSource().name()))
                .setSequence(tick.getSequence())
                .setNormalizedAt(Instant.now())
                .setOriginPartition(record.partition())
                .setOriginOffset(record.offset())
                .build();
    }

    private void publish(com.aggora.avro.canonical.CanonicalTick canonical) {
        canonicalTemplate.send(props.topics().ticksCanonical(), canonical.getSymbol(), canonical)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("[canonico] fallo publicando {}: {}", canonical.getSymbol(), ex.getMessage());
                        return;
                    }
                    published.incrementAndGet();
                });
    }

    /** Log resumido: los primeros y uno de cada 500, para no inundar la consola. */
    private void logCanonical(ConsumerRecord<String, Tick> record,
                              com.aggora.avro.canonical.CanonicalTick canonical, long count) {
        if (count <= 5 || count % 500 == 0) {
            log.info("[canonico] {} {} {} precio={} size={} seq={} fuente={} t={} -> canonical(part={} offset={}) | recibidos={} descartados={} publicados={}",
                    canonical.getSymbol(), canonical.getAssetClass(), canonical.getCurrency(),
                    canonical.getPrice(), canonical.getSize(), canonical.getSequence(),
                    canonical.getSource(), canonical.getEventTime(),
                    record.partition(), record.offset(),
                    count, discarded.get(), published.get());
        }
    }

    public long receivedCount() {
        return received.get();
    }

    public long discardedCount() {
        return discarded.get();
    }

    /** Devuelve el motivo por el que el mensaje no es valido, o null si lo es. */
    private String validate(ConsumerRecord<String, Tick> record) {
        Tick tick = record.value();
        if (tick == null) {
            return "payload nulo";
        }
        if (tick.getSymbol() == null || tick.getSymbol().isBlank()) {
            return "symbol vacio";
        }
        if (record.key() != null && !record.key().equals(tick.getSymbol())) {
            return "la key (" + record.key() + ") no coincide con el symbol (" + tick.getSymbol() + ")";
        }
        if (tick.getPrice() == null || tick.getPrice().signum() <= 0) {
            return "precio ausente o no positivo";
        }
        if (tick.getEventTime() == null) {
            return "sin timestamp";
        }
        if (tick.getEventTime().isAfter(Instant.now().plus(Duration.ofMinutes(5)))) {
            return "timestamp en el futuro: " + tick.getEventTime();
        }
        if (tick.getAssetClass() == null || tick.getExchange() == null || tick.getSource() == null) {
            return "falta assetClass, exchange o source";
        }
        try {
            Currency.getInstance(tick.getCurrency());
        } catch (IllegalArgumentException ex) {
            return "divisa no ISO-4217: " + tick.getCurrency();
        }
        return null;
    }

    /** Ver "aggora.processing-delay-ms": sirve para poder matar el proceso a media faena. */
    private void simulateSlowProcessing() {
        long processingDelayMs = props.processingDelayMs();
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
