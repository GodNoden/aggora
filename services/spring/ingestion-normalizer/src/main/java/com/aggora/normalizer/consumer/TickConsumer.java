package com.aggora.normalizer.consumer;

import java.time.Duration;
import java.time.Instant;
import java.util.Currency;
import java.util.concurrent.atomic.AtomicLong;

import com.aggora.avro.Tick;
import com.aggora.normalizer.config.AggoraProperties;

import java.nio.charset.StandardCharsets;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
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
 * Los mensajes invalidos van al topic de descartes (*.DLT) con el motivo en una cabecera,
 * y el offset se confirma igual: el descarte no para la particion.
 *
 * Y desde la Fase 11 el DLT tambien cubre lo que NO SE PUEDE LEER: un mensaje que no es Avro
 * lo publica el ErrorHandlingDeserializer junto con el DeadLetterPublishingRecoverer
 * (ver KafkaConsumerConfig), con los bytes originales y el mismo x-dlt-reason. Antes de eso,
 * un solo mensaje no-Avro metia a este consumidor en un bucle de reintentos que escribio
 * 17,4 GB de log en seis minutos.
 */
@Component
public class TickConsumer {

    private static final Logger log = LoggerFactory.getLogger(TickConsumer.class);

    /** Cabecera donde viaja el motivo del descarte, para poder investigarlo despues. */
    private static final String DLT_REASON_HEADER = "x-dlt-reason";

    private final AggoraProperties props;
    private final KafkaTemplate<String, com.aggora.avro.canonical.CanonicalTick> canonicalTemplate;
    private final KafkaTemplate<String, com.aggora.avro.reference.FxRate> fxTemplate;
    private final KafkaTemplate<String, Tick> deadLetterTemplate;
    private final AtomicLong received = new AtomicLong();
    private final AtomicLong discarded = new AtomicLong();
    private final AtomicLong published = new AtomicLong();

    public TickConsumer(AggoraProperties props,
                        KafkaTemplate<String, com.aggora.avro.canonical.CanonicalTick> canonicalTemplate,
                        KafkaTemplate<String, com.aggora.avro.reference.FxRate> fxTemplate,
                        KafkaTemplate<String, Tick> deadLetterTemplate) {
        this.props = props;
        this.canonicalTemplate = canonicalTemplate;
        this.fxTemplate = fxTemplate;
        this.deadLetterTemplate = deadLetterTemplate;
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
            sendToDeadLetter(record, problem);
        } else {
            com.aggora.avro.canonical.CanonicalTick canonical = toCanonical(record);
            publish(canonical);
            publishFxReference(record.value());
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

    /**
     * Un mensaje que no se puede procesar NO se descarta en silencio: se publica al topic
     * de descartes CON EL MOTIVO en una cabecera. Asi se puede mirar que llego mal y por
     * que, y el consumidor sigue avanzando en vez de atascarse.
     *
     * Se confirma el offset igualmente: el mensaje ya esta a salvo en el DLT.
     */
    private void sendToDeadLetter(ConsumerRecord<String, Tick> record, String reason) {
        // Se copian las cabeceras originales y se anade el motivo: dentro de tres semanas,
        // quien mire el DLT agradecera saber POR QUE se descarto cada mensaje.
        ProducerRecord<String, Tick> dead = new ProducerRecord<>(
                props.topics().ticksRawDlt(), null, record.key(), record.value());
        record.headers().forEach(dead.headers()::add);
        dead.headers().add(DLT_REASON_HEADER, reason.getBytes(StandardCharsets.UTF_8));

        deadLetterTemplate.send(dead)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("[DLT] no se pudo enviar {} al DLT: {}", record.key(), ex.getMessage());
                    }
                });
        discarded.incrementAndGet();
        log.warn("[DLT] part={} offset={} key={} -> {} | motivo: {}",
                record.partition(), record.offset(), record.key(), props.topics().ticksRawDlt(), reason);
    }

    /**
     * Los pares de divisas son DATO DE REFERENCIA: ademas del evento canonico se
     * publican a un topic compactado para que otros servicios los lean como tabla (el
     * ultimo tipo de cambio por par) sin tener que reprocesar el stream entero.
     *
     * ponytail: se asume que el par esta cotizado como XXX/USD, es decir, el precio dice
     * cuantos USD vale una unidad de la divisa base. Con pares al reves habria que
     * invertir el tipo.
     */
    private void publishFxReference(Tick tick) {
        if (tick.getAssetClass() != com.aggora.avro.AssetClass.FX) {
            return;
        }
        com.aggora.avro.reference.FxRate rate = com.aggora.avro.reference.FxRate.newBuilder()
                .setPair(tick.getSymbol())
                .setRate(tick.getPrice())
                .setEventTime(tick.getEventTime())
                .build();
        fxTemplate.send(props.topics().fxReference(), tick.getSymbol(), rate);
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
