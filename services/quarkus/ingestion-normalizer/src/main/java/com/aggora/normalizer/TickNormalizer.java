package com.aggora.normalizer;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import com.aggora.avro.Tick;
import com.aggora.avro.canonical.CanonicalTick;
import com.aggora.avro.reference.FxRate;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.smallrye.common.annotation.Blocking;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.smallrye.reactive.messaging.kafka.KafkaRecord;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;
import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;

/**
 * El normalizer de Aggora en Quarkus: mismo comportamiento que la version Spring, otra
 * implementacion. Consume {@code market.ticks.raw}, valida, publica el evento canonico en
 * {@code market.ticks.canonical}, publica los tipos de cambio de referencia en el topic
 * compactado y manda lo que no se puede procesar a {@code market.ticks.raw.DLT} con el motivo
 * en una cabecera.
 *
 * <p><b>Las dos puertas al DLT.</b> Lo que falla al VALIDAR lo manda este metodo. Lo que no se
 * puede ni LEER (no es Avro) no llega aqui: lo cubre {@link DltDeserializationFailureHandler},
 * que publica los bytes originales y devuelve {@code null}; entonces aqui se ve un payload nulo,
 * se confirma el offset y se sigue. Sin las dos, un mensaje ilegible revocaba las particiones
 * (ver docs/decisions.md).
 *
 * <p><b>El mapeo de conceptos, que es lo que se viene a comparar:</b>
 *
 * <table>
 *   <tr><td>{@code @KafkaListener} + {@code Acknowledgment}</td><td>{@code @Incoming} + {@code record.ack()}</td></tr>
 *   <tr><td>{@code KafkaTemplate} por tipo</td><td>un {@code Emitter} por canal ({@code @Channel})</td></tr>
 *   <tr><td>{@code AckMode.MANUAL}</td><td>{@code enable.auto.commit=false} + {@code commit-strategy=latest}</td></tr>
 *   <tr><td>{@code spring.kafka.consumer.*}</td><td>{@code mp.messaging.incoming.<canal>.*}</td></tr>
 * </table>
 *
 * <p><b>El at-least-once</b> se consigue igual que en Spring: se procesa y se publica primero,
 * y el offset se confirma despues. Si el proceso muere en medio, al volver se relee desde el
 * ultimo offset confirmado y ese mensaje se vuelve a procesar (puede repetirse, no puede
 * perderse).
 *
 * <p>Un matiz que vale para las dos implementaciones y que hay que decir: el envio al topic
 * canonico es asincrono y <b>no se espera</b> antes de confirmar el offset (igual que en la
 * version Spring). Es lo que hace que el pipeline tenga el rendimiento que tiene, y tambien
 * significa que una caida entre el envio y su confirmacion puede perder ese mensaje. Se queda
 * asi para que las dos implementaciones se comporten igual y la comparacion sea justa.
 */
@ApplicationScoped
public class TickNormalizer {

    private static final Logger log = LoggerFactory.getLogger(TickNormalizer.class);

    /** Cabecera donde viaja el motivo del descarte, para poder investigarlo despues. */
    static final String DLT_REASON_HEADER = "x-dlt-reason";

    private final MutinyEmitter<CanonicalTick> canonicalEmitter;
    private final MutinyEmitter<FxRate> fxEmitter;
    private final MutinyEmitter<Tick> deadLetterEmitter;
    private final TickValidator validator = new TickValidator();
    private final long processingDelayMs;

    private final AtomicLong received = new AtomicLong();
    private final AtomicLong discarded = new AtomicLong();
    private final AtomicLong published = new AtomicLong();

    @Inject
    public TickNormalizer(@Channel("ticks-canonical") MutinyEmitter<CanonicalTick> canonicalEmitter,
                          @Channel("fx-reference") MutinyEmitter<FxRate> fxEmitter,
                          @Channel("ticks-raw-dlt") MutinyEmitter<Tick> deadLetterEmitter,
                          @ConfigProperty(name = "aggora.processing-delay-ms", defaultValue = "0")
                          long processingDelayMs) {
        this.canonicalEmitter = canonicalEmitter;
        this.fxEmitter = fxEmitter;
        this.deadLetterEmitter = deadLetterEmitter;
        this.processingDelayMs = processingDelayMs;
    }

    /**
     * Un tick cada vez.
     *
     * <p>{@code @Blocking} porque el metodo hace trabajo bloqueante (el retardo de simulacion,
     * y el commit que dispara el ack): sin la anotacion, SmallRye lo ejecutaria en el hilo de
     * eventos y lo dejaria todo parado.
     *
     * <p>Devuelve {@code CompletionStage<Void>} porque el metodo recibe un {@code Message}: en
     * SmallRye, quien consume un mensaje tiene que decir cuando termina, y la forma de decirlo
     * es devolver el resultado del {@code ack()}. Es el equivalente exacto del
     * {@code Acknowledgment} de Spring, solo que ahi se llama y aqui se devuelve.
     */
    @Incoming("ticks-raw")
    @Blocking
    public CompletionStage<Void> onTick(KafkaRecord<String, Tick> record) {
        long count = received.incrementAndGet();

        // Payload nulo = el registro no se pudo deserializar. El DltDeserializationFailureHandler
        // ya lo dejo en el DLT con los bytes originales y su x-dlt-reason: aqui solo se confirma
        // el offset para que el grupo siga avanzando (antes esto revocaba las particiones).
        if (record.getPayload() == null) {
            discarded.incrementAndGet();
            log.warn("[DLT] part={} offset={} key={} no deserializable: ya esta en el DLT, se confirma y se sigue",
                    record.getPartition(), offsetDe(record), record.getKey());
            return record.ack();
        }

        String problem = validator.validate(record.getKey(), record.getPayload());

        if (problem != null) {
            sendToDeadLetter(record, problem);
        } else {
            CanonicalTick canonical = toCanonical(record);
            publish(canonical);
            publishFxReference(record.getPayload());
            logCanonical(record, canonical, count);
        }

        simulateSlowProcessing();

        // El commit: a partir de aqui el mensaje no se repetira aunque el proceso se caiga
        // justo despues. Con commit-strategy=latest, este ack es un commit de su offset.
        return record.ack();
    }

    /**
     * Normalizacion: mismo dato, pero como evento canonico con su propio esquema. Ademas de
     * los campos del tick, anade cuando se normalizo y de que particion y offset del crudo
     * salio (trazabilidad).
     */
    /** El offset no esta en {@link KafkaRecord}: viaja en los metadatos del mensaje entrante. */
    private static long offsetDe(KafkaRecord<String, Tick> record) {
        return record.getMetadata(IncomingKafkaRecordMetadata.class)
                .map(IncomingKafkaRecordMetadata::getOffset)
                .orElse(-1L);
    }

    private CanonicalTick toCanonical(KafkaRecord<String, Tick> record) {
        Tick tick = record.getPayload();
        return CanonicalTick.newBuilder()
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
                .setOriginPartition(record.getPartition())
                .setOriginOffset(offsetDe(record))
                .build();
    }

    private void publish(CanonicalTick canonical) {
        // La key sigue siendo el simbolo: el reparto por particion del canonico es el mismo
        // que el del crudo y el orden por simbolo se mantiene.
        canonicalEmitter.sendMessage(KafkaRecord.of(canonical.getSymbol(), canonical))
                .subscribe().with(
                        ignorado -> published.incrementAndGet(),
                        ex -> log.error("[canonico] fallo publicando {}: {}", canonical.getSymbol(), ex.getMessage()));
    }

    /**
     * Un mensaje que no se puede procesar NO se descarta en silencio: se publica al topic de
     * descartes CON EL MOTIVO en una cabecera, copiando las cabeceras originales. Asi se puede
     * mirar que llego mal y por que, y el consumidor sigue avanzando en vez de atascarse.
     */
    private void sendToDeadLetter(KafkaRecord<String, Tick> record, String reason) {
        Headers headers = new RecordHeaders();
        record.getMetadata(IncomingKafkaRecordMetadata.class)
                .ifPresent(meta -> meta.getHeaders().forEach(headers::add));
        headers.add(DLT_REASON_HEADER, reason.getBytes(StandardCharsets.UTF_8));

        OutgoingKafkaRecordMetadata<String> metadata = OutgoingKafkaRecordMetadata.<String>builder()
                .withKey(record.getKey())
                .withHeaders(headers)
                .build();

        deadLetterEmitter.sendMessage(Message.of(record.getPayload()).addMetadata(metadata))
                .subscribe().with(
                        ignorado -> { },
                        ex -> log.error("[DLT] no se pudo enviar {} al DLT: {}", record.getKey(), ex.getMessage()));
        discarded.incrementAndGet();
        log.warn("[DLT] part={} offset={} key={} -> market.ticks.raw.DLT | motivo: {}",
                record.getPartition(), offsetDe(record), record.getKey(), reason);
    }

    /**
     * Los pares de divisas son DATO DE REFERENCIA: ademas del evento canonico se publican a un
     * topic compactado para que otros servicios los lean como tabla (el ultimo tipo de cambio
     * por par) sin reprocesar el stream entero.
     *
     * <p>ponytail: se asume que el par esta cotizado como XXX/USD, es decir, el precio dice
     * cuantos USD vale una unidad de la divisa base. Con pares al reves habria que invertirlo.
     */
    private void publishFxReference(Tick tick) {
        if (tick.getAssetClass() != com.aggora.avro.AssetClass.FX) {
            return;
        }
        FxRate rate = FxRate.newBuilder()
                .setPair(tick.getSymbol())
                .setRate(tick.getPrice())
                .setEventTime(tick.getEventTime())
                .build();
        fxEmitter.sendMessage(KafkaRecord.of(tick.getSymbol(), rate))
                .subscribe().with(
                        ignorado -> { },
                        ex -> log.error("[fx] fallo publicando {}: {}", tick.getSymbol(), ex.getMessage()));
    }

    /** Log resumido: los primeros y uno de cada 500, para no inundar la consola. */
    private void logCanonical(KafkaRecord<String, Tick> record, CanonicalTick canonical, long count) {
        if (count <= 5 || count % 500 == 0) {
            log.info("[canonico] {} {} {} precio={} size={} seq={} fuente={} t={} -> canonical(part={} offset={}) | recibidos={} descartados={} publicados={}",
                    canonical.getSymbol(), canonical.getAssetClass(), canonical.getCurrency(),
                    canonical.getPrice(), canonical.getSize(), canonical.getSequence(),
                    canonical.getSource(), canonical.getEventTime(),
                    record.getPartition(), offsetDe(record),
                    count, discarded.get(), published.get());
        }
    }

    /** Ver {@code aggora.processing-delay-ms}: sirve para poder matar el proceso a media faena. */
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

    public long receivedCount() {
        return received.get();
    }

    public long discardedCount() {
        return discarded.get();
    }
}
