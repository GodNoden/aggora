package com.aggora.simulator.producer;

import java.util.concurrent.atomic.AtomicLong;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import com.aggora.avro.Tick;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.smallrye.reactive.messaging.MutinyEmitter;
import io.smallrye.reactive.messaging.kafka.KafkaRecord;

/**
 * Envio de ticks a Kafka: el equivalente del {@code KafkaTemplate} de Spring.
 *
 * <p>Asincrono y sin bloquear (como en Spring): el productor manda y sigue generando ticks. El
 * productor es idempotente ({@code enable.idempotence=true} en el yaml) y {@code acks=all}, de
 * modo que un reintento por fallo transitorio NO duplica el mensaje.
 *
 * <p><b>Una diferencia real que se nota al portar:</b> con el {@code KafkaTemplate} de Spring,
 * el callback del envio recibe un {@code SendResult} con la particion y el offset que le toco al
 * mensaje, y se pueden registrar. Con un {@code Emitter} de SmallRye solo hay un
 * {@code CompletionStage<Void>}: no hay resultado que mirar. El log de cada 1000 ticks puede
 * contar cuantos van y cuanto ha fallado, pero ya no puede decir en que particion cayeron. Es
 * una de esas cosas que solo se ven cuando se hacen las dos versiones.
 */
@ApplicationScoped
public class TickEmitter {

    private static final Logger log = LoggerFactory.getLogger(TickEmitter.class);

    private final MutinyEmitter<Tick> emitter;
    private final AtomicLong sent = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();

    @Inject
    public TickEmitter(@Channel("ticks-raw") MutinyEmitter<Tick> emitter) {
        this.emitter = emitter;
    }

    public void send(Tick tick) {
        // La key es el simbolo, igual que en la version Spring: el reparto por particion y el
        // orden por instrumento dependen de eso.
        emitter.sendMessage(KafkaRecord.of(tick.getSymbol(), tick))
                .subscribe().with(
                        ignorado -> {
                            long total = sent.incrementAndGet();
                            if (total % 1000 == 0) {
                                log.info("[produce] {} ticks enviados | ultimo: {} | fallos={}",
                                        total, tick.getSymbol(), failed.get());
                            }
                        },
                        ex -> {
                            failed.incrementAndGet();
                            log.error("[produce] fallo enviando {}: {}", tick.getSymbol(), ex.getMessage());
                        });
    }

    public long sentCount() {
        return sent.get();
    }

    public long failedCount() {
        return failed.get();
    }
}
