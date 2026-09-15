package com.aggora.audit.consumer;

import java.util.concurrent.atomic.AtomicLong;

import com.aggora.audit.store.AuditStore;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.BackOff;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.stereotype.Component;

/**
 * Escucha los eventos que hay que auditar y los guarda (evento + recado de publicacion,
 * en una sola transaccion de base de datos).
 *
 * Se declara el valor como Object porque los tres topics traen tipos Avro distintos: el
 * deserializador devuelve la clase concreta segun el esquema que traiga el mensaje.
 */
@Component
public class AuditConsumer {

    private static final Logger log = LoggerFactory.getLogger(AuditConsumer.class);

    private final AuditStore store;
    private final AtomicLong stored = new AtomicLong();
    private final AtomicLong duplicates = new AtomicLong();

    public AuditConsumer(AuditStore store) {
        this.store = store;
    }

    /**
     * El patron de RETRY TOPICS, declarativo: si el procesamiento falla, el mensaje no se
     * reintenta en el sitio (eso bloquearia la particion) ni se descarta; se publica a un
     * topic de reintento con espera creciente y, si sigue fallando, al topic de descartes.
     *
     *   orders.incoming -> orders.incoming-retry-0 (0,5 s) -> -retry-1 (1 s) -> .DLT
     *
     * Es lo que hace que una base de datos caida un minuto no se lleve por delante la
     * auditoria: los eventos esperan en los topics de reintento y se procesan al volver.
     */
    @RetryableTopic(
            attempts = "3",
            backOff = @BackOff(delay = 500, multiplier = 2.0),
            retryTopicSuffix = ".retry",
            dltTopicSuffix = ".DLT")
    @KafkaListener(id = "audit-log",
            topics = {
                    "${aggora.topics.executions}",
                    "${aggora.topics.portfolio-updates}",
                    "${aggora.topics.alerts}"
            })
    public void onEvent(ConsumerRecord<String, Object> record) {
        SpecificRecord event = (SpecificRecord) record.value();
        boolean isNew = store.append(record, event, AuditStore.toJson(event));

        long total = isNew ? stored.incrementAndGet() : duplicates.incrementAndGet();
        if (total <= 5 || total % 100 == 0) {
            log.info("[auditoria] {} guardados ({} ya estaban) | ultimo: {} part={} offset={}",
                    stored.get(), duplicates.get(), record.topic(), record.partition(), record.offset());
        }
    }

    /**
     * Lo que acaba aqui ya no se reintenta solo: hay que mirarlo. Se registra con el
     * topic y el offset de origen para poder ir a buscarlo.
     */
    @DltHandler
    public void onDeadLetter(ConsumerRecord<String, Object> record) {
        log.error("[DLT] no se pudo auditar {} part={} offset={}: queda en {} para revisarlo",
                record.topic(), record.partition(), record.offset(), record.topic());
    }

    public long storedCount() {
        return stored.get();
    }
}
