package com.aggora.audit.consumer;

import java.util.concurrent.atomic.AtomicLong;

import com.aggora.audit.store.AuditStore;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
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

    public long storedCount() {
        return stored.get();
    }
}
