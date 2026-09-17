package com.aggora.normalizer;

import java.nio.charset.StandardCharsets;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import com.aggora.avro.Tick;

import org.apache.kafka.common.header.Headers;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.smallrye.common.annotation.Identifier;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.smallrye.reactive.messaging.kafka.DeserializationFailureHandler;
import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;

/**
 * Cierra el agujero del DLT en Quarkus: cubre el fallo de DESERIALIZACION, que es el que ocurre
 * antes de que el codigo del consumidor vea nada.
 *
 * <p>SmallRye llama a este bean cuando el deserializador del canal no puede leer un registro
 * ({@code mp.messaging.incoming.ticks-raw.value-deserialization-failure-handler}). Aqui se publican
 * los BYTES ORIGINALES en {@code market.ticks.raw.DLT} con la misma cabecera {@code x-dlt-reason}
 * que usa el camino de validacion, y se devuelve {@code null}.
 *
 * <p>Devolver {@code null} es lo que deja al consumidor VIVO: el registro llega al metodo
 * {@code onTick} con payload nulo, se confirma su offset y el grupo sigue con el siguiente. Antes
 * de esto, SmallRye marcaba la aplicacion como unhealthy y el consumidor revocaba las particiones.
 *
 * <p>El envio es asincrono a proposito: este metodo corre en el hilo que hace {@code poll()}, y
 * bloquearlo esperando al DLT pararia el consumo igual que lo paraba el fallo.
 */
@ApplicationScoped
@Identifier("ticks-raw-dlt-deserializacion")
public class DltDeserializationFailureHandler implements DeserializationFailureHandler<Tick> {

    private static final Logger log = LoggerFactory.getLogger(DltDeserializationFailureHandler.class);

    /** Productor de bytes crudos: el DLT tiene que guardar lo que llego, no lo que entendemos. */
    private final MutinyEmitter<byte[]> dltEmitter;

    @Inject
    public DltDeserializationFailureHandler(@Channel("ticks-raw-dlt-bytes") MutinyEmitter<byte[]> dltEmitter) {
        this.dltEmitter = dltEmitter;
    }

    @Override
    public Tick handleDeserializationFailure(String topic, boolean isKey, String deserializer,
                                             byte[] data, Exception exception, Headers headers) {
        String motivo = "fallo de deserializacion: " + detalle(exception);
        if (headers != null) {
            headers.add(TickNormalizer.DLT_REASON_HEADER, motivo.getBytes(StandardCharsets.UTF_8));
        }

        OutgoingKafkaRecordMetadata<String> metadata = OutgoingKafkaRecordMetadata.<String>builder()
                .withHeaders(headers)
                .build();

        byte[] original = data == null ? new byte[0] : data;
        dltEmitter.sendMessage(Message.of(original).addMetadata(metadata))
                .subscribe().with(
                        ignorado -> log.warn("[DLT] mensaje no deserializable (topic={}, eraKey={}) -> market.ticks.raw.DLT | motivo: {}",
                                topic, isKey, motivo),
                        ex -> log.error("[DLT] no se pudo enviar el mensaje no deserializable al DLT: {}", ex.getMessage()));

        // null = "ya lo he atendido, sigue con el siguiente". El payload nulo lo confirma onTick.
        return null;
    }

    /** Mensaje corto y de una linea: la cabecera no es sitio para un volcado de bytes. */
    private static String detalle(Exception exception) {
        Throwable causa = exception;
        while (causa.getCause() != null && causa.getCause() != causa) {
            causa = causa.getCause();
        }
        String mensaje = causa.getMessage() == null ? exception.getClass().getSimpleName() : causa.getMessage();
        mensaje = mensaje.replaceAll("\\s+", " ").trim();
        return mensaje.length() > 160 ? mensaje.substring(0, 160) : mensaje;
    }
}
