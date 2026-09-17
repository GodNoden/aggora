package com.aggora.normalizer.config;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import com.aggora.normalizer.consumer.RebalanceLogger;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.kafka.autoconfigure.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Fábrica de contenedores de escucha, MAS el manejador de errores que cierra el agujero del DLT.
 *
 * <p>Hace falta una fábrica propia por dos motivos: colgarle el RebalanceLogger, que al ser
 * un callback del cliente y no una property no se puede configurar desde YAML; y colgarle el
 * {@link DefaultErrorHandler} que atiende los fallos de DESERIALIZACION (los de validacion los
 * sigue mandando el propio consumidor a mano, con el mismo topic y la misma cabecera).
 *
 * <p>configurer.configure(...) aplica igualmente todo lo de spring.kafka.listener.*
 * (ack-mode=manual_immediate, concurrency, missing-topics-fatal...), así que la
 * configuración sigue viviendo en application.yml y no aquí.
 *
 * <p>Los genéricos son &lt;Object, Object&gt; porque es lo que exige el configurer de Spring Boot:
 * el tipo real de los mensajes lo fijan los deserializadores del yml, no el bean de la fábrica.
 */
@Configuration
public class KafkaConsumerConfig {

    /** Cabecera donde viaja el motivo del descarte, para poder investigarlo despues. */
    private static final String DLT_REASON_HEADER = "x-dlt-reason";

    @Bean
    public ConcurrentKafkaListenerContainerFactory<Object, Object> kafkaListenerContainerFactory(
            ConsumerFactory<Object, Object> consumerFactory,
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            RebalanceLogger rebalanceLogger,
            DefaultErrorHandler kafkaErrorHandler) {

        ConcurrentKafkaListenerContainerFactory<Object, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, consumerFactory);
        factory.getContainerProperties().setConsumerRebalanceListener(rebalanceLogger);
        factory.setCommonErrorHandler(kafkaErrorHandler);
        return factory;
    }

    /**
     * El manejador que cubre el fallo de deserializacion: cuando ErrorHandlingDeserializer no
     * puede leer un mensaje, el contenedor lanza DeserializationException y esto la recoge.
     *
     * <p>DeserializationException esta en la lista de "no reintentables" de Spring Kafka, asi que
     * un mensaje que no es Avro se manda al DLT AL PRIMERO y el consumidor sigue con el siguiente:
     * nada de reintentos infinitos ni de particion atascada.
     */
    @Bean
    public DefaultErrorHandler kafkaErrorHandler(
            AggoraProperties props,
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {

        // Productor aparte para el DLT: tiene que escribir los BYTES ORIGINALES, y el
        // KafkaTemplate de Avro no sabe serializar un byte[] que no es Avro. Es un productor
        // minimo (String como key, byte[] como valor) y no necesita el registro de esquemas.
        Map<String, Object> productor = new HashMap<>();
        productor.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        productor.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        productor.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        productor.put(ProducerConfig.ACKS_CONFIG, "all");
        KafkaTemplate<String, byte[]> dltTemplate =
                new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(productor));

        // El destino explicito: el default de Spring seria market.ticks.raw-dlt, con guion.
        // Particion -1 = que decida el particionador (el topic tiene una sola particion).
        DeadLetterPublishingRecoverer dlt = new DeadLetterPublishingRecoverer(dltTemplate,
                (record, ex) -> new TopicPartition(props.topics().ticksRawDlt(), -1));
        // La misma cabecera que usa el camino de validacion: quien lea el DLT ve el motivo
        // igual venga el mensaje de donde venga.
        dlt.setHeadersFunction((record, ex) -> {
            Headers headers = new RecordHeaders();
            headers.add(DLT_REASON_HEADER, motivoDelDescarte(ex).getBytes(StandardCharsets.UTF_8));
            return headers;
        });
        // Deja rastro en el log de cada descarte: sin esto el fallo de deserializacion se
        // arreglaba en silencio y en el log no se veia ni que habia pasado.
        dlt.setLogRecoveryRecord(true);

        // Dos reintentos de un segundo para los fallos que si pueden ser transitorios; el
        // fallo de deserializacion no llega aqui: es fatal y va directo al DLT.
        return new DefaultErrorHandler(dlt, new FixedBackOff(1000L, 2L));
    }

    /**
     * Motivo legible para la cabecera x-dlt-reason. Se distingue el fallo de lectura (lo que
     * este manejador arregla) del fallo al procesar, porque no se investigan igual.
     */
    private static String motivoDelDescarte(Exception ex) {
        boolean deserializacion = false;
        Throwable causa = ex;
        while (causa != null) {
            if (causa instanceof DeserializationException) {
                deserializacion = true;
            }
            if (causa.getCause() == null || causa.getCause() == causa) {
                break;
            }
            causa = causa.getCause();
        }
        String detalle = causa.getMessage() == null ? ex.getClass().getSimpleName() : causa.getMessage();
        detalle = detalle.replaceAll("\\s+", " ").trim();
        if (detalle.length() > 160) {
            detalle = detalle.substring(0, 160);
        }
        return deserializacion ? "fallo de deserializacion: " + detalle
                : "error procesando el mensaje: " + detalle;
    }
}
