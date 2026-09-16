package com.aggora.normalizer.lambda;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.Currency;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import com.aggora.avro.Tick;
import com.aggora.avro.canonical.CanonicalTick;
import com.aggora.avro.reference.FxRate;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.KafkaEvent;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * El normalizer de Aggora como funcion AWS Lambda (la version serverless de la Fase 10).
 *
 * <p><b>No hay bucle de consumo.</b> Con un <i>event source mapping</i> de Kafka, quien hace
 * el polling es Lambda: agrupa los mensajes de una o varias particiones en un lote y llama a
 * {@link #handleRequest} con el. Por eso esta clase no implementa nada de consumidor: recorre
 * los {@link KafkaEvent.KafkaEventRecord} del lote y devuelve una cadena (el valor de retorno
 * de una funcion con event source mapping de Kafka se ignora, salvo en los fallos parciales
 * que aqui no se usan, ver mas abajo).
 *
 * <p><b>El value llega en base64</b> (Lambda no manda bytes en JSON), asi que se decodifica y
 * se deserializa con {@link KafkaAvroDeserializer} en modo <i>specific</i> contra el Schema
 * Registry: el resultado es un {@link Tick}.
 *
 * <p><b>Las reglas de validacion son una copia a proposito</b> de
 * {@code services/quarkus/ingestion-normalizer/src/main/java/com/aggora/normalizer/TickValidator.java}
 * (que a su vez es las mismas que el {@code validate} de
 * {@code services/spring/ingestion-normalizer/.../TickConsumer.java}). El proyecto decidio no
 * tener un modulo comun, asi que el port se copia; si algun dia las reglas se separan, los
 * tests de los tres arboles son los que lo dicen.
 *
 * <p><b>Un mensaje valido</b> se publica como {@link CanonicalTick} en el topic de salida, con
 * la misma construccion que los otros normalizers (mismos campos, mismos nombres de enum
 * canonicos y la particion/offset de origen para la trazabilidad). El productor es
 * <b>idempotente</b> ({@code acks=all}, {@code enable.idempotence=true}) y se crea una sola vez
 * en un campo estatico: el contenedor de Lambda se reutiliza mientras sigue caliente, y el
 * productor sobrevive entre invocaciones en vez de abrir conexiones nuevas cada vez.
 *
 * <p><b>Un mensaje invalido, o uno que no se puede ni deserializar, va al topic {@code .DLT}</b>
 * con el motivo en la cabecera {@code x-dlt-reason} (el mismo nombre que usan los otros
 * normalizers) y <b>NO se lanza ninguna excepcion</b>. Esto es lo importante: si un mensaje
 * venenoso tumbara la invocacion, Lambda reintentaria el lote ENTERO y el resto de mensajes
 * buenos se reprocesaria (o el lote se quedaria atascado para siempre). Al DLT se manda el
 * registro ORIGINAL, byte a byte tal y como llego, porque puede no ser ni un Avro valido y no
 * se puede reconstruir.
 *
 * <p><b>Solo se propaga la excepcion cuando falla la INFRAESTRUCTURA</b> (no se puede conectar
 * al broker, no se puede publicar): ahi si interesa que Lambda reintente el lote, y para eso
 * esta la <i>on-failure destination</i> (la cola SQS que crea la infraestructura; no se
 * configura aqui).
 *
 * <p><b>No se implementa {@code ReportBatchItemFailures}</b>: no se ha verificado el formato
 * exacto del {@code itemIdentifier} para Kafka, asi que no se afirma nada. La proteccion
 * contra mensajes venenosos en esta funcion es el DLT, no el fallo parcial: el DLT saca el
 * mensaje malo del camino sin lanzar, y por eso no hace falta decirle a Lambda cual fallo.
 *
 * <p>La configuracion entra por variables de entorno (en Lambda no hay application.yml):
 * {@code BOOTSTRAP_SERVERS}, {@code SCHEMA_REGISTRY_URL}, {@code TOPIC_CANONICAL},
 * {@code TOPIC_DLT} y las opcionales de seguridad {@code KAFKA_SECURITY_PROTOCOL},
 * {@code KAFKA_SASL_MECHANISM} y {@code KAFKA_SASL_JAAS_CONFIG}; si las de seguridad no
 * vienen, no se configura nada y el cliente usa PLAINTEXT.
 */
public class AggoraNormalizerHandler implements RequestHandler<KafkaEvent, String> {

    private static final Logger log = LoggerFactory.getLogger(AggoraNormalizerHandler.class);

    /** Cabecera donde viaja el motivo del descarte, igual que en los otros normalizers. */
    static final String DLT_REASON_HEADER = "x-dlt-reason";

    private static final String TOPIC_CANONICAL_POR_DEFECTO = "market.ticks.canonical";
    private static final String TOPIC_DLT_POR_DEFECTO = "market.ticks.raw.DLT";
    private static final String TOPIC_FX_POR_DEFECTO = "market.fx.reference";
    private static final String BOOTSTRAP_POR_DEFECTO = "kafka-1:9092,kafka-2:9092,kafka-3:9092";
    private static final String REGISTRY_POR_DEFECTO = "http://schema-registry:8081";

    /**
     * El deserializador de Avro, creado la primera vez que hace falta y reutilizado en las
     * invocaciones siguientes del mismo contenedor caliente.
     */
    private static KafkaAvroDeserializer deserializador;

    /** Lo unico que el handler necesita del mundo exterior, para poder probarlo sin Kafka. */
    interface TickSink {

        /** Publica el evento canonico en el topic de salida, con la key que se le pase. */
        void publicar(String key, CanonicalTick tick);

        /** Publica el registro original en el topic de descartes con el motivo del descarte. */
        void publicarDeadLetter(String key, byte[] valorOriginal, String motivo);

        /**
         * Publica el tipo de cambio de referencia en el topic compactado. Solo lo usan los pares
         * de divisas: es el mismo dato que el evento canonico, pero leido como tabla.
         */
        void publicarFx(String pair, FxRate rate);
    }

    private final TickSink sink;

    /**
     * El constructor que usa Lambda: el sink real, que habla con Kafka. No abre ninguna
     * conexion aqui; los productores se crean de forma perezosa en el primer envio.
     */
    public AggoraNormalizerHandler() {
        this(new KafkaTickSink());
    }

    /** Constructor de paquete para los tests: el sink de mentira apunta lo que se publica. */
    AggoraNormalizerHandler(TickSink sink) {
        this.sink = sink;
    }

    /**
     * Procesa el lote entero. Devuelve un resumen en texto: Lambda lo ignora en un event
     * source mapping de Kafka, pero queda en el log de la invocacion.
     */
    @Override
    public String handleRequest(KafkaEvent evento, Context context) {
        if (evento == null || evento.getRecords() == null) {
            return "procesados=0";
        }
        int procesados = 0;
        for (var entrada : evento.getRecords().entrySet()) {
            for (KafkaEvent.KafkaEventRecord registro : entrada.getValue()) {
                procesar(registro);
                procesados++;
            }
        }
        return "procesados=" + procesados;
    }

    /**
     * Un registro: decodifica el value, lo deserializa, lo valida y lo manda al canonico o al
     * DLT. Ni la base64 rota ni el Avro ilegible lanzan: los dos acaban en el DLT con motivo.
     */
    private void procesar(KafkaEvent.KafkaEventRecord registro) {
        String key = clave(registro);
        byte[] valorOriginal = null;
        Tick tick = null;
        String motivo = null;

        try {
            valorOriginal = registro.getValue() == null
                    ? null
                    : Base64.getDecoder().decode(registro.getValue());
            tick = valorOriginal == null
                    ? null
                    : (Tick) deserializador().deserialize(registro.getTopic(), valorOriginal);
        } catch (RuntimeException ex) {
            motivo = "no se pudo deserializar el value: " + ex.getMessage();
        }

        if (motivo == null) {
            motivo = validar(key, tick);
        }

        if (motivo != null) {
            // El registro original va tal cual al DLT, con el motivo en la cabecera. Nada
            // se propaga: un mensaje venenoso no puede tumbar el lote.
            sink.publicarDeadLetter(key, valorOriginal, motivo);
            log.warn("[DLT] topic={} part={} offset={} key={} | motivo: {}",
                    registro.getTopic(), registro.getPartition(), registro.getOffset(), key, motivo);
            return;
        }

        sink.publicar(tick.getSymbol(), aCanonico(tick, registro));
        publicarReferenciaFx(tick);
    }

    /**
     * Los pares de divisas son DATO DE REFERENCIA: ademas del evento canonico se publican al
     * topic compactado, para que quien lo necesite lo lea como tabla (el ultimo tipo de cambio
     * por par) sin reprocesar el stream entero. Copiado del normalizer de Spring, incluida la
     * suposicion de que el par cotiza como XXX/USD.
     */
    private void publicarReferenciaFx(Tick tick) {
        if (tick.getAssetClass() != com.aggora.avro.AssetClass.FX) {
            return;
        }
        sink.publicarFx(tick.getSymbol(), FxRate.newBuilder()
                .setPair(tick.getSymbol())
                .setRate(tick.getPrice())
                .setEventTime(tick.getEventTime())
                .build());
    }

    /**
     * La normalizacion: mismo dato con el esquema canonico, mas cuando se normalizo y de que
     * particion/offset del crudo salio. Copiada de los otros dos normalizers para que los tres
     * produzcan exactamente el mismo evento.
     */
    private static CanonicalTick aCanonico(Tick tick, KafkaEvent.KafkaEventRecord registro) {
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
                .setOriginPartition(registro.getPartition())
                .setOriginOffset(registro.getOffset())
                .build();
    }

    /**
     * Las reglas EXACTAS del {@code TickValidator} de Quarkus (y del {@code validate} de
     * Spring): mismo motivo, mismo texto y en el mismo orden. Es una copia a proposito porque
     * el proyecto no tiene modulo comun. Devuelve el motivo, o null si el tick es valido.
     */
    private static String validar(String key, Tick tick) {
        if (tick == null) {
            return "payload nulo";
        }
        if (tick.getSymbol() == null || tick.getSymbol().isBlank()) {
            return "symbol vacio";
        }
        if (key != null && !key.equals(tick.getSymbol())) {
            return "la key (" + key + ") no coincide con el symbol (" + tick.getSymbol() + ")";
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

    /**
     * La key de Kafka tambien viaja en base64 en el evento de Lambda. Si no lo fuera (un
     * evento fabricado a mano), se usa el texto tal cual: la key no puede tumbar el lote.
     */
    private static String clave(KafkaEvent.KafkaEventRecord registro) {
        if (registro.getKey() == null) {
            return null;
        }
        try {
            return new String(Base64.getDecoder().decode(registro.getKey()), StandardCharsets.UTF_8);
        } catch (IllegalArgumentException ex) {
            return registro.getKey();
        }
    }

    private static synchronized KafkaAvroDeserializer deserializador() {
        if (deserializador == null) {
            KafkaAvroDeserializer nuevo = new KafkaAvroDeserializer();
            Map<String, Object> config = new HashMap<>(seguridad());
            config.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                    configuracion("SCHEMA_REGISTRY_URL", REGISTRY_POR_DEFECTO));
            config.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
            nuevo.configure(config, false);
            deserializador = nuevo;
        }
        return deserializador;
    }

    /** En Lambda no hay application.yml: la configuracion son variables de entorno. */
    private static String configuracion(String nombre, String porDefecto) {
        String valor = System.getenv(nombre);
        return valor == null || valor.isBlank() ? porDefecto : valor;
    }

    /**
     * Las opcionales de seguridad, tal cual, para el productor y el deserializador. Un broker
     * gestionado exige SASL; si no vienen, no se pone nada y el cliente usa PLAINTEXT.
     */
    private static Map<String, Object> seguridad() {
        Map<String, Object> props = new HashMap<>();
        String protocolo = System.getenv("KAFKA_SECURITY_PROTOCOL");
        if (protocolo != null && !protocolo.isBlank()) {
            props.put("security.protocol", protocolo);
        }
        String mecanismo = System.getenv("KAFKA_SASL_MECHANISM");
        if (mecanismo != null && !mecanismo.isBlank()) {
            props.put("sasl.mechanism", mecanismo);
        }
        // Las credenciales llegan separadas (usuario y clave) porque asi las guarda el
        // despliegue; la cadena JAAS que espera el cliente de Kafka se monta aqui. Si alguien
        // prefiere pasarla ya montada, KAFKA_SASL_JAAS_CONFIG manda.
        String jaas = System.getenv("KAFKA_SASL_JAAS_CONFIG");
        if (jaas == null || jaas.isBlank()) {
            String usuario = System.getenv("KAFKA_SASL_USERNAME");
            String clave = System.getenv("KAFKA_SASL_PASSWORD");
            if (usuario != null && !usuario.isBlank() && clave != null && !clave.isBlank()) {
                // El modulo JAAS depende del mecanismo: PLAIN y SCRAM no usan el mismo.
                String modulo = mecanismo != null && mecanismo.contains("SCRAM")
                        ? "org.apache.kafka.common.security.scram.ScramLoginModule"
                        : "org.apache.kafka.common.security.plain.PlainLoginModule";
                jaas = modulo + " required username=\"" + usuario + "\" password=\"" + clave + "\";";
            }
        }
        if (jaas != null && !jaas.isBlank()) {
            props.put("sasl.jaas.config", jaas);
        }
        return props;
    }

    /**
     * El sink real: dos productores Kafka idempotentes. Son campos estaticos y se crean la
     * primera vez que se publica, para que sobrevivan a las invocaciones mientras el
     * contenedor siga caliente.
     */
    private static final class KafkaTickSink implements TickSink {

        // Los nombres son los del contrato de despliegue (deploy/terraform/main.tf y
        // deploy/README.md): si se cambia uno aqui hay que cambiarlo alli, y al reves.
        private final String topicCanonical = configuracion("KAFKA_TARGET_TOPIC", TOPIC_CANONICAL_POR_DEFECTO);
        private final String topicDlt = configuracion("KAFKA_DEAD_LETTER_TOPIC", TOPIC_DLT_POR_DEFECTO);
        private final String topicFx = configuracion("KAFKA_FX_REFERENCE_TOPIC", TOPIC_FX_POR_DEFECTO);

        private static KafkaProducer<String, CanonicalTick> productorCanonico;
        private static KafkaProducer<String, byte[]> productorDlt;
        private static KafkaProducer<String, FxRate> productorFx;

        @Override
        public void publicar(String key, CanonicalTick tick) {
            enviar(productorCanonico(), new ProducerRecord<>(topicCanonical, key, tick), topicCanonical);
        }

        @Override
        public void publicarFx(String pair, FxRate rate) {
            enviar(productorFx(), new ProducerRecord<>(topicFx, pair, rate), topicFx);
        }

        @Override
        public void publicarDeadLetter(String key, byte[] valorOriginal, String motivo) {
            ProducerRecord<String, byte[]> registro = new ProducerRecord<>(topicDlt, key, valorOriginal);
            registro.headers().add(new RecordHeader(DLT_REASON_HEADER, motivo.getBytes(StandardCharsets.UTF_8)));
            enviar(productorDlt(), registro, topicDlt);
        }

        /**
         * Se espera al resultado del envio a proposito: si el broker no esta, la excepcion
         * sube y Lambda reintenta el lote (on-failure destination). Mandar y no mirar seria
         * perder el mensaje en silencio.
         */
        private static <T> void enviar(KafkaProducer<String, T> productor,
                                       ProducerRecord<String, T> registro, String topic) {
            try {
                productor.send(registro).get();
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("envio a " + topic + " interrumpido", ex);
            } catch (ExecutionException ex) {
                throw new IllegalStateException("no se pudo publicar en " + topic, ex.getCause());
            }
        }

        private static synchronized KafkaProducer<String, CanonicalTick> productorCanonico() {
            if (productorCanonico == null) {
                Properties props = configuracionProductor();
                props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
                productorCanonico = new KafkaProducer<>(props);
            }
            return productorCanonico;
        }

        private static synchronized KafkaProducer<String, FxRate> productorFx() {
            if (productorFx == null) {
                Properties props = configuracionProductor();
                props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
                productorFx = new KafkaProducer<>(props);
            }
            return productorFx;
        }

        private static synchronized KafkaProducer<String, byte[]> productorDlt() {
            if (productorDlt == null) {
                Properties props = configuracionProductor();
                // Al DLT se manda el registro original BYTE A BYTE: puede no ser un Avro
                // valido, asi que no se puede volver a serializar con el serializador Avro.
                props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
                productorDlt = new KafkaProducer<>(props);
            }
            return productorDlt;
        }

        private static Properties configuracionProductor() {
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                    configuracion("KAFKA_BOOTSTRAP_SERVERS", BOOTSTRAP_POR_DEFECTO));
            props.put(ProducerConfig.ACKS_CONFIG, "all");
            props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                    configuracion("SCHEMA_REGISTRY_URL", REGISTRY_POR_DEFECTO));
            props.putAll(seguridad());
            return props;
        }
    }
}
