package com.aggora.normalizer.lambda;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.aggora.avro.AssetClass;
import com.aggora.avro.Exchange;
import com.aggora.avro.Tick;
import com.aggora.avro.TickSource;
import com.aggora.avro.canonical.CanonicalTick;
import com.aggora.avro.reference.FxRate;

import com.amazonaws.services.lambda.runtime.events.KafkaEvent;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * El handler, probado SIN broker y SIN Docker: el sink es de mentira y el Schema Registry es
 * el "mock://" en memoria que el pom pasa por SCHEMA_REGISTRY_URL (el mismo mecanismo que usan
 * los tests de topologia del repo). La serializacion del test y la deserializacion del handler
 * comparten ese registry porque el scope del mock es el mismo.
 */
class AggoraNormalizerHandlerTest {

    private static final String TOPIC_RAW = "market.ticks.raw";

    /** El mismo scope que el pom pone en SCHEMA_REGISTRY_URL. */
    private static final String REGISTRY = "mock://aggora-lambda-test";

    private final SinkFalso sink = new SinkFalso();
    private final AggoraNormalizerHandler handler = new AggoraNormalizerHandler(sink);

    @Test
    @DisplayName("Un tick valido se publica en el canonico y no va al DLT")
    void un_tick_valido_va_al_canonico() {
        handler.handleRequest(lote(registro("AAPL", tick("AAPL", "100.2500"))), null);

        assertThat(sink.canonicos).hasSize(1);
        CanonicalTick canonico = sink.canonicos.get(0);
        assertThat(canonico.getSymbol()).isEqualTo("AAPL");
        assertThat(canonico.getPrice()).isEqualByComparingTo("100.2500");
        assertThat(canonico.getSize()).isEqualTo(100);
        // La trazabilidad del crudo: la particion y el offset del registro de origen.
        assertThat(canonico.getOriginPartition()).isEqualTo(3);
        assertThat(canonico.getOriginOffset()).isEqualTo(42L);
        assertThat(sink.dltMotivos).isEmpty();
    }

    @Test
    @DisplayName("Un par de divisas se publica tambien como referencia, y una accion no")
    void los_pares_de_divisas_van_al_topic_compactado() {
        handler.handleRequest(lote(
                registro("AAPL", tick("AAPL", "100.2500")),
                registro("EUR/USD", tickFx("EUR/USD", "1.0875"))), null);

        // Las dos van al canonico, pero solo el par genera dato de referencia.
        assertThat(sink.canonicos).extracting(CanonicalTick::getSymbol).containsExactly("AAPL", "EUR/USD");
        assertThat(sink.tiposDeCambio).hasSize(1);
        assertThat(sink.tiposDeCambio.get(0).getPair()).isEqualTo("EUR/USD");
        assertThat(sink.tiposDeCambio.get(0).getRate()).isEqualByComparingTo("1.0875");
    }

    @Test
    @DisplayName("Un tick invalido (precio no positivo) va al DLT con motivo y no al canonico")
    void un_tick_invalido_va_al_dlt() {
        Tick tick = tick("AAPL", "-1.0000");

        handler.handleRequest(lote(registro("AAPL", tick)), null);

        assertThat(sink.canonicos).isEmpty();
        assertThat(sink.dltClaves).containsExactly("AAPL");
        // Una regla REAL del validador, palabra por palabra.
        assertThat(sink.dltMotivos).containsExactly("precio ausente o no positivo");
        // Al DLT va el registro ORIGINAL, byte a byte.
        assertThat(sink.dltValores.get(0)).isEqualTo(serializar(tick));
    }

    @Test
    @DisplayName("Un value que no es Avro valido va al DLT y el handler NO lanza")
    void un_value_que_no_es_avro_va_al_dlt() {
        KafkaEvent.KafkaEventRecord basura = KafkaEvent.KafkaEventRecord.builder()
                .withTopic(TOPIC_RAW)
                .withPartition(0)
                .withOffset(7L)
                .withKey(base64("AAPL".getBytes(StandardCharsets.UTF_8)))
                .withValue(base64("esto no es avro valido".getBytes(StandardCharsets.UTF_8)))
                .build();

        assertThatCode(() -> handler.handleRequest(lote(basura), null)).doesNotThrowAnyException();

        assertThat(sink.canonicos).isEmpty();
        assertThat(sink.dltClaves).containsExactly("AAPL");
        assertThat(sink.dltMotivos).hasSize(1);
        assertThat(sink.dltMotivos.get(0)).startsWith("no se pudo deserializar el value");
    }

    @Test
    @DisplayName("En un lote, cada registro acaba donde le toca")
    void en_un_lote_cada_registro_va_a_su_sitio() {
        handler.handleRequest(lote(
                registro("AAPL", tick("AAPL", "100.0000")),
                registro("MSFT", tick("MSFT", "-5.0000")),
                KafkaEvent.KafkaEventRecord.builder()
                        .withTopic(TOPIC_RAW)
                        .withPartition(1)
                        .withOffset(9L)
                        .withKey(base64("TSLA".getBytes(StandardCharsets.UTF_8)))
                        .withValue(base64("tampoco es avro".getBytes(StandardCharsets.UTF_8)))
                        .build()), null);

        assertThat(sink.canonicos).extracting(CanonicalTick::getSymbol).containsExactly("AAPL");
        assertThat(sink.dltClaves).containsExactly("MSFT", "TSLA");
    }

    /** Un sink de mentira: apunta lo publicado en vez de hablar con Kafka. */
    private static final class SinkFalso implements AggoraNormalizerHandler.TickSink {

        private final List<CanonicalTick> canonicos = new ArrayList<>();
        private final List<FxRate> tiposDeCambio = new ArrayList<>();
        private final List<String> dltClaves = new ArrayList<>();
        private final List<byte[]> dltValores = new ArrayList<>();
        private final List<String> dltMotivos = new ArrayList<>();

        @Override
        public void publicar(String key, CanonicalTick tick) {
            canonicos.add(tick);
        }

        @Override
        public void publicarFx(String pair, FxRate rate) {
            tiposDeCambio.add(rate);
        }

        @Override
        public void publicarDeadLetter(String key, byte[] valorOriginal, String motivo) {
            dltClaves.add(key);
            dltValores.add(valorOriginal);
            dltMotivos.add(motivo);
        }
    }

    /**
     * Serializa el Tick con el MISMO serializador que usan los otros arboles, contra el
     * Schema Registry "mock://" compartido con el deserializador del handler.
     */
    private static byte[] serializar(Tick tick) {
        KafkaAvroSerializer serializer = new KafkaAvroSerializer();
        serializer.configure(Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, REGISTRY), false);
        return serializer.serialize(TOPIC_RAW, tick);
    }

    /** Un registro del lote tal y como lo entrega un event source mapping de Kafka: base64. */
    private static KafkaEvent.KafkaEventRecord registro(String key, Tick tick) {
        return KafkaEvent.KafkaEventRecord.builder()
                .withTopic(TOPIC_RAW)
                .withPartition(3)
                .withOffset(42L)
                .withKey(base64(key.getBytes(StandardCharsets.UTF_8)))
                .withValue(base64(serializar(tick)))
                .build();
    }

    private static KafkaEvent lote(KafkaEvent.KafkaEventRecord... registros) {
        return KafkaEvent.builder().withRecords(Map.of(TOPIC_RAW, List.of(registros))).build();
    }

    private static String base64(byte[] bytes) {
        return Base64.getEncoder().encodeToString(bytes);
    }

    /** Un par de divisas, que ademas de ir al canonico genera dato de referencia. */
    private static Tick tickFx(String symbol, String precio) {
        return Tick.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setSymbol(symbol)
                .setAssetClass(AssetClass.FX)
                .setExchange(Exchange.FX)
                .setCurrency("USD")
                .setPrice(new BigDecimal(precio))
                .setSize(100)
                .setEventTime(Instant.now())
                .setSource(TickSource.SYNTHETIC)
                .setSequence(1L)
                .build();
    }

    private static Tick tick(String symbol, String precio) {
        return Tick.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setSymbol(symbol)
                .setAssetClass(AssetClass.EQUITY)
                .setExchange(Exchange.NASDAQ)
                .setCurrency("USD")
                .setPrice(new BigDecimal(precio))
                .setSize(100)
                .setEventTime(Instant.now())
                .setSource(TickSource.SYNTHETIC)
                .setSequence(1L)
                .build();
    }
}
