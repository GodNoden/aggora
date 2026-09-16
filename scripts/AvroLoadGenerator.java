import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import com.aggora.avro.AssetClass;
import com.aggora.avro.Exchange;
import com.aggora.avro.Tick;
import com.aggora.avro.TickSource;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

/**
 * Generador de carga de la Fase 10: publica ticks CRUDOS validos en {@code market.ticks.raw} a una
 * tasa fija, para poder empujar los dos pipelines (Spring y Quarkus) por encima de lo que da el
 * simulador (~84 msg/s) y ver donde se rompe cada uno.
 *
 * <p><b>Por que no vale {@code kafka-producer-perf-test}</b>: manda bytes arbitrarios, y el
 * normalizer tiene un esquema Avro y un validador detras. Un byte cualquiera no es un tick: iria al
 * DLT y lo que se mediria seria el camino del descarte, no el pipeline. Por eso este generador usa
 * **el mismo serializador de Confluent** contra el registro de esquemas y construye ticks que pasan
 * el validador (key == symbol, precio positivo, divisa ISO-4217, timestamp de ahora). La
 * comprobacion de que la medicion es valida la hace el propio test de estres: si el topic
 * {@code market.ticks.raw.DLT} crece, el generador esta mandando basura y el experimento no vale.
 *
 * <p>Uso (dentro del devcontainer, ver {@code scripts/throughput-test.sh}, que lo orquesta):
 *
 * <pre>
 *   java -cp "$(cat /tmp/cp.txt)" scripts/AvroLoadGenerator.java \
 *        --rate 1000 --seconds 60 --symbols 40
 * </pre>
 *
 * <p>Opciones: {@code --topic} (por defecto {@code market.ticks.raw}), {@code --rate} (msg/s),
 * {@code --seconds}, {@code --symbols} (cuantos simbolos distintos; con 6 particiones conviene que
 * sean bastantes mas), {@code --bootstrap} y {@code --registry}.
 */
public class AvroLoadGenerator {

    private static final String BOOTSTRAP_POR_DEFECTO = "kafka-1:9092,kafka-2:9092,kafka-3:9092";
    private static final String REGISTRY_POR_DEFECTO = "http://schema-registry:8081";

    public static void main(String[] args) throws Exception {
        Map<String, String> opciones = new HashMap<>();
        for (int i = 0; i + 1 < args.length; i += 2) {
            opciones.put(args[i].replaceFirst("^--", ""), args[i + 1]);
        }
        String topic = opciones.getOrDefault("topic", "market.ticks.raw");
        int tasa = Integer.parseInt(opciones.getOrDefault("rate", "1000"));
        int segundos = Integer.parseInt(opciones.getOrDefault("seconds", "60"));
        int simbolos = Integer.parseInt(opciones.getOrDefault("symbols", "40"));
        String bootstrap = opciones.getOrDefault("bootstrap", BOOTSTRAP_POR_DEFECTO);
        String registry = opciones.getOrDefault("registry", REGISTRY_POR_DEFECTO);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, registry);
        // Un productor de carga tiene que poder ir MAS rapido que el pipeline que mide; si no, el
        // cuello de botella seria el generador y la comparacion no diria nada.
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.LINGER_MS_CONFIG, "0");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");

        System.out.println("Generador: topic=" + topic + " tasa=" + tasa + " msg/s durante "
                + segundos + " s con " + simbolos + " simbolos");
        long enviados = 0;
        long inicio;
        long ultimoAviso;
        try (KafkaProducer<String, Tick> productor = new KafkaProducer<>(props)) {
            Random azar = new Random(20260916L);
            inicio = System.currentTimeMillis();
            ultimoAviso = inicio;
            long fin = inicio + segundos * 1000L;
            while (System.currentTimeMillis() < fin) {
                long transcurrido = System.currentTimeMillis() - inicio;
                long objetivo = tasa * transcurrido / 1000L;
                if (enviados < objetivo) {
                    productor.send(tick(topic, azar, simbolos, enviados));
                    enviados++;
                } else {
                    // Un milimetro de nada: la tasa es lo que manda, no la CPU del generador.
                    Thread.sleep(1);
                }
                long ahora = System.currentTimeMillis();
                if (ahora - ultimoAviso >= 5000) {
                    System.out.printf("  ... %d enviados (%.0f msg/s reales)%n", enviados,
                            enviados * 1000.0 / (ahora - inicio));
                    ultimoAviso = ahora;
                }
            }
            productor.flush();
        }
        long total = System.currentTimeMillis() - inicio;
        System.out.printf("RESULTADO generador: %d mensajes en %.1f s = %.0f msg/s reales%n",
                enviados, total / 1000.0, enviados * 1000.0 / total);
    }

    /** Un tick que pasa el validador del normalizer: key == symbol, precio positivo, timestamp de ahora. */
    private static ProducerRecord<String, Tick> tick(String topic, Random azar, int cuantosSimbolos, long indice) {
        String symbol = String.format("SYN%03d", azar.nextInt(Math.max(1, cuantosSimbolos)));
        // Paseo aleatorio alrededor de 100: precios con decimales y tamanos variados, como el
        // simulador, para que las ventanas de la analitica hagan el mismo trabajo que en produccion.
        BigDecimal precio = BigDecimal.valueOf(100 + azar.nextGaussian() * 2).setScale(4, RoundingMode.HALF_UP);
        Tick tick = Tick.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setSymbol(symbol)
                .setAssetClass(AssetClass.EQUITY)
                .setExchange(Exchange.NASDAQ)
                .setCurrency("USD")
                .setPrice(precio)
                .setSize(1 + azar.nextInt(500))
                .setEventTime(Instant.now())
                .setSource(TickSource.SYNTHETIC)
                .setSequence(indice)
                .build();
        return new ProducerRecord<>(topic, symbol, tick);
    }
}
