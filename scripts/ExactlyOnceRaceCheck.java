import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Comprueba, contra un cluster de verdad, la semantica de transacciones en la que se apoya el test
 * de integracion {@code ExactlyOnceKafkaIT} de order-matching-engine. Existe por un motivo muy
 * concreto: ese IT fallo en CI dos veces con el mismo sintoma y el primer diagnostico ("es una
 * carrera al leer") era falso. La causa real es que {@code send()} es asincrono y
 * {@code abortTransaction()} DESCARTA lo que el hilo emisor todavia no ha mandado, asi que un
 * registro abortado puede no llegar a existir en el log.
 *
 * <p>Este programa mide las dos secuencias contra el cluster local, sin Testcontainers (que en el
 * devcontainer del proyecto no arranca, porque el socket de Docker Desktop no es el del motor):
 *
 * <pre>
 *   VIEJA (lo que hacia el test):   send + abortTransaction inmediato
 *   NUEVA (el arreglo del test):    send + esperar a VERLO con read_uncommitted + abort
 * </pre>
 *
 * <p>Salida esperada en una maquina normal: la vieja falla casi siempre (medido 8 de 8) y la nueva
 * pasa siempre. Si la vieja empieza a pasar a menudo, es que la maquina da tiempo al hilo emisor y
 * el hallazgo es que la carrera tiene menos holgura de la que se creia.
 *
 * <p>Como se ejecuta (dentro del devcontainer, con el cluster local levantado):
 *
 * <pre>
 *   cd services
 *   mvn -q -pl spring/order-matching-engine dependency:build-classpath -Dmdep.outputFile=/tmp/cp.txt
 *   cd ..
 *   java -cp "$(cat /tmp/cp.txt)" scripts/ExactlyOnceRaceCheck.java [brokers] [repeticiones]
 * </pre>
 *
 * <p>Sale con codigo 1 si la secuencia NUEVA falla alguna vez: eso si es un problema (seria una
 * regresion en la semantica de la que depende el test o en el propio Kafka).
 */
public class ExactlyOnceRaceCheck {

    private static final String BROKERS_POR_DEFECTO = "kafka-1:9092,kafka-2:9092,kafka-3:9092";

    /**
     * Kafka cuenta TODO lo que hace en INFO (volcados de configuracion incluidos) y tapa el
     * resultado. Se le dice a logback que solo saque WARN, y hay que hacerlo antes de que se cree
     * el primer logger: de ahi que sea un bloque estatico y no una linea dentro de main.
     */
    static {
        try {
            java.nio.file.Path silencio = java.nio.file.Files.createTempFile("aggora-logback", ".xml");
            java.nio.file.Files.writeString(silencio,
                    "<configuration><root level=\"WARN\"/></configuration>");
            System.setProperty("logback.configurationFile", silencio.toString());
        } catch (java.io.IOException ignorado) {
            // Si no se puede, se vera mas ruido y ya esta: el resultado sigue saliendo al final.
        }
    }

    public static void main(String[] args) throws Exception {
        String brokers = args.length > 0 ? args[0] : BROKERS_POR_DEFECTO;
        int repeticiones = args.length > 1 ? Integer.parseInt(args[1]) : 5;

        System.out.println("Cluster: " + brokers + " | repeticiones: " + repeticiones);
        int fallosVieja = 0;
        int fallosNueva = 0;
        for (int i = 1; i <= repeticiones; i++) {
            boolean viejaOk = secuenciaVieja(brokers, i);
            boolean nuevaOk = secuenciaNueva(brokers, i);
            if (!viejaOk) {
                fallosVieja++;
            }
            if (!nuevaOk) {
                fallosNueva++;
            }
        }

        System.out.println();
        System.out.println("--- RESUMEN");
        System.out.println("  VIEJA (send + abort inmediato): " + fallosVieja + "/" + repeticiones
                + " intentos en los que la abortada NO llego al log");
        System.out.println("  NUEVA (send + esperar + abort): " + fallosNueva + "/" + repeticiones
                + " intentos en los que la abortada NO llego al log");
        if (fallosNueva > 0) {
            System.err.println("FALLO: con la secuencia nueva la abortada tiene que estar SIEMPRE en el log.");
            System.exit(1);
        }
        System.out.println("OK: la secuencia del test es determinista; la vieja era la que no probaba nada.");
    }

    /**
     * La secuencia que tenia el test: publicar y abortar sin mas. El registro suele quedarse en el
     * bufer del productor y abortar lo descarta, asi que no se puede afirmar que este en el log.
     */
    private static boolean secuenciaVieja(String brokers, int vuelta) throws Exception {
        String topic = nuevoTopic(brokers);
        try (KafkaProducer<String, String> productor = productor(brokers)) {
            productor.initTransactions();
            productor.beginTransaction();
            productor.send(new ProducerRecord<>(topic, "ASML", "confirmada"));
            productor.commitTransaction();

            productor.beginTransaction();
            productor.send(new ProducerRecord<>(topic, "AAPL", "abortada"));
            productor.abortTransaction();
        }
        List<String> vistos = leer(brokers, topic, "read_uncommitted", 2, Duration.ofSeconds(5));
        boolean ok = vistos.contains("AAPL");
        System.out.printf("  vieja %d: %s  read_uncommitted=%s%n", vuelta, ok ? "ok   " : "FALLA", vistos);
        return ok;
    }

    /**
     * La secuencia del test arreglado: antes de abortar se comprueba que el registro esta en el log.
     * read_uncommitted no filtra por transaccion, asi que ve los registros de una transaccion
     * abierta; en cuanto aparece, esta escrito, y abortar a partir de ahi solo puede esconderlo.
     */
    private static boolean secuenciaNueva(String brokers, int vuelta) throws Exception {
        String topic = nuevoTopic(brokers);
        try (KafkaProducer<String, String> productor = productor(brokers)) {
            productor.initTransactions();
            productor.beginTransaction();
            productor.send(new ProducerRecord<>(topic, "ASML", "confirmada"));
            productor.commitTransaction();

            productor.beginTransaction();
            productor.send(new ProducerRecord<>(topic, "AAPL", "abortada"));
            boolean enElLog = leer(brokers, topic, "read_uncommitted", 2, Duration.ofSeconds(15)).contains("AAPL");
            productor.abortTransaction();
            if (!enElLog) {
                System.out.printf("  nueva %d: FALLA  la abortada no llego al log con la transaccion abierta%n", vuelta);
                return false;
            }
        }
        List<String> confirmados = leer(brokers, topic, "read_committed", 1, Duration.ofSeconds(15));
        List<String> todos = leer(brokers, topic, "read_uncommitted", 2, Duration.ofSeconds(15));
        boolean ok = confirmados.equals(List.of("ASML")) && todos.containsAll(List.of("ASML", "AAPL"));
        System.out.printf("  nueva %d: %s  read_committed=%s  read_uncommitted=%s%n",
                vuelta, ok ? "ok   " : "FALLA", confirmados, todos);
        return ok;
    }

    private static String nuevoTopic(String brokers) throws ExecutionException, InterruptedException {
        String topic = "prueba.transacciones." + System.nanoTime();
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", brokers))) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get();
        }
        return topic;
    }

    private static KafkaProducer<String, String> productor(String brokers) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "prueba-" + UUID.randomUUID());
        return new KafkaProducer<>(props);
    }

    /** Lee hasta ver {@code esperados} claves (o hasta que se agote el tiempo), con ese aislamiento. */
    private static List<String> leer(String brokers, String topic, String aislamiento,
                                     int esperados, Duration tope) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "prueba-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, aislamiento);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        List<String> claves = new ArrayList<>();
        long limite = System.currentTimeMillis() + tope.toMillis();
        try (KafkaConsumer<String, String> consumidor = new KafkaConsumer<>(props)) {
            consumidor.subscribe(List.of(topic));
            while (claves.size() < esperados && System.currentTimeMillis() < limite) {
                for (ConsumerRecord<String, String> registro : consumidor.poll(Duration.ofMillis(300))) {
                    claves.add(registro.key());
                }
            }
        }
        return claves;
    }
}
