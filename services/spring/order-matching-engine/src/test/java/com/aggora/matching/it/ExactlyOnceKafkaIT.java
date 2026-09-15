package com.aggora.matching.it;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.aggora.avro.orders.Execution;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * El test de integracion que importa de esta fase: **exactly-once contra un broker de verdad**.
 *
 * <p>Es el experimento de la Fase 4, pero automatizado y con Kafka y el Schema Registry levantados
 * por Testcontainers: se publica una ejecucion en una transaccion que se confirma y otra en una
 * transaccion que se ABORTA. Los dos mensajes estan escritos en el topic, y sin embargo un
 * consumidor {@code read_committed} solo ve el primero. Eso es lo que no se puede probar con un
 * mock: depende del broker, del productor transaccional y del Schema Registry.
 *
 * <p>De paso prueba el ida y vuelta de Avro con el registro de verdad (serializador, ID de esquema y
 * deserializador), que es la otra frontera que solo se cae en produccion.
 *
 * <p>Se ejecuta con {@code mvn verify}. Nota: en el test se usa la imagen de Confluent para el
 * broker porque es la que el contenedor {@code KafkaContainer} de Testcontainers trae probada; el
 * proyecto en produccion usa {@code apache/kafka}, y lo que se prueba aqui es el cableado del
 * cliente, no la imagen.
 */
@Testcontainers
class ExactlyOnceKafkaIT {

    private static final String TOPIC = "orders.executions";
    private static final String SCHEMA_REGISTRY_IMAGE = "confluentinc/cp-schema-registry:8.3.1";

    private static final Network RED = Network.newNetwork();

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))
            .withNetwork(RED)
            .withNetworkAliases("kafka");

    @Container
    static final GenericContainer<?> SCHEMA_REGISTRY = new GenericContainer<>(DockerImageName.parse(SCHEMA_REGISTRY_IMAGE))
            .withNetwork(RED)
            .withExposedPorts(8081)
            .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
            .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
            .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://kafka:9092")
            .dependsOn(KAFKA);

    private static String registryUrl() {
        return "http://" + SCHEMA_REGISTRY.getHost() + ":" + SCHEMA_REGISTRY.getMappedPort(8081);
    }

    @BeforeAll
    static void crearTopic() throws Exception {
        try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(TOPIC, 1, (short) 1))).all().get();
        }
    }

    @Test
    @DisplayName("Una transaccion abortada se queda en el topic y no la ve ningun consumidor read_committed")
    void la_transaccion_abortada_no_la_ve_nadie() {
        // 1) Un productor transaccional publica una ejecucion y CONFIRMA.
        try (KafkaProducer<String, Execution> productor = productorTransaccional()) {
            productor.initTransactions();
            productor.beginTransaction();
            productor.send(new ProducerRecord<>(TOPIC, "ASML", ejecucion("ASML")));
            productor.commitTransaction();

            // 2) Publica otra y la ABORTA: el mensaje esta en el log, pero no cuenta para nadie.
            productor.beginTransaction();
            productor.send(new ProducerRecord<>(TOPIC, "AAPL", ejecucion("AAPL")));
            productor.abortTransaction();
        }

        List<String> confirmados = leer(TOPIC, "read_committed");
        List<String> todos = leer(TOPIC, "read_uncommitted");

        assertThat(confirmados).as("solo la transaccion confirmada").containsExactly("ASML");
        assertThat(todos).as("la abortada si esta en el log").containsExactlyInAnyOrder("ASML", "AAPL");
    }

    /** Productor transaccional con el serializador Avro de Confluent contra el registro de verdad. */
    private static KafkaProducer<String, Execution> productorTransaccional() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put("schema.registry.url", registryUrl());
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        // El transactional.id es lo que convierte al productor en transaccional de verdad.
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "aggora-it-" + UUID.randomUUID());
        return new KafkaProducer<>(props);
    }

    /** Lee el topic con el nivel de aislamiento que se le pida. */
    private static List<String> leer(String topic, String isolationLevel) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "it-" + isolationLevel + "-" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        props.put("schema.registry.url", registryUrl());
        // El nivel de aislamiento es LA diferencia entre los dos consumidores de este test.
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolationLevel);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        List<String> simbolos = new ArrayList<>();
        try (KafkaConsumer<String, Execution> consumidor = new KafkaConsumer<>(props)) {
            consumidor.subscribe(List.of(topic));
            long limite = System.currentTimeMillis() + Duration.ofSeconds(15).toMillis();
            while (System.currentTimeMillis() < limite) {
                ConsumerRecords<String, Execution> records = consumidor.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, Execution> record : records) {
                    simbolos.add(record.value().getSymbol());
                }
                if (!simbolos.isEmpty()) {
                    break;
                }
            }
        }
        return simbolos;
    }

    private static Execution ejecucion(String symbol) {
        return Execution.newBuilder()
                .setExecutionId(UUID.randomUUID().toString())
                .setSymbol(symbol)
                .setPrice(new BigDecimal("130.0000"))
                .setQuantity(10)
                .setCurrency("USD")
                .setBuyOrderId("buy-1")
                .setSellOrderId("sell-1")
                .setBuyAccountId("ACC-01")
                .setSellAccountId("ACC-02")
                .setExecutedAt(Instant.now())
                .build();
    }
}
