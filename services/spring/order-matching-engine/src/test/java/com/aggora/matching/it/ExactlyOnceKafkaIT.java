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
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.containers.wait.strategy.Wait;
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
    // En Testcontainers 2.x el contenedor de Kafka vive en `org.testcontainers.kafka` y hay una
    // clase por familia de imagenes: ConfluentKafkaContainer para las de Confluent (la que ya usaba
    // este test), KafkaContainer para la de Apache. Se mantiene la de Confluent a proposito.
    static final ConfluentKafkaContainer KAFKA =
            new ConfluentKafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))
            .withNetwork(RED)
            .withNetworkAliases("kafka");

    @Container
    static final GenericContainer<?> SCHEMA_REGISTRY = new GenericContainer<>(DockerImageName.parse(SCHEMA_REGISTRY_IMAGE))
            .withNetwork(RED)
            .withExposedPorts(8081)
            .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
            .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
            // El puerto NO es un detalle: 9092 es el listener que el broker anuncia para el HOST
            // (localhost:<puerto mapeado>), y por eso el registro no llegaba a el desde dentro de la
            // red de contenedores (su chequeo kafka-ready veia "0 brokers"). El listener interno del
            // contenedor de Confluent es el 9093, y su direccion anunciada SI es el alias de la red
            // (kafka:9093). El prefijo sigue siendo PLAINTEXT porque ahi va el protocolo de
            // seguridad, no el nombre del listener (el mapa del contenedor declara BROKER:PLAINTEXT).
            // Comprobado leyendo la configuracion del propio contenedor de Testcontainers.
            .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://kafka:9093")
            .dependsOn(KAFKA)
            // Espera EXPLICITA a que el registro escuche de verdad. Antes no habia ninguna: el
            // contenedor se daba por arrancado en cuanto el proceso existia, y el registro de
            // Confluent tarda bastante mas (se conecta a su broker, crea el topic _schemas...). El
            // test empezo a usarlo demasiado pronto y fallo con "Connection refused" al pedir las
            // asociaciones. Era una carrera latente que la version vieja de Testcontainers tapaba
            // por casualidad de tiempos y que la nueva (que arranca contenedores antes) ha
            // destapado: el fallo no era de la version, era del test.
            .waitingFor(Wait.forHttp("/subjects").forPort(8081).forStatusCode(200)
                    .withStartupTimeout(Duration.ofMinutes(2)));

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

            // ANTES DE ABORTAR hay que comprobar que el registro ha llegado al log, porque
            // send() es asincrono y abortTransaction() DESCARTA lo que el hilo emisor todavia no
            // habia mandado. Abortar justo despues del send es una carrera, y el CI la destapo dos
            // veces: el registro abortado no llegaba a escribirse y el test fallaba diciendo que
            // faltaba AAPL (con lo que no probaba nada de la transaccion, solo que no se escribio).
            //
            // read_uncommitted no filtra por transaccion: si el registro se ve con la transaccion
            // ABIERTA, esta en el log, y a partir de ahi abortar solo puede esconderlo. Medido
            // contra el cluster local: aparece en ~0,6 s.
            assertThat(leer(TOPIC, "read_uncommitted", 2))
                    .as("la abortada llega al log antes de abortar (si no, el test no probaria nada)")
                    .contains("AAPL");

            productor.abortTransaction();
        }

        // Con el registro ya en el log, esto es determinista: el commit lo deja ver y el abort lo
        // esconde. Se le pide a cada consumidor lo que TIENE que ver y se espera a que lo vea.
        assertThat(leer(TOPIC, "read_committed", 1)).as("solo la transaccion confirmada").containsExactly("ASML");
        assertThat(leer(TOPIC, "read_uncommitted", 2)).as("la abortada si esta en el log").containsExactlyInAnyOrder("ASML", "AAPL");
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

    /**
     * Lee el topic con el nivel de aislamiento que se le pida, hasta ver {@code esperados} mensajes
     * (o hasta que se acabe el tiempo).
     */
    private static List<String> leer(String topic, String isolationLevel, int esperados) {
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
            // 30 s y no 15: la primera lectura del test ocurre con la transaccion todavia abierta y
            // dentro de estos segundos entran el arranque del consumidor, el descubrimiento del
            // coordinador de grupo y la asignacion de particiones.
            long limite = System.currentTimeMillis() + Duration.ofSeconds(30).toMillis();
            while (System.currentTimeMillis() < limite) {
                simbolos.addAll(simbolosDe(consumidor.poll(Duration.ofMillis(500))));
                if (simbolos.size() >= esperados) {
                    // Una vuelta mas para dar tiempo a que aparezca lo que falta y a descartar que
                    // haya de mas (que es justo lo que distingue a los dos niveles de aislamiento).
                    simbolos.addAll(simbolosDe(consumidor.poll(Duration.ofMillis(700))));
                    break;
                }
            }
        }
        return simbolos;
    }

    private static List<String> simbolosDe(ConsumerRecords<String, Execution> records) {
        List<String> simbolos = new ArrayList<>();
        for (ConsumerRecord<String, Execution> record : records) {
            simbolos.add(record.value().getSymbol());
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
