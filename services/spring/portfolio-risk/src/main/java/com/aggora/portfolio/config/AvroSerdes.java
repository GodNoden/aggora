package com.aggora.portfolio.config;

import java.util.Map;

import com.aggora.avro.orders.Execution;
import com.aggora.avro.portfolio.PortfolioPosition;
import com.aggora.avro.portfolio.PositionDelta;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;

/** Serdes Avro de la topologia. Clase normal (no bean) para poder probarla con "mock://". */
public class AvroSerdes {

    private final String schemaRegistryUrl;

    public AvroSerdes(String schemaRegistryUrl) {
        this.schemaRegistryUrl = schemaRegistryUrl;
    }

    public Serde<Execution> executions() {
        return specific();
    }

    public Serde<PositionDelta> deltas() {
        return specific();
    }

    public Serde<PortfolioPosition> positions() {
        return specific();
    }

    private <T extends SpecificRecord> Serde<T> specific() {
        SpecificAvroSerde<T> serde = new SpecificAvroSerde<>();
        serde.configure(Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl), false);
        return serde;
    }
}
