package com.aggora.alerting.config;

import java.util.Map;

import com.aggora.avro.alerts.Alert;
import com.aggora.avro.analytics.SymbolMetrics;
import com.aggora.avro.portfolio.PortfolioPosition;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;

/** Serdes Avro del servicio. Clase normal para poder probarla con "mock://". */
public class AvroSerdes {

    private final String schemaRegistryUrl;

    public AvroSerdes(String schemaRegistryUrl) {
        this.schemaRegistryUrl = schemaRegistryUrl;
    }

    public Serde<SymbolMetrics> metrics() {
        return specific();
    }

    public Serde<PortfolioPosition> positions() {
        return specific();
    }

    public Serde<Alert> alerts() {
        return specific();
    }

    private <T extends SpecificRecord> Serde<T> specific() {
        SpecificAvroSerde<T> serde = new SpecificAvroSerde<>();
        serde.configure(Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl), false);
        return serde;
    }
}
