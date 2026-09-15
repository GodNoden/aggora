package com.aggora.analytics.config;

import java.util.Map;

import com.aggora.avro.analytics.MetricsAccumulator;
import com.aggora.avro.analytics.SymbolMetrics;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;

/**
 * Serdes Avro para la topologia.
 *
 * Kafka Streams necesita saber convertir bytes <-> objeto en tres sitios: la entrada
 * (ticks canonicos), el estado de las ventanas (el acumulador) y la salida (metricas).
 * SpecificAvroSerde hace el trabajo leyendo el ID de esquema de cada mensaje.
 *
 * Es una clase normal (no un @Component) a proposito: en los tests se le pasa una URL
 * "mock://" y asi la topologia se prueba sin Schema Registry ni broker.
 */
public class AvroSerdes {

    private final String schemaRegistryUrl;

    public AvroSerdes(String schemaRegistryUrl) {
        this.schemaRegistryUrl = schemaRegistryUrl;
    }

    public Serde<com.aggora.avro.canonical.CanonicalTick> canonicalTicks() {
        return specific();
    }

    public Serde<MetricsAccumulator> accumulator() {
        return specific();
    }

    public Serde<SymbolMetrics> metrics() {
        return specific();
    }

    public Serde<com.aggora.avro.reference.FxRate> fxRates() {
        return specific();
    }

    public Serde<com.aggora.avro.analytics.ArbitrageSpread> spreads() {
        return specific();
    }

    private <T extends SpecificRecord> Serde<T> specific() {
        SpecificAvroSerde<T> serde = new SpecificAvroSerde<>();
        serde.configure(Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl), false);
        return serde;
    }
}
