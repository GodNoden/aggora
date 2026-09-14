package com.aggora.simulator.domain;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Contrato JSON de market.ticks.raw (v1, todavía sin Schema Registry).
 *
 * El key del mensaje Kafka es {@code symbol}: todos los ticks de un instrumento
 * caen en la misma partición, así que se procesan en orden dentro de él.
 *
 * Ojo: el consumidor (ingestion-normalizer) tiene su PROPIA copia de esta clase.
 * No hay módulo compartido a propósito: en la Fase 2 el Schema Registry pasa a ser
 * el contrato real y este es el dolor que lo justifica.
 */
public record TickEvent(
        String eventId,
        String symbol,
        AssetClass assetClass,
        Exchange exchange,
        String currency,
        BigDecimal price,
        int size,
        Instant eventTime,
        TickSource source,
        long sequence) {

    public enum AssetClass { EQUITY, FX, COMMODITY }

    /**
     * REFERENCE = precio real recién traído de la API (size 0: es una cotización,
     * no una operación).
     * SYNTHETIC = tick interpolado entre dos referencias.
     */
    public enum TickSource { REFERENCE, SYNTHETIC }
}
