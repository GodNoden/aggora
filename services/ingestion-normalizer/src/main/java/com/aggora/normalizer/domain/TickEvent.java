package com.aggora.normalizer.domain;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Copia propia del contrato de market.ticks.raw.
 *
 * A propósito NO hay un módulo compartido con market-data-simulator: cada servicio
 * define su vista del contrato, que es lo que pasa de verdad entre equipos. El
 * precio de esa libertad (nadie te avisa si el emisor cambia el JSON) es justo lo
 * que motiva el Schema Registry de la Fase 2.
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

    public enum TickSource { REFERENCE, SYNTHETIC }

    /** Enum de exchanges conocido por ESTE servicio (solo los que sabe normalizar). */
    public enum Exchange { NYSE, NASDAQ, EURONEXT, SSE, FX, COMMODITY }
}
