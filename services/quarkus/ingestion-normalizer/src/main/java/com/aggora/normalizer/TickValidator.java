package com.aggora.normalizer;

import java.time.Duration;
import java.time.Instant;
import java.util.Currency;

import com.aggora.avro.Tick;

/**
 * Las reglas de validacion del tick, separadas del consumo para poder probarlas sin Kafka.
 *
 * <p>Son EXACTAMENTE las mismas que las de la version Spring (mismo motivo, mismo texto y en
 * el mismo orden): el port tiene que comportarse igual, y la unica forma de saberlo es que
 * las reglas sean las mismas y esten escritas una vez.
 */
public class TickValidator {

    /**
     * Devuelve el motivo por el que el mensaje no es valido, o {@code null} si lo es.
     *
     * @param key la key del mensaje en Kafka, que tiene que coincidir con el simbolo
     * @param tick el valor deserializado, que puede venir nulo
     */
    public String validate(String key, Tick tick) {
        if (tick == null) {
            return "payload nulo";
        }
        if (tick.getSymbol() == null || tick.getSymbol().isBlank()) {
            return "symbol vacio";
        }
        if (key != null && !key.equals(tick.getSymbol())) {
            return "la key (" + key + ") no coincide con el symbol (" + tick.getSymbol() + ")";
        }
        if (tick.getPrice() == null || tick.getPrice().signum() <= 0) {
            return "precio ausente o no positivo";
        }
        if (tick.getEventTime() == null) {
            return "sin timestamp";
        }
        if (tick.getEventTime().isAfter(Instant.now().plus(Duration.ofMinutes(5)))) {
            return "timestamp en el futuro: " + tick.getEventTime();
        }
        if (tick.getAssetClass() == null || tick.getExchange() == null || tick.getSource() == null) {
            return "falta assetClass, exchange o source";
        }
        try {
            Currency.getInstance(tick.getCurrency());
        } catch (IllegalArgumentException ex) {
            return "divisa no ISO-4217: " + tick.getCurrency();
        }
        return null;
    }
}
