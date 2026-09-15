package com.aggora.portfolio.topology;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.List;

import com.aggora.avro.orders.Execution;
import com.aggora.avro.portfolio.PortfolioPosition;
import com.aggora.avro.portfolio.PositionDelta;
import com.aggora.portfolio.config.AggoraProperties;
import com.aggora.portfolio.config.AvroSerdes;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * De ejecuciones a posiciones.
 *
 *   orders.executions --(cada ejecucion son DOS movimientos)--> re-clavado por
 *   cuenta|simbolo --> KTable de posiciones --> portfolio.updates
 *
 * Detalles que importan:
 *
 *  - El re-clavado no es capricho: las ejecuciones vienen con key = SIMBOLO (porque el
 *    motor de matching las agrupa asi), y el estado que queremos es por CUENTA. Cambiar
 *    la clave obliga a repartir, y Kafka Streams lo hace solo.
 *  - El coste medio solo cambia al ABRIR o AUMENTAR. Al cerrar parte de la posicion lo
 *    que se materializa es el resultado (P&L realizado); el coste de lo que queda
 *    abierto no se toca.
 *  - El estado va a un state store con su changelog, asi que las posiciones sobreviven a
 *    un reinicio del servicio.
 */
public final class PortfolioTopology {

    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(PortfolioTopology.class);
    private static final java.util.concurrent.atomic.AtomicLong SAMPLED = new java.util.concurrent.atomic.AtomicLong();

    /** El estado se guarda por cuenta Y simbolo; en el topic la clave es solo la cuenta. */
    private static final String KEY_SEPARATOR = "|";

    private PortfolioTopology() {
    }

    public static KStream<String, PortfolioPosition> apply(StreamsBuilder builder,
                                                           AggoraProperties props,
                                                           AvroSerdes serdes) {
        KStream<String, Execution> executions = builder.stream(
                props.topics().executions(),
                Consumed.with(Serdes.String(), serdes.executions()));

        KStream<String, PositionDelta> deltas = executions.flatMap((key, execution) -> List.of(
                KeyValue.pair(stateKey(execution.getBuyAccountId(), execution.getSymbol()),
                        deltaOf(execution, execution.getBuyAccountId(), execution.getQuantity())),
                KeyValue.pair(stateKey(execution.getSellAccountId(), execution.getSymbol()),
                        deltaOf(execution, execution.getSellAccountId(), -execution.getQuantity()))));

        KTable<String, PortfolioPosition> positions = deltas
                .groupByKey(Grouped.with(Serdes.String(), serdes.deltas()))
                .aggregate(
                        PortfolioTopology::emptyPosition,
                        (stateKey, delta, position) -> apply(delta, position, props.marginLimit()),
                        Materialized.<String, PortfolioPosition, KeyValueStore<Bytes, byte[]>>as("positions-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(serdes.positions()));

        KStream<String, PortfolioPosition> updates = positions.toStream()
                // En el topic la clave es la CUENTA (como pide el spec), no la clave
                // compuesta que usa el estado.
                .map((stateKey, position) -> KeyValue.pair(position.getAccountId(), position));

        updates
                // Log muestreado: el topic va en Avro binario, asi que esto es lo que
                // permite ver las posiciones por la consola.
                .peek((accountId, position) -> {
                    if (SAMPLED.incrementAndGet() % 50 == 0) {
                        log.info("[cartera] {} {} | cantidad={} coste medio={} P&L realizado={} exposicion={} {} {}",
                                position.getAccountId(), position.getSymbol(), position.getQuantity(),
                                position.getAverageCost(), position.getRealizedPnl(), position.getExposure(),
                                position.getCurrency(),
                                position.getMarginBreach() ? "MARGEN SUPERADO" : "");
                    }
                })
                .to(props.topics().portfolioUpdates(), Produced.with(Serdes.String(), serdes.positions()));
        return updates;
    }

    static String stateKey(String accountId, String symbol) {
        return accountId + KEY_SEPARATOR + symbol;
    }

    static PositionDelta deltaOf(Execution execution, String accountId, int quantityDelta) {
        return PositionDelta.newBuilder()
                .setAccountId(accountId)
                .setSymbol(execution.getSymbol())
                .setCurrency(execution.getCurrency())
                .setQuantityDelta(quantityDelta)
                .setPrice(execution.getPrice())
                .setExecutionId(execution.getExecutionId())
                .setExecutedAt(execution.getExecutedAt())
                .build();
    }

    static PortfolioPosition emptyPosition() {
        return PortfolioPosition.newBuilder()
                .setAccountId("")
                .setSymbol("")
                .setCurrency("")
                .setQuantity(0L)
                .setAverageCost(decimal(0.0))
                .setRealizedPnl(decimal(0.0))
                .setExposure(decimal(0.0))
                .setMarginLimit(decimal(0.0))
                .setMarginBreach(false)
                .setExecutions(0L)
                .setLastUpdated(Instant.EPOCH)
                .build();
    }

    /**
     * Aplica un movimiento a la posicion. Es la aritmetica de una cartera, y por eso
     * tiene test: abrir o aumentar cambia el coste medio; cerrar materializa resultado.
     */
    static PortfolioPosition apply(PositionDelta delta, PortfolioPosition position, BigDecimal marginLimit) {
        long previousQuantity = position.getQuantity();
        long newQuantity = previousQuantity + delta.getQuantityDelta();
        double averageCost = position.getAverageCost().doubleValue();
        double realized = position.getRealizedPnl().doubleValue();
        double price = delta.getPrice().doubleValue();

        boolean openingOrIncreasing = previousQuantity == 0
                || Long.signum(previousQuantity) == Long.signum(delta.getQuantityDelta());

        if (openingOrIncreasing) {
            // Coste medio ponderado de todo lo que queda abierto.
            averageCost = (averageCost * previousQuantity + price * delta.getQuantityDelta()) / newQuantity;
        } else {
            long closing = Math.min(Math.abs(previousQuantity), Math.abs(delta.getQuantityDelta()));
            // Resultado de la parte que se cierra: en una posicion larga se gana si se
            // vende mas caro de lo que costo; en una corta, al reves.
            realized += Math.signum(previousQuantity) * (price - averageCost) * closing;
            if (Math.abs(delta.getQuantityDelta()) > Math.abs(previousQuantity)) {
                // La operacion da la vuelta a la posicion: el resto abre al precio nuevo.
                averageCost = price;
            }
        }

        if (newQuantity == 0) {
            averageCost = 0.0;
        }

        double exposure = Math.abs(newQuantity) * averageCost;
        return PortfolioPosition.newBuilder(position)
                .setAccountId(delta.getAccountId())
                .setSymbol(delta.getSymbol())
                .setCurrency(delta.getCurrency())
                .setQuantity(newQuantity)
                .setAverageCost(decimal(averageCost))
                .setRealizedPnl(decimal(realized))
                .setExposure(decimal(exposure))
                .setMarginLimit(marginLimit)
                .setMarginBreach(BigDecimal.valueOf(exposure).compareTo(marginLimit) > 0)
                .setExecutions(position.getExecutions() + 1)
                .setLastUpdated(delta.getExecutedAt())
                .build();
    }

    private static BigDecimal decimal(double value) {
        return BigDecimal.valueOf(value).setScale(4, RoundingMode.HALF_UP);
    }
}
