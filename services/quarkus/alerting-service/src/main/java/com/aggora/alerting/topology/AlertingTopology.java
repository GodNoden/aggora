package com.aggora.alerting.topology;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import com.aggora.alerting.config.AggoraConfig;
import com.aggora.alerting.config.AvroSerdes;
import com.aggora.avro.alerts.Alert;
import com.aggora.avro.alerts.AlertType;
import com.aggora.avro.alerts.Severity;
import com.aggora.avro.analytics.SymbolMetrics;
import com.aggora.avro.analytics.WindowKind;
import com.aggora.avro.portfolio.PortfolioPosition;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Las tres reglas de alerta.
 *
 * Las dos primeras son "por dato": cada mensaje que llega se mira contra un umbral.
 * La tercera es "por ausencia" y necesita un punctuator (ver StaleFeedDetector).
 *
 *   market.analytics   --pico de precio--> \\
 *   portfolio.updates  --margen superado-->  +--> alerts.raised
 *   market.analytics   --feed parado-----> /
 */
public final class AlertingTopology {

    private static final Logger log = LoggerFactory.getLogger(AlertingTopology.class);
    private static final AtomicLong SAMPLED = new AtomicLong();

    private AlertingTopology() {
    }

    public static KStream<String, Alert> apply(StreamsBuilder builder,
                                               AggoraConfig props,
                                               AvroSerdes serdes) {
        // Solo la ventana movil: la fija trae casi la misma informacion y no queremos dos
        // alertas por lo mismo.
        KStream<String, SymbolMetrics> metrics = builder.stream(
                        props.topics().analytics(),
                        Consumed.with(Serdes.String(), serdes.metrics()))
                .filter((key, value) -> value.getWindowKind() == WindowKind.HOPPING);

        // Regla 1: pico de precio. Se compara el ultimo precio con la media de su ventana,
        // y se avisa una vez por episodio (ver SpikeDetector).
        KStream<String, Alert> spikes = metrics.process(
                new SpikeDetector(props.priceSpikeBps(), props.priceSpikeRearmBps()));

        // Regla 2: margen superado, que ya viene marcado por portfolio-risk.
        KStream<String, Alert> breaches = builder.stream(
                        props.topics().portfolioUpdates(),
                        Consumed.with(Serdes.String(), serdes.positions()))
                .filter((key, position) -> position.getMarginBreach())
                .flatMapValues(AlertingTopology::breachAlert);

        // Regla 3: feed parado.
        KStream<String, Alert> stale = metrics.process(
                new StaleFeedDetector(props.staleFeedTimeout(), props.staleFeedCheckInterval()));

        KStream<String, Alert> alerts = spikes.merge(breaches).merge(stale);

        alerts
                .peek((key, alert) -> {
                    if (SAMPLED.incrementAndGet() % 20 == 0) {
                        log.info("[alerta] {} {} sobre {}: {} ({} vs limite {})",
                                alert.getSeverity(), alert.getType(), alert.getSubject(),
                                alert.getDetail(), alert.getValue(), alert.getThreshold());
                    }
                })
                .to(props.topics().alertsRaised(), Produced.with(Serdes.String(), serdes.alerts()));

        return alerts;
    }

    private static List<Alert> breachAlert(PortfolioPosition position) {
        return List.of(StaleFeedDetector.alert(AlertType.MARGIN_BREACH, Severity.CRITICAL, position.getAccountId(),
                "exposicion de " + position.getExposure() + " " + position.getCurrency()
                        + " en " + position.getSymbol() + " por encima del limite",
                position.getExposure().doubleValue(), position.getMarginLimit().doubleValue()));
    }
}
