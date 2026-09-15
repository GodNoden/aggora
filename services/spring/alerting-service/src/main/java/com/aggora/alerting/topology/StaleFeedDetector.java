package com.aggora.alerting.topology;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.aggora.avro.alerts.Alert;
import com.aggora.avro.alerts.AlertType;
import com.aggora.avro.alerts.Severity;
import com.aggora.avro.analytics.SymbolMetrics;

import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Detector de FEED PARADO: el simbolo lleva demasiado tiempo sin dar senales.
 *
 * Es el unico caso en el que hay que detectar que algo NO llega, y eso no se puede hacer
 * reaccionando a los mensajes: si no llegan mensajes, no hay nada que reaccione. Kafka
 * Streams tiene para esto los "punctuators": una funcion que el propio Streams llama cada
 * X tiempo de reloj, haya datos o no.
 *
 * Se avisa UNA vez por episodio: cuando un simbolo se queda callado se levanta la alerta
 * y no se repite hasta que vuelvan a llegar datos. Si no, un mercado cerrado (que
 * legitimamente no tiene ticks) generaria una alerta cada vuelta del punctuator.
 *
 * ponytail: dos limitaciones conocidas. (1) El estado vive en memoria de la instancia, no
 * en un state store: un rebalanceo lo reinicia. (2) Un mercado cerrado es un "feed parado"
 * legitimo y aqui se avisa igual; en produccion se cruzaria con el calendario de mercado.
 */
public class StaleFeedDetector implements ProcessorSupplier<String, SymbolMetrics, String, Alert> {

    private static final Logger log = LoggerFactory.getLogger(StaleFeedDetector.class);

    private final Duration staleTimeout;
    private final Duration checkInterval;

    public StaleFeedDetector(Duration staleTimeout, Duration checkInterval) {
        this.staleTimeout = staleTimeout;
        this.checkInterval = checkInterval;
    }

    @Override
    public Processor<String, SymbolMetrics, String, Alert> get() {
        return new Processor<>() {

            private final Map<String, Long> lastSeen = new HashMap<>();
            private final Set<String> alreadyAlerted = new HashSet<>();
            private ProcessorContext<String, Alert> context;

            @Override
            public void init(ProcessorContext<String, Alert> context) {
                this.context = context;
                // Reloj de pared: aunque no entre ni un mensaje, esto se ejecuta.
                context.schedule(checkInterval, PunctuationType.WALL_CLOCK_TIME, this::checkStaleSymbols);
            }

            @Override
            public void process(Record<String, SymbolMetrics> record) {
                // currentSystemTimeMs() es el reloj del task: en produccion es el reloj de
                // verdad y en los tests es el tiempo simulado del driver, asi el detector se
                // puede probar avanzando el reloj a mano.
                lastSeen.put(record.key(), context.currentSystemTimeMs());
                // Llego datos: el episodio de silencio ha terminado.
                alreadyAlerted.remove(record.key());
            }

            private void checkStaleSymbols(long now) {
                lastSeen.forEach((symbol, seenAt) -> {
                    long silentMillis = now - seenAt;
                    if (silentMillis > staleTimeout.toMillis() && alreadyAlerted.add(symbol)) {
                        log.info("[alerta] feed parado: {} lleva {} s sin datos", symbol, silentMillis / 1000);
                        context.forward(new Record<>(symbol,
                                alert(AlertType.STALE_FEED, Severity.CRITICAL, symbol,
                                        "sin datos desde hace " + silentMillis / 1000 + " s",
                                        silentMillis / 1000.0, staleTimeout.getSeconds()),
                                now));
                    }
                });
            }
        };
    }

    static Alert alert(AlertType type, Severity severity, String subject, String detail,
                       double value, double threshold) {
        return Alert.newBuilder()
                .setAlertId(UUID.randomUUID().toString())
                .setType(type)
                .setSeverity(severity)
                .setSubject(subject)
                .setDetail(detail)
                .setValue(decimal(value))
                .setThreshold(decimal(threshold))
                .setRaisedAt(Instant.now())
                .build();
    }

    static BigDecimal decimal(double value) {
        return BigDecimal.valueOf(value).setScale(4, RoundingMode.HALF_UP);
    }
}
