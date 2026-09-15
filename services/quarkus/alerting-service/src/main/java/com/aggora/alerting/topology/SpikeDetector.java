package com.aggora.alerting.topology;

import java.util.HashSet;
import java.util.Set;

import com.aggora.avro.alerts.Alert;
import com.aggora.avro.alerts.AlertType;
import com.aggora.avro.alerts.Severity;
import com.aggora.avro.analytics.SymbolMetrics;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pico de precio, avisando UNA vez por episodio.
 *
 * Escribirlo como procesador y no como un simple filtro tiene un motivo que se aprendio en
 * vivo: la ventana movil emite una metrica cada pocos segundos, asi que un filtro "si la
 * desviacion supera el umbral" producia miles de alertas por el mismo pico (36.000 en unos
 * minutos, de verdad). Un aviso que se repite deja de ser un aviso.
 *
 * El procesador recuerda que simbolos estan "en pico" y no vuelve a avisar hasta que el
 * precio se normaliza, momento en el que se rearma. La vuelta a la normalidad se mide con
 * un umbral MAS BAJO que el de disparo (histeresis): si se usara el mismo, un precio que
 * oscila justo alrededor del limite estaria avisando sin parar, que es lo que pasaba en la
 * practica. Es la misma idea que el termostato que enciende a 19 grados y no apaga hasta
 * los 21.
 *
 * ponytail: el estado vive en memoria de la instancia, no en un state store, asi que un
 * rebalanceo lo reinicia (y podria repetir un aviso). La regla de margen tiene el mismo
 * problema de repeticion; se arregla igual cuando moleste.
 */
public class SpikeDetector implements ProcessorSupplier<String, SymbolMetrics, String, Alert> {

    private static final Logger log = LoggerFactory.getLogger(SpikeDetector.class);

    private final double thresholdBps;
    private final double rearmBps;

    public SpikeDetector(double thresholdBps, double rearmBps) {
        this.thresholdBps = thresholdBps;
        this.rearmBps = rearmBps;
    }

    @Override
    public Processor<String, SymbolMetrics, String, Alert> get() {
        return new Processor<>() {

            private final Set<String> inSpike = new HashSet<>();
            private ProcessorContext<String, Alert> context;

            @Override
            public void init(ProcessorContext<String, Alert> context) {
                this.context = context;
            }

            @Override
            public void process(Record<String, SymbolMetrics> record) {
                SymbolMetrics metrics = record.value();
                double mean = metrics.getMovingAverage().doubleValue();
                if (mean == 0.0) {
                    return;
                }
                double deviationBps = Math.abs(metrics.getLastPrice().doubleValue() - mean) / mean * 10_000.0;

                if (deviationBps <= rearmBps) {
                    // El precio se ha normalizado de verdad: la proxima vez que se dispare,
                    // volvera a avisar.
                    inSpike.remove(record.key());
                    return;
                }
                if (deviationBps <= thresholdBps) {
                    return; // zona muerta entre el rearme y el disparo: ni avisa ni rearma
                }
                if (!inSpike.add(record.key())) {
                    return; // ya se aviso de este pico
                }
                log.info("[alerta] pico de precio en {}: {} puntos basicos", record.key(), Math.round(deviationBps));
                context.forward(new Record<>(record.key(),
                        StaleFeedDetector.alert(AlertType.PRICE_SPIKE, Severity.WARNING, metrics.getSymbol(),
                                "el ultimo precio se aleja " + Math.round(deviationBps)
                                        + " puntos basicos de la media de la ventana",
                                deviationBps, thresholdBps),
                        record.timestamp()));
            }
        };
    }
}
