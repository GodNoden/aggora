package com.aggora.gateway.quarkus.config;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.config.MeterFilter;

/**
 * Las etiquetas comunes de las metricas, iguales que en los otros dos servicios de Quarkus: son
 * las que permiten poner las dos implementaciones en el mismo panel de Grafana filtrando por
 * {@code stack}. Spring las pone en el yaml; Quarkus, con un MeterFilter.
 */
@ApplicationScoped
public class MetricsTags {

    @Produces
    @ApplicationScoped
    public MeterFilter etiquetasComunes() {
        return MeterFilter.commonTags(Tags.of("stack", "quarkus", "service", "gateway-ws"));
    }
}
