package com.aggora.simulator.config;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.Tags;

/**
 * Las etiquetas comunes de las metricas.
 *
 * <p>La version Spring las pone en el yaml ({@code management.metrics.tags.stack=spring}), y son
 * las que permiten poner las dos implementaciones en el mismo panel de Grafana filtrando por
 * {@code stack}. Quarkus no tiene esa propiedad global (solo etiquetas para el binder de HTTP),
 * asi que se anade con un {@code MeterFilter}, que es la forma nativa de Micrometer.
 */
@ApplicationScoped
public class MetricsTags {

    @Produces
    @ApplicationScoped
    public MeterFilter etiquetasComunes() {
        return MeterFilter.commonTags(Tags.of("stack", "quarkus", "service", "market-data-simulator"));
    }
}
