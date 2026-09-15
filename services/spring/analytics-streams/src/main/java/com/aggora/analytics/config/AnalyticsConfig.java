package com.aggora.analytics.config;

import com.aggora.analytics.topology.ArbitrageTopology;
import com.aggora.analytics.topology.MetricsTopology;
import com.aggora.avro.analytics.SymbolMetrics;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer;

/**
 * Enganche de la topologia con Spring.
 *
 * @EnableKafkaStreams (en la clase principal) crea y arranca el StreamsBuilder y su
 * ciclo de vida. Aqui solo le decimos QUE construir: un bean que recibe el builder y
 * devuelve el stream final, que es la forma que documenta Spring Boot.
 */
@Configuration
public class AnalyticsConfig {

    private static final Logger log = LoggerFactory.getLogger(AnalyticsConfig.class);

    /**
     * Que hacer cuando Kafka Streams se encuentra un error que no puede manejar el solo.
     *
     * Por defecto, el cliente se para: pasa a estado ERROR y deja de procesar. Y aqui esta lo
     * peligroso, que se descubrio operando el cluster: **el proceso Java sigue vivo**, el
     * endpoint HTTP responde y el servicio PARECE sano mientras no hace nada. Un fallo
     * silencioso, que es el peor tipo.
     *
     * Con este manejador se sustituye el hilo que fallo en vez de matar el cliente, que es lo
     * que recomienda la documentacion de Kafka Streams y lo que hace que un error de
     * procesamiento no se lleve por delante al servicio entero.
     *
     * OJO, y esto costo una tarde: **hay errores que esto no arregla**, y el peor es el del
     * hilo global de una GlobalKTable. Su estado vive en disco y su checkpoint apunta a offsets
     * de un topic compactado; cuando la compactacion se lleva por delante esos offsets, Kafka
     * Streams limpia el estado local y pide un reinicio que nadie le da: el cliente se queda en
     * ERROR y el servicio devuelve 503 para siempre. Se probo a responder
     * `SHUTDOWN_APPLICATION` (que sobre el papel para la aplicacion entera) y sale peor: dentro
     * de Spring el cierre se enreda, el consumidor entra en un bucle de "Request joining group
     * due to: Shutdown requested" que escribio 429 MB de log en 28 segundos, y el proceso
     * TAMPOCO muere.
     *
     * La conclusion no es "buscar otra respuesta del manejador", es que **un servicio no
     * deberia decidir suicidarse**: quien levanta un proceso caido es el supervisor (la
     * politica de reinicio de Docker, systemd, Kubernetes) y quien le dice que esta roto es la
     * sonda de salud. Por eso el arreglo de verdad es {@link com.aggora.analytics.health.StreamsHealth}:
     * `/actuator/health` baja a DOWN cuando el motor no procesa, y el supervisor hace el resto.
     */
    @Bean
    public StreamsBuilderFactoryBeanConfigurer streamsResilience() {
        return factoryBean -> factoryBean.setStreamsUncaughtExceptionHandler(throwable -> {
            log.error("[streams] error no controlado ({}): se sustituye el hilo para seguir procesando; "
                    + "si el motor se queda en ERROR, /actuator/health lo dira y el supervisor reiniciara",
                    throwable.getMessage());
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
        });
    }

    @Bean
    public AvroSerdes avroSerdes(AggoraProperties props) {
        return new AvroSerdes(props.schemaRegistryUrl());
    }

    @Bean
    public KStream<String, SymbolMetrics> analyticsTopology(StreamsBuilder builder,
                                                           AggoraProperties props,
                                                           AvroSerdes serdes) {
        // Las dos topologias viven en la MISMA aplicacion de Kafka Streams (un solo
        // StreamsBuilder), asi que comparten instancia, hilos y ciclo de vida.
        ArbitrageTopology.apply(builder, props, serdes);
        return MetricsTopology.apply(builder, props, serdes);
    }
}
