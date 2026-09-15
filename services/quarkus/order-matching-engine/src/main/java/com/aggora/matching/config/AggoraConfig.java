package com.aggora.matching.config;

import io.smallrye.config.ConfigMapping;

/**
 * Configuracion del servicio. Los NOMBRES de las claves son los mismos que en el
 * {@code aggora:} del yml de la version Spring (para poder compararlos uno a uno); aqui viven en
 * {@code application.properties} porque son cinco claves planas y no hace falta YAML.
 */
@ConfigMapping(prefix = "aggora")
public interface AggoraConfig {

    String schemaRegistryUrl();

    Topics topics();

    /**
     * Gancho para el ejercicio de exactly-once: si es &gt; 0, cada N ordenes se lanza una
     * excepcion DESPUES de publicar las ejecuciones.
     */
    int failEveryNOrders();

    interface Topics {
        String ordersIncoming();

        String executions();
    }
}
