package com.aggora.normalizer.config;

import io.confluent.kafka.serializers.subject.AssociatedNameStrategy;
import io.confluent.kafka.serializers.subject.RecordNameStrategy;
import io.confluent.kafka.serializers.subject.TopicNameStrategy;
import io.confluent.kafka.serializers.subject.TopicRecordNameStrategy;
import io.quarkus.runtime.annotations.RegisterForReflection;

/**
 * Lo que hay que declarar para que el binario NATIVO funcione.
 *
 * <p>En la JVM, el serializador de Confluent construye la estrategia de nombre de subject por
 * reflexion ({@code Utils.newInstance(className)}). En una imagen nativa la reflexion no existe
 * salvo que se declare, y al arrancar el binario fallaba con:
 *
 * <pre>
 *   NoSuchMethodException: io.confluent.kafka.serializers.subject.AssociatedNameStrategy.&lt;init&gt;()
 * </pre>
 *
 * <p>Es el gotcha clasico de GraalVM y el que el informe de la Fase 9 pide documentar: el codigo no
 * cambia, lo que cambia es que hay que DECIRLE al compilador que esas clases se usan por reflexion.
 * Se declaran las cuatro estrategias de subject que trae Confluent (la de por defecto y las tres
 * alternativas) para no depender de cual elija la configuracion.
 */
@RegisterForReflection(targets = {
        TopicNameStrategy.class,
        RecordNameStrategy.class,
        TopicRecordNameStrategy.class,
        AssociatedNameStrategy.class
})
public class ReflexionNativa {
}
