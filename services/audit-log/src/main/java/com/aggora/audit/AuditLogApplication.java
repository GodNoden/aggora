package com.aggora.audit;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * audit-log: deja constancia de todo lo que pasa, sin perder ni un evento.
 *
 * El problema que resuelve: guardar el evento en Postgres y publicarlo en Kafka son dos
 * escrituras en dos sistemas distintos, y no hay forma de hacerlas "a la vez". Si se
 * guarda y luego se cae antes de publicar, el evento se pierde para los demas; si se
 * publica y luego se cae antes de guardar, en el registro no queda.
 *
 * El patron TRANSACTIONAL OUTBOX le da la vuelta: en UNA transaccion de base de datos se
 * guardan dos cosas: el evento auditado y un "recado" en la tabla outbox diciendo "esto
 * hay que publicarlo". Eso si es atomico, porque es la misma base de datos. Despues, un
 * publicador aparte lee los recados pendientes y los manda a Kafka, marcandolos como
 * publicados. Si se cae a mitad, al volver a arrancar los vuelve a mandar: como mucho se
 * publica dos veces (at-least-once), y eso se tolera porque el topic es COMPACTADO y la
 * clave es la entidad: quedarse con el ultimo valor hace que el duplicado sea inofensivo.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
@EnableScheduling
public class AuditLogApplication {

    public static void main(String[] args) {
        SpringApplication.run(AuditLogApplication.class, args);
    }
}
