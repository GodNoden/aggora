-- Esquema de la auditoria. Spring Boot lo ejecuta al arrancar (spring.sql.init.mode=always),
-- asi que el servicio crea sus propias tablas. En un proyecto mas grande esto seria Flyway.

-- Todo lo que ha pasado, con su origen para poder rastrearlo.
create table if not exists audit_events (
    id              bigserial primary key,
    source_topic    text        not null,
    source_partition int        not null,
    source_offset   bigint      not null,
    entity_type     text        not null,
    entity_id       text        not null,
    payload         text        not null,
    occurred_at     timestamptz not null,
    recorded_at     timestamptz not null default now()
);

-- El indice unico es lo que hace idempotente la ingesta: si Kafka reentrega un mensaje
-- (at-least-once), la insercion no se repite y no se audita dos veces lo mismo.
create unique index if not exists audit_events_source_idx
    on audit_events (source_topic, source_partition, source_offset);

-- Bandeja de salida: los eventos que quedan pendientes de publicar en Kafka. Se escriben
-- en la MISMA transaccion que el evento auditado.
create table if not exists audit_outbox (
    id           bigserial primary key,
    entity_type  text        not null,
    entity_id    text        not null,
    payload      text        not null,
    created_at   timestamptz not null default now(),
    published_at timestamptz
);

-- Indice parcial: al publicador solo le interesan los pendientes.
create index if not exists audit_outbox_pending_idx
    on audit_outbox (published_at) where published_at is null;
