package com.aggora.simulator.domain;

import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Horario real de cada mercado, en su propia zona horaria.
 *
 * El objetivo del ejercicio (spec 3.3.5) es que el simulador se calle cuando el
 * mercado está cerrado: no hay ticks de NYSE a las 03:00 de Nueva York, ni durante
 * el descanso del mediodía de Shanghai.
 *
 * ponytail: sin calendario de festivos ni medias sesiones; FX/commodities se
 * aproximan a 24x5 en UTC. Si hace falta precisión, el sitio donde meterlo es
 * este enum (un Set<LocalDate> de festivos por exchange).
 */
public enum Exchange {

    NYSE("America/New_York", "09:30-16:00"),
    NASDAQ("America/New_York", "09:30-16:00"),
    EURONEXT("Europe/Paris", "09:00-17:30"),
    /** Shanghai tiene descanso de mediodía: dos sesiones el mismo día. */
    SSE("Asia/Shanghai", "09:30-11:30,13:00-15:00"),
    /** Divisas: de domingo noche a viernes noche; aquí simplificado a L-V completo. */
    FX("UTC", ""),
    COMMODITY("UTC", "");

    private final ZoneId zone;
    private final String hoursText;
    private final List<Session> sessions;

    Exchange(String zone, String hours) {
        this.zone = ZoneId.of(zone);
        this.hoursText = hours;
        this.sessions = Session.parseAll(hours);
    }

    public ZoneId zone() {
        return zone;
    }

    /** Texto legible del horario, para logs. */
    public String hours() {
        return sessions.isEmpty() ? "24x5" : hoursText;
    }

    /** ¿Está el mercado abierto en ese instante? (fin de semana cerrado siempre) */
    public boolean isOpen(Instant instant) {
        ZonedDateTime local = instant.atZone(zone);
        if (local.getDayOfWeek() == DayOfWeek.SATURDAY || local.getDayOfWeek() == DayOfWeek.SUNDAY) {
            return false;
        }
        if (sessions.isEmpty()) {
            return true;
        }
        LocalTime time = local.toLocalTime();
        return sessions.stream().anyMatch(session -> session.contains(time));
    }

    /** Rango [apertura, cierre) en hora local del mercado. */
    private record Session(LocalTime open, LocalTime close) {

        static List<Session> parseAll(String hours) {
            if (hours == null || hours.isBlank()) {
                return List.of();
            }
            List<Session> parsed = new ArrayList<>();
            Arrays.stream(hours.split(",")).map(Session::parse).forEach(parsed::add);
            return List.copyOf(parsed);
        }

        static Session parse(String range) {
            String[] parts = range.split("-");
            return new Session(LocalTime.parse(parts[0]), LocalTime.parse(parts[1]));
        }

        boolean contains(LocalTime time) {
            return !time.isBefore(open) && time.isBefore(close);
        }
    }
}
