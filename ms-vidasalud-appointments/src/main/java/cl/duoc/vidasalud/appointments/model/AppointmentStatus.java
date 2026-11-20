package cl.duoc.vidasalud.appointments.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum AppointmentStatus {
    SOLICITADA,
    CONFIRMADA,
    EN_ESPERA,
    EN_ATENCION,
    CERRADA,
    CANCELADA;

    @JsonCreator
    public static AppointmentStatus fromString(String val) {
        if (val == null) return null;
        String normalized = val.trim().toUpperCase()
                .replace("Ã“", "O")
                .replace("Ã“", "O")
                .replace("Ó", "O");
        for (AppointmentStatus s : values()) {
            if (s.name().equalsIgnoreCase(normalized)) return s;
        }
        return valueOf(val);
    }

    @JsonValue
    public String toValue() {
        if (this == EN_ATENCION) {
            return "EN_ATENCIÓN";
        }
        return this.name();
    }

    public static boolean isValid(String status) {
        if (status == null) return false;
        try {
            return fromString(status) != null;
        } catch (Exception e) {
            return false;
        }
    }
}
