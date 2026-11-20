package cl.duoc.vidasalud.appointments.model.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AppointmentTelemetryEventDTO implements Serializable {
    private static final long serialVersionUID = 1L;

    private String eventId;
    private Long appointmentId;
    private String patientRut;
    private String patientName;
    private Long doctorId;
    private String doctorName;
    private String specialty;
    private String centerId;
    private String status;
    private Integer estimatedWaitTimeMinutes;
    private String timestamp;
    private String traceId;
}
