package cl.duoc.vidasalud.appointments.controller;

import cl.duoc.vidasalud.appointments.model.dto.AppointmentTelemetryEventDTO;
import cl.duoc.vidasalud.appointments.model.dto.AuditLogEventDTO;
import cl.duoc.vidasalud.appointments.publisher.KafkaEventPublisher;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/appointments/kafka")
@RequiredArgsConstructor
@CrossOrigin(origins = "*")
@Slf4j
public class KafkaProducerController {

    private final KafkaEventPublisher kafkaEventPublisher;

    /**
     * Endpoint para publicar evento de telemetría a Kafka ('vidasalud.telemetry.appointments').
     */
    @PostMapping("/publish-telemetry")
    public ResponseEntity<?> publishTelemetry(@RequestBody(required = false) Map<String, Object> payload) {
        String eventId = UUID.randomUUID().toString();
        String patientName = (payload != null && payload.containsKey("patientName"))
                ? payload.get("patientName").toString() : "Paciente Streaming Telemetría";
        String specialty = (payload != null && payload.containsKey("specialty"))
                ? payload.get("specialty").toString() : "Medicina General";
        String status = (payload != null && payload.containsKey("status"))
                ? payload.get("status").toString() : "CONFIRMADA";

        AppointmentTelemetryEventDTO event = AppointmentTelemetryEventDTO.builder()
                .eventId(eventId)
                .appointmentId(System.currentTimeMillis() % 100000)
                .patientRut("12.345.678-9")
                .patientName(patientName)
                .doctorId(101L)
                .doctorName("Dra. Francisca Molina")
                .specialty(specialty)
                .centerId("CENTRO-SANTIAGO-CENTRO")
                .status(status)
                .estimatedWaitTimeMinutes(15)
                .timestamp(LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME))
                .traceId("trace-kafka-" + UUID.randomUUID().toString().substring(0, 8))
                .build();

        kafkaEventPublisher.publishTelemetry(event);

        return ResponseEntity.ok(Map.of(
                "status", "SUCCESS",
                "message", "Evento de telemetría clínica publicado exitosamente en Kafka",
                "topic", "vidasalud.telemetry.appointments",
                "event", event
        ));
    }

    /**
     * Endpoint para publicar log de auditoría a Kafka ('vidasalud.audit.logs').
     */
    @PostMapping("/publish-audit")
    public ResponseEntity<?> publishAudit(@RequestBody(required = false) Map<String, Object> payload) {
        String eventId = UUID.randomUUID().toString();
        String userEmail = (payload != null && payload.containsKey("userEmail"))
                ? payload.get("userEmail").toString() : "admin.general@vidasalud.cl";
        String action = (payload != null && payload.containsKey("action"))
                ? payload.get("action").toString() : "ACCESS_STREAMING_DASHBOARD";
        String details = (payload != null && payload.containsKey("details"))
                ? payload.get("details").toString() : "Auditoría en tiempo real generada desde consola";

        AuditLogEventDTO event = AuditLogEventDTO.builder()
                .eventId(eventId)
                .eventType("SECURITY_AUDIT_STREAM")
                .userEmail(userEmail)
                .userRole("Admin")
                .action(action)
                .resource("/api/admin/kafka")
                .details(details)
                .ipAddress("127.0.0.1")
                .status("SUCCESS")
                .timestamp(LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME))
                .build();

        kafkaEventPublisher.publishAudit(event);

        return ResponseEntity.ok(Map.of(
                "status", "SUCCESS",
                "message", "Log de auditoría publicado exitosamente en Kafka",
                "topic", "vidasalud.audit.logs",
                "event", event
        ));
    }

    /**
     * Endpoint para simular error o payload defectuoso para probar reintentos en Kafka (Indicador 6).
     */
    @PostMapping("/publish-error")
    public ResponseEntity<?> publishError() {
        String eventId = UUID.randomUUID().toString();
        AppointmentTelemetryEventDTO event = AppointmentTelemetryEventDTO.builder()
                .eventId(eventId)
                .appointmentId(99999L)
                .patientRut("00.000.000-0")
                .patientName("ERROR_SIMULATION_PAYLOAD")
                .specialty("DESCONOCIDA")
                .status("ERROR_RETRY_TEST")
                .timestamp(LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME))
                .build();

        kafkaEventPublisher.publishTelemetry(event);

        return ResponseEntity.ok(Map.of(
                "status", "SUCCESS",
                "message", "Evento con payload de error simulado publicado para verificar reintentos del consumidor",
                "topic", "vidasalud.telemetry.appointments",
                "event", event
        ));
    }
}
