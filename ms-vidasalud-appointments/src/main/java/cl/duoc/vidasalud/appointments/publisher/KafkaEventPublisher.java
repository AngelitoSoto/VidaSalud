package cl.duoc.vidasalud.appointments.publisher;

import cl.duoc.vidasalud.appointments.model.dto.AppointmentTelemetryEventDTO;
import cl.duoc.vidasalud.appointments.model.dto.AuditLogEventDTO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Component
@RequiredArgsConstructor
@Slf4j
public class KafkaEventPublisher {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${app.kafka.topics.telemetry:vidasalud.telemetry.appointments}")
    private String telemetryTopic;

    @Value("${app.kafka.topics.audit:vidasalud.audit.logs}")
    private String auditTopic;

    public CompletableFuture<SendResult<String, Object>> publishTelemetry(AppointmentTelemetryEventDTO event) {
        String key = event.getPatientRut() != null ? event.getPatientRut() : String.valueOf(event.getAppointmentId());
        log.info("[KAFKA PRODUCER] Publicando telemetría a tópico '{}' [Key: '{}', EventId: '{}', Atención: #{}]",
                telemetryTopic, key, event.getEventId(), event.getAppointmentId());

        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(telemetryTopic, key, event);
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("[KAFKA PRODUCER] Telemetría enviada exitosamente [Topic: '{}', Partición: {}, Offset: {}, EventId: '{}']",
                        result.getRecordMetadata().topic(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset(),
                        event.getEventId());
            } else {
                log.error("[KAFKA PRODUCER] Error enviando telemetría [EventId: '{}']: {}",
                        event.getEventId(), ex.getMessage(), ex);
            }
        });
        return future;
    }

    public CompletableFuture<SendResult<String, Object>> publishAudit(AuditLogEventDTO event) {
        String key = event.getUserEmail() != null ? event.getUserEmail() : event.getEventId();
        log.info("[KAFKA PRODUCER] Publicando log de auditoría a tópico '{}' [Key: '{}', Acción: '{}', Usuario: '{}']",
                auditTopic, key, event.getAction(), event.getUserEmail());

        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(auditTopic, key, event);
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("[KAFKA PRODUCER] Log enviado exitosamente [Topic: '{}', Partición: {}, Offset: {}, EventId: '{}']",
                        result.getRecordMetadata().topic(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset(),
                        event.getEventId());
            } else {
                log.error("[KAFKA PRODUCER] Error enviando auditoría [EventId: '{}']: {}",
                        event.getEventId(), ex.getMessage(), ex);
            }
        });
        return future;
    }
}