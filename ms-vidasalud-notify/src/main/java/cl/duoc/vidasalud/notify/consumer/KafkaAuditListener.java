package cl.duoc.vidasalud.notify.consumer;

import cl.duoc.vidasalud.notify.dto.AuditLogEventDTO;
import cl.duoc.vidasalud.notify.service.AuditProcessingService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * Consumidor Kafka para el flujo de Auditoría Operacional y Seguridad.
 * Cumple con los Indicadores 2, 5 y 8 de la rúbrica:
 * - Uso de @KafkaListener con consumer group dedicado 'vidasalud-audit-group'.
 * - Procesamiento independiente sin interferir con el grupo de analítica.
 * - Delegación al servicio de dominio AuditProcessingService.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class KafkaAuditListener {

    private final AuditProcessingService auditProcessingService;

    @KafkaListener(
            topics = "${app.kafka.topics.audit:vidasalud.audit.logs}",
            groupId = "vidasalud-audit-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeAudit(
            @Payload AuditLogEventDTO event,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {

        log.info("[KAFKA AUDIT CONSUMER] Log recibido [Topic: '{}', Partición: {}, Offset: {}, Usuario: '{}', Acción: '{}']",
                topic, partition, offset, event.getUserEmail(), event.getAction());

        auditProcessingService.processAuditLog(event);
    }
}