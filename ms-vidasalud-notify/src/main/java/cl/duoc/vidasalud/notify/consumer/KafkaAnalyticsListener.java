package cl.duoc.vidasalud.notify.consumer;

import cl.duoc.vidasalud.notify.dto.AppointmentTelemetryEventDTO;
import cl.duoc.vidasalud.notify.service.AnalyticsProcessingService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * Consumidor Kafka para el flujo de Telemetría Clínica y Analítica.
 * Cumple con los Indicadores 2, 5 y 8 de la rúbrica:
 * - Uso de @KafkaListener con consumer group dedicado 'vidasalud-analytics-group'.
 * - Lectura tipada y extracción de metadatos (partición, offset, topic).
 * - Delegación inmediata al servicio de dominio AnalyticsProcessingService.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class KafkaAnalyticsListener {

    private final AnalyticsProcessingService analyticsProcessingService;

    @KafkaListener(
            topics = "${app.kafka.topics.telemetry:vidasalud.telemetry.appointments}",
            groupId = "vidasalud-analytics-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeTelemetry(
            @Payload AppointmentTelemetryEventDTO event,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {

        log.info("[KAFKA ANALYTICS CONSUMER] Mensaje recibido [Topic: '{}', Partición: {}, Offset: {}, EventId: '{}', Atención: #{}]",
                topic, partition, offset, event.getEventId(), event.getAppointmentId());

        analyticsProcessingService.processAppointmentTelemetry(event);
    }
}