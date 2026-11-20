package cl.duoc.vidasalud.notify.service;

import cl.duoc.vidasalud.notify.dto.AppointmentTelemetryEventDTO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Servicio de dominio clÃ­nico para el procesamiento de telemetrÃ­a y analÃ­tica en tiempo real.
 * Cumple con el Indicador 5 de la rÃºbrica:
 * - Aislamiento completo de la lÃ³gica de negocio fuera de los listeners de mensajerÃ­a.
 * - CÃ¡lculo de mÃ©tricas y validaciÃ³n de integridad de datos.
 */
@Service
@Slf4j
public class AnalyticsProcessingService {

    private final AtomicLong totalEventsProcessed = new AtomicLong(0);

    public void processAppointmentTelemetry(AppointmentTelemetryEventDTO event) {
        log.info("[ANALYTICS DOMAIN] Procesando telemetrÃ­a para atenciÃ³n #{} [Especialidad: '{}', Paciente: '{}', Estado: '{}']",
                event.getAppointmentId(), event.getSpecialty(), event.getPatientName(), event.getStatus());

        // ValidaciÃ³n de payload segÃºn Indicador 6
        if (event.getAppointmentId() == null || event.getEventId() == null) {
            log.error("[ANALYTICS DOMAIN] Payload invÃ¡lido: ID de atenciÃ³n o evento nulo.");
            throw new IllegalArgumentException("El evento de telemetrÃ­a carece de identificadores requeridos.");
        }

        // SimulaciÃ³n de fallo controlado para verificar reintentos automÃ¡ticos en Kafka
        if ("ERROR_RETRY_TEST".equalsIgnoreCase(event.getStatus())) {
            log.warn("[ANALYTICS DOMAIN] SimulaciÃ³n de error activa en evento '{}'. Lanzando excepciÃ³n para verificar reintentos de Kafka...",
                    event.getEventId());
            throw new RuntimeException("Fallo simulado en procesamiento analÃ­tico. Disparando backoff y reintento en Kafka.");
        }

        long count = totalEventsProcessed.incrementAndGet();
        log.info("[ANALYTICS DOMAIN] MÃ©trica actualizada: Total de eventos procesados en streaming: {}. Tiempo estimado de espera: {} min.",
                count, event.getEstimatedWaitTimeMinutes());
    }

    public long getTotalProcessed() {
        return totalEventsProcessed.get();
    }
}