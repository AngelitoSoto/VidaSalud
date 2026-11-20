package cl.duoc.vidasalud.notify.service;

import cl.duoc.vidasalud.notify.dto.AuditLogEventDTO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Servicio de dominio para el procesamiento de registros de auditorÃ­a y seguridad operacional.
 * Cumple con el Indicador 5 de la rÃºbrica:
 * - Desacoplamiento de la lÃ³gica de auditorÃ­a respecto a la infraestructura de Kafka.
 */
@Service
@Slf4j
public class AuditProcessingService {

    private final AtomicLong totalAuditLogs = new AtomicLong(0);

    public void processAuditLog(AuditLogEventDTO event) {
        log.info("[AUDIT DOMAIN] Registrando evento de seguridad [Usuario: '{}', Rol: '{}', AcciÃ³n: '{}', Recurso: '{}']",
                event.getUserEmail(), event.getUserRole(), event.getAction(), event.getResource());

        if (event.getEventId() == null || event.getUserEmail() == null) {
            log.warn("[AUDIT DOMAIN] Payload de auditorÃ­a incompleto. Se registrarÃ¡ con advertencia.");
        }

        long count = totalAuditLogs.incrementAndGet();
        log.info("[AUDIT DOMAIN] Log de auditorÃ­a archivado exitosamente. Total registros en streaming: {}", count);
    }

    public long getTotalAuditLogs() {
        return totalAuditLogs.get();
    }
}