package cl.duoc.vidasalud.notify.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AuditLogEventDTO implements Serializable {
    private static final long serialVersionUID = 1L;

    private String eventId;
    private String eventType;
    private String userEmail;
    private String userRole;
    private String action;
    private String resource;
    private String details;
    private String ipAddress;
    private String status;
    private String timestamp;
}