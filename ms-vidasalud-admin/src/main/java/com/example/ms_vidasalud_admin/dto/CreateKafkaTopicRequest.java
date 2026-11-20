package com.example.ms_vidasalud_admin.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.Map;

/**
 * DTO para la creación dinámica de tópicos de Kafka.
 * Cumple con validaciones de entrada para evitar nombres vacíos o configuraciones inválidas.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CreateKafkaTopicRequest {

    @NotBlank(message = "El nombre del tópico es obligatorio y no puede estar vacío.")
    @Pattern(regexp = "^[a-zA-Z0-9._-]+$", message = "El nombre del tópico solo puede contener letras, números, puntos, guiones y guiones bajos.")
    private String name;

    @Min(value = 1, message = "El número de particiones debe ser al menos 1.")
    @Max(value = 50, message = "El número de particiones no puede exceder 50.")
    @Builder.Default
    private int partitions = 3;

    @Min(value = 1, message = "El factor de replicación debe ser al menos 1.")
    @Builder.Default
    private short replicationFactor = 1;

    @Builder.Default
    private Long retentionMs = 604800000L; // 7 días por defecto

    @Builder.Default
    private String cleanupPolicy = "delete";

    @Builder.Default
    private Map<String, String> additionalConfigs = new HashMap<>();
}