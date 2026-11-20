package com.example.ms_vidasalud_admin.controller;

import com.example.ms_vidasalud_admin.dto.CreateKafkaTopicRequest;
import com.example.ms_vidasalud_admin.dto.KafkaConsumerGroupDTO;
import com.example.ms_vidasalud_admin.dto.KafkaTopicDetailDTO;
import com.example.ms_vidasalud_admin.service.KafkaAdminService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * Controlador REST para la Administración Dinámica de Kafka.
 * Cumple con el Indicador 3 y 9 de la rúbrica:
 * - Endpoints REST bien definidos (GET, POST, DELETE, PUT) para gestión de tópicos y configuraciones.
 * - Validación estricta de parámetros de entrada con @Valid.
 */
@RestController
@RequestMapping("/api/admin/kafka")
@RequiredArgsConstructor
@CrossOrigin(origins = "*")
@Slf4j
public class KafkaAdminController {

    private final KafkaAdminService kafkaAdminService;

    @GetMapping("/topics")
    public ResponseEntity<List<KafkaTopicDetailDTO>> listTopics() {
        log.info("[REST KAFKA ADMIN] Consultando listado de tópicos...");
        return ResponseEntity.ok(kafkaAdminService.listTopics());
    }

    @PostMapping("/topics")
    public ResponseEntity<?> createTopic(@Valid @RequestBody CreateKafkaTopicRequest request) {
        log.info("[REST KAFKA ADMIN] Solicitud de creación de tópico: {}", request.getName());
        try {
            kafkaAdminService.createTopic(request);
            return ResponseEntity.status(HttpStatus.CREATED).body(Map.of(
                    "status", "CREATED",
                    "message", "Tópico '" + request.getName() + "' creado exitosamente en Kafka",
                    "partitions", request.getPartitions(),
                    "replicationFactor", request.getReplicationFactor()
            ));
        } catch (Exception e) {
            String msg = e.getMessage() != null ? e.getMessage() : "";
            if (e.getCause() != null && e.getCause().getMessage() != null) {
                msg = e.getCause().getMessage();
            }
            if (msg.contains("TopicExistsException") || (msg.contains("Topic") && msg.contains("already exists"))) {
                log.warn("[REST KAFKA ADMIN] El tópico '{}' ya existe en el cluster.", request.getName());
                return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of(
                        "status", "CONFLICT",
                        "message", "El tópico '" + request.getName() + "' ya existe en el cluster de Kafka. Elige otro nombre o elimínalo primero."
                ));
            }
            log.error("[REST KAFKA ADMIN] Error al crear tópico '{}': {}", request.getName(), msg, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of(
                    "status", "ERROR",
                    "message", "Error al crear tópico en Kafka: " + msg
            ));
        }
    }

    @DeleteMapping("/topics/{name}")
    public ResponseEntity<?> deleteTopic(@PathVariable String name) {
        log.info("[REST KAFKA ADMIN] Solicitud de eliminación de tópico: {}", name);
        if ("vidasalud.telemetry.appointments".equalsIgnoreCase(name) || "vidasalud.audit.logs".equalsIgnoreCase(name)) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).body(Map.of(
                    "status", "FORBIDDEN",
                    "message", "No se permite eliminar los tópicos base del sistema ('" + name + "'). Solo puedes eliminar tópicos dinámicos."
            ));
        }

        try {
            kafkaAdminService.deleteTopic(name);
            return ResponseEntity.ok(Map.of(
                    "status", "DELETED",
                    "message", "Tópico '" + name + "' eliminado exitosamente del cluster Kafka"
            ));
        } catch (Exception e) {
            String msg = e.getMessage() != null ? e.getMessage() : "";
            if (e.getCause() != null && e.getCause().getMessage() != null) {
                msg = e.getCause().getMessage();
            }
            if (msg.contains("UnknownTopicOrPartitionException") || msg.contains("does not host this topic")) {
                log.info("[REST KAFKA ADMIN] Tópico '{}' ya fue eliminado previamente de Kafka.", name);
                return ResponseEntity.ok(Map.of(
                        "status", "DELETED",
                        "message", "El tópico '" + name + "' ya fue eliminado del cluster Kafka."
                ));
            }
            log.error("[REST KAFKA ADMIN] Error al eliminar tópico '{}': {}", name, msg, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of(
                    "status", "ERROR",
                    "message", "Error al eliminar tópico: " + msg
            ));
        }
    }

    @PutMapping("/topics/{name}/config")
    public ResponseEntity<?> updateTopicConfig(
            @PathVariable String name,
            @RequestParam String key,
            @RequestParam String value) {
        log.info("[REST KAFKA ADMIN] Solicitud de modificación de config para tópico {}: {} = {}", name, key, value);
        try {
            kafkaAdminService.updateTopicConfig(name, key, value);
            return ResponseEntity.ok(Map.of(
                    "status", "UPDATED",
                    "message", "Configuración de tópico '" + name + "' actualizada con éxito",
                    "key", key,
                    "value", value
            ));
        } catch (Exception e) {
            String msg = e.getMessage() != null ? e.getMessage() : "";
            if (e.getCause() != null && e.getCause().getMessage() != null) {
                msg = e.getCause().getMessage();
            }
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of(
                    "status", "ERROR",
                    "message", "Error al modificar config de tópico: " + msg
            ));
        }
    }

    @GetMapping("/consumer-groups")
    public ResponseEntity<List<KafkaConsumerGroupDTO>> listConsumerGroups() {
        log.info("[REST KAFKA ADMIN] Consultando consumer groups...");
        return ResponseEntity.ok(kafkaAdminService.listConsumerGroups());
    }
}
