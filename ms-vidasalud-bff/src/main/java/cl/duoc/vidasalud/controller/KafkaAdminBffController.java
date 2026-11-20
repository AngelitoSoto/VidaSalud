package cl.duoc.vidasalud.controller;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientResponseException;

import java.util.Map;

/**
 * BFF Proxy para la Administración Dinámica de Kafka.
 * Enruta las peticiones desde el Frontend / API Gateway hacia el microservicio ms-vidasalud-admin.
 */
@RestController
@RequestMapping("/api/admin/kafka")
@CrossOrigin(origins = "*")
public class KafkaAdminBffController {

    private final RestClient adminRestClient;

    public KafkaAdminBffController(@Qualifier("adminRestClient") RestClient adminRestClient) {
        this.adminRestClient = adminRestClient;
    }

    @GetMapping("/topics")
    public ResponseEntity<?> listTopics() {
        try {
            return adminRestClient.get()
                    .uri("/api/admin/kafka/topics")
                    .retrieve()
                    .toEntity(Object.class);
        } catch (RestClientResponseException ex) {
            return ResponseEntity.status(ex.getStatusCode())
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(ex.getResponseBodyAsString());
        }
    }

    @PostMapping("/topics")
    public ResponseEntity<?> createTopic(@RequestBody Map<String, Object> payload) {
        try {
            return adminRestClient.post()
                    .uri("/api/admin/kafka/topics")
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(payload)
                    .retrieve()
                    .toEntity(Object.class);
        } catch (RestClientResponseException ex) {
            return ResponseEntity.status(ex.getStatusCode())
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(ex.getResponseBodyAsString());
        }
    }

    @DeleteMapping("/topics/{name}")
    public ResponseEntity<?> deleteTopic(@PathVariable String name) {
        try {
            return adminRestClient.delete()
                    .uri("/api/admin/kafka/topics/{name}", name)
                    .retrieve()
                    .toEntity(Object.class);
        } catch (RestClientResponseException ex) {
            return ResponseEntity.status(ex.getStatusCode())
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(ex.getResponseBodyAsString());
        }
    }

    @PutMapping("/topics/{name}/config")
    public ResponseEntity<?> updateTopicConfig(
            @PathVariable String name,
            @RequestParam String key,
            @RequestParam String value) {
        try {
            return adminRestClient.put()
                    .uri(uriBuilder -> uriBuilder
                            .path("/api/admin/kafka/topics/{name}/config")
                            .queryParam("key", key)
                            .queryParam("value", value)
                            .build(name))
                    .retrieve()
                    .toEntity(Object.class);
        } catch (RestClientResponseException ex) {
            return ResponseEntity.status(ex.getStatusCode())
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(ex.getResponseBodyAsString());
        }
    }

    @GetMapping("/consumer-groups")
    public ResponseEntity<?> listConsumerGroups() {
        try {
            return adminRestClient.get()
                    .uri("/api/admin/kafka/consumer-groups")
                    .retrieve()
                    .toEntity(Object.class);
        } catch (RestClientResponseException ex) {
            return ResponseEntity.status(ex.getStatusCode())
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(ex.getResponseBodyAsString());
        }
    }
}
