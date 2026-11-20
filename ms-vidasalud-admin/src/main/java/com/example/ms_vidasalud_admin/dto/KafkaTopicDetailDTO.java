package com.example.ms_vidasalud_admin.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class KafkaTopicDetailDTO {
    private String name;
    private int partitions;
    private int replicationFactor;
    private boolean isInternal;
    private Map<String, String> configs;
}