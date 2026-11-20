package com.example.ms_vidasalud_admin.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class KafkaConsumerGroupDTO {
    private String groupId;
    private String state;
    private boolean isSimpleConsumerGroup;
}