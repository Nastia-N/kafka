package com.kafka.order;

import lombok.Data;

@Data
public class OrderRequest {
    private Long userId;
    private Integer amount;
}