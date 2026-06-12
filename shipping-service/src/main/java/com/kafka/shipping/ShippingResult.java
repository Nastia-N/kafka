package com.kafka.shipping;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ShippingResult {
    private Long orderId;
    private Long userId;
    private Integer amount;
    private String shippingStatus;
    private LocalDateTime timestamp;
}