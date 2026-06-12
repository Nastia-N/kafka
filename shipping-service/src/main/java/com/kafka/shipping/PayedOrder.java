package com.kafka.shipping;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class PayedOrder {
    private Long orderId;
    private Long userId;
    private Integer amount;
    private String paymentStatus;
    private LocalDateTime timestamp;
}
