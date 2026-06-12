package com.kafka.order;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderProducer {

    private static final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    @Value("${kafka.topic.new-orders:new_orders}")
    private String newOrdersTopic;

    @Value("${kafka.topic.status-updates:order_status_updates}")
    private String statusUpdatesTopic;

    private final KafkaTemplate<String, String> kafkaTemplate;

    public void sendOrder(Order order) {
        String key = String.valueOf(order.getUserId());

        try {
            String payload = objectMapper.writeValueAsString(order);

            kafkaTemplate.send(newOrdersTopic, key, payload)
                    .whenComplete((result, ex) -> {
                        if (ex == null) {
                            log.info("Заказ отправлен в Kafka: orderId={}, userId={}, topic={}, partition={}, offset={}",
                                    order.getOrderId(),
                                    order.getUserId(),
                                    newOrdersTopic,
                                    result.getRecordMetadata().partition(),
                                    result.getRecordMetadata().offset());
                        } else {
                            log.error("Ошибка отправки заказа: orderId={}", order.getOrderId(), ex);
                        }
                    });
        } catch (Exception e) {
            log.error("Ошибка сериализации заказа: orderId={}", order.getOrderId(), e);
        }
    }

    public void sendStatusUpdate(Order order) {
        String key = String.valueOf(order.getUserId());

        try {
            Map<String, Object> statusEvent = new HashMap<>();
            statusEvent.put("eventType", "ORDER_STATUS_UPDATED");
            statusEvent.put("orderId", order.getOrderId());
            statusEvent.put("userId", order.getUserId());
            statusEvent.put("status", order.getStatus());
            statusEvent.put("timestamp", LocalDateTime.now().toString());

            String payload = objectMapper.writeValueAsString(statusEvent);

            kafkaTemplate.send(statusUpdatesTopic, key, payload)
                    .whenComplete((result, ex) -> {
                        if (ex == null) {
                            log.info("Статус заказа отправлен в Kafka: orderId={}, status={}, topic={}, partition={}, offset={}",
                                    order.getOrderId(),
                                    order.getStatus(),
                                    statusUpdatesTopic,
                                    result.getRecordMetadata().partition(),
                                    result.getRecordMetadata().offset());
                        } else {
                            log.error("Ошибка отправки статуса: orderId={}", order.getOrderId(), ex);
                        }
                    });
        } catch (Exception e) {
            log.error("Ошибка сериализации статуса: orderId={}", order.getOrderId(), e);
        }
    }
}
