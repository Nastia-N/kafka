package com.kafka.order;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderProducer {

    private static final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    @Value("${kafka.topic.new-orders}")
    private String topic;

    private final KafkaTemplate<String, String> kafkaTemplate;

    public void sendOrder(Order order) {
        String key = String.valueOf(order.getUserId());

        try {
            String payload = objectMapper.writeValueAsString(order);

            kafkaTemplate.send(topic, key, payload)
                    .whenComplete((result, ex) -> {
                        if (ex == null) {
                            log.info("Заказ отправлен в Kafka: orderId={}, userId={}, topic={}, partition={}, offset={}",
                                    order.getOrderId(),
                                    order.getUserId(),
                                    topic,
                                    result.getRecordMetadata().partition(),
                                    result.getRecordMetadata().offset());
                        } else {
                            log.error("Не удалось отправить заказ в Kafka: orderId={}", order.getOrderId(), ex);
                        }
                    });
        } catch (Exception e) {
            log.error("Не удалось сериализовать заказ: orderId={}", order.getOrderId(), e);
        }
    }
}
