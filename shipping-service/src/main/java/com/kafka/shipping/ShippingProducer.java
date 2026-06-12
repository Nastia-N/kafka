package com.kafka.shipping;

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
public class ShippingProducer {

    private static final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    @Value("${kafka.topic.sent-orders}")
    private String sentOrdersTopic;

    @Value("${kafka.topic.out-of-stock}")
    private String outOfStockTopic;

    private final KafkaTemplate<String, String> kafkaTemplate;

    public void sendSuccess(ShippingResult result) {
        sendToTopic(sentOrdersTopic, result, "SENT");
    }

    public void sendOutOfStock(ShippingResult result) {
        sendToTopic(outOfStockTopic, result, "OUT_OF_STOCK");
    }

    private void sendToTopic(String topic, ShippingResult result, String status) {
        String key = String.valueOf(result.getUserId());

        try {
            String payload = objectMapper.writeValueAsString(result);

            kafkaTemplate.send(topic, key, payload)
                    .whenComplete((metadata, ex) -> {
                        if (ex == null) {
                            log.info("Результат отгрузки отправлен: orderId={}, status={}, topic={}, partition={}, offset={}",
                                    result.getOrderId(), status, topic,
                                    metadata.getRecordMetadata().partition(),
                                    metadata.getRecordMetadata().offset());
                        } else {
                            log.error("Ошибка отправки результата отгрузки: orderId={}", result.getOrderId(), ex);
                        }
                    });
        } catch (Exception e) {
            log.error("Ошибка сериализации: orderId={}", result.getOrderId(), e);
        }
    }
}
