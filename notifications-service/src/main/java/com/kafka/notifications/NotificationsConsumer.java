package com.kafka.notifications;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class NotificationsConsumer {

    private static final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    @KafkaListener(
            topics = "${kafka.topic.sent-orders}",
            groupId = "${spring.kafka.consumer.group-id}"
    )
    public void consumeShippingResult(String message) {
        try {
            ShippingResult result = objectMapper.readValue(message, ShippingResult.class);

            if ("SENT".equals(result.getShippingStatus())) {
                log.info("Уведомление отправлено пользователю: userId={}, заказ orderId={} отправлен!",
                        result.getUserId(), result.getOrderId());
            } else {
                log.info("Уведомление: заказ orderId={}, статус={} (уведомление не требуется)",
                        result.getOrderId(), result.getShippingStatus());
            }

        } catch (Exception e) {
            log.error("Ошибка обработки уведомления: {}", e.getMessage());
        }
    }
}
