package com.kafka.payment;

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
public class PaymentProducer {

    private static final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    @Value("${kafka.topic.payed-orders}")
    private String topic;

    private final KafkaTemplate<String, String> kafkaTemplate;

    public void sendPaymentResult(PaymentResult paymentResult) {
        String key = String.valueOf(paymentResult.getUserId());

        try {
            String payload = objectMapper.writeValueAsString(paymentResult);

            kafkaTemplate.send(topic, key, payload)
                    .whenComplete((result, ex) -> {
                        if (ex == null) {
                            log.info("Отправлен результат платежа: orderId={}, userId={}, status={}, partition={}, offset={}",
                                    paymentResult.getOrderId(),
                                    paymentResult.getUserId(),
                                    paymentResult.getPaymentStatus(),
                                    result.getRecordMetadata().partition(),
                                    result.getRecordMetadata().offset());
                        } else {
                            log.error("Не удалось отправить результат платежа: orderId={}", paymentResult.getOrderId(), ex);
                        }
                    });
        } catch (Exception e) {
            log.error("Не удалось сериализовать результат платежа: orderId={}", paymentResult.getOrderId(), e);
        }
    }
}
