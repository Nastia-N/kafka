package com.kafka.payment;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Slf4j
@Service
@RequiredArgsConstructor
public class PaymentConsumer {

    private final PaymentProducer paymentProducer;
    private static final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    @KafkaListener(
            topics = "${kafka.topic.new-orders}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeOrder(String message, Acknowledgment ack) {
        try {
            Order order = objectMapper.readValue(message, Order.class);

            log.info("Получен заказ на оплату: orderId={}, userId={}, amount={}",
                    order.getOrderId(), order.getUserId(), order.getAmount());

            boolean paymentSuccess = processPayment(order);

            PaymentResult result = new PaymentResult(
                    order.getOrderId(),
                    order.getUserId(),
                    order.getAmount(),
                    paymentSuccess ? "PAYED" : "FAILED",
                    LocalDateTime.now()
            );

            paymentProducer.sendPaymentResult(result);

            ack.acknowledge();

            log.info("Обработанный платеж: orderId={}, status={}",
                    order.getOrderId(), result.getPaymentStatus());

        } catch (Exception e) {
            log.error("Ошибка при обработке платежа: {}", e.getMessage());
            throw new RuntimeException("Сбой в обработке платежа", e);
        }
    }

    private boolean processPayment(Order order) {
        if (order.getUserId() == 999L) {
            log.warn("Недостаточно средств у пользователя: {}", order.getUserId());
            return false;
        }

        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        return true;
    }
}
