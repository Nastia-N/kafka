package com.kafka.shipping;

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
public class ShippingConsumer {

    private final ShippingProducer shippingProducer;
    private static final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    @KafkaListener(
            topics = "${kafka.topic.payed-orders}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumePayedOrder(String message, Acknowledgment ack) {
        try {
            PayedOrder payedOrder = objectMapper.readValue(message, PayedOrder.class);

            if (!"PAYED".equals(payedOrder.getPaymentStatus())) {
                log.info("Пропускаем неуспешный платёж: orderId={}, paymentStatus={}",
                        payedOrder.getOrderId(), payedOrder.getPaymentStatus());
                ack.acknowledge();
                return;
            }

            log.info("Получен заказ на отгрузку: orderId={}, userId={}, amount={}",
                    payedOrder.getOrderId(), payedOrder.getUserId(), payedOrder.getAmount());

            ShippingResult result = processShipping(payedOrder);

            if ("OUT_OF_STOCK".equals(result.getShippingStatus())) {
                shippingProducer.sendOutOfStock(result);
            } else {
                shippingProducer.sendSuccess(result);
            }

            ack.acknowledge();

            log.info("Отгрузка обработана: orderId={}, shippingStatus={}",
                    payedOrder.getOrderId(), result.getShippingStatus());

        } catch (Exception e) {
            log.error("Ошибка обработки отгрузки: {}", e.getMessage());
            throw new RuntimeException("Ошибка обработки отгрузки", e);
        }
    }

    private ShippingResult processShipping(PayedOrder order) {
        boolean inStock = order.getOrderId() % 2 != 0;

        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        String status = inStock ? "SENT" : "OUT_OF_STOCK";

        return new ShippingResult(
                order.getOrderId(),
                order.getUserId(),
                order.getAmount(),
                status,
                LocalDateTime.now()
        );
    }
}
