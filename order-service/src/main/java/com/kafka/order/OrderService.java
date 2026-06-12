package com.kafka.order;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Service
public class OrderService {

    private final OrderProducer orderProducer;
    private final ConcurrentHashMap<Long, Order> orderStorage = new ConcurrentHashMap<>();
    private final AtomicLong orderIdGenerator = new AtomicLong(1);

    public OrderService(OrderProducer orderProducer) {
        this.orderProducer = orderProducer;
    }

    public Order createOrder(Long userId, Integer amount) {
        Long orderId = orderIdGenerator.getAndIncrement();

        Order order = new Order(
                orderId,
                userId,
                amount,
                "CREATED",
                LocalDateTime.now()
        );

        orderStorage.put(orderId, order);
        log.info("Заказ создан: orderId={}, userId={}, amount={}", orderId, userId, amount);
        orderProducer.sendOrder(order);
        return order;
    }

    public Order updateStatus(Long orderId, String status) {
        Order order = orderStorage.get(orderId);
        if (order == null) {
            throw new RuntimeException("Заказ не найден: " + orderId);
        }

        String oldStatus = order.getStatus();
        order.setStatus(status);
        orderStorage.put(orderId, order);

        log.info("Статус заказа обновлён: orderId={}, oldStatus={}, newStatus={}",
                orderId, oldStatus, status);
        orderProducer.sendStatusUpdate(order);
        return order;
    }

    public Order getOrder(Long orderId) {
        return orderStorage.get(orderId);
    }
}