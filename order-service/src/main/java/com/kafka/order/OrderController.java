package com.kafka.order;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@Slf4j
@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderService orderService;

    @PostMapping
    public ResponseEntity<Order> createOrder(@RequestBody OrderRequest request) {
        log.info("Получен запрос на заказ: userId={}, amount={}",
                request.getUserId(), request.getAmount());

        Order order = orderService.createOrder(request.getUserId(), request.getAmount());

        return ResponseEntity.status(HttpStatus.CREATED).body(order);
    }

    @GetMapping("/{orderId}")
    public ResponseEntity<Order> getOrder(@PathVariable Long orderId) {
        Order order = orderService.getOrder(orderId);

        if (order == null) {
            return ResponseEntity.notFound().build();
        }

        return ResponseEntity.ok(order);
    }

    @PutMapping("/{orderId}/status")
    public ResponseEntity<Order> updateStatus(
            @PathVariable Long orderId,
            @RequestParam String status) {

        log.info("Запрос на обновление статуса: orderId={}, status={}", orderId, status);

        try {
            Order order = orderService.updateStatus(orderId, status);
            return ResponseEntity.ok(order);
        } catch (RuntimeException e) {
            log.error("Ошибка обновления статуса: {}", e.getMessage());
            return ResponseEntity.notFound().build();
        }
    }

}
