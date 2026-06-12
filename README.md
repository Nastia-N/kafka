# Kafka Order Processing System

Микросервисная система обработки заказов с использованием Apache Kafka.

## Архитектура

Order → new_orders → Payment → payed_orders → Shipping → sent_orders → Notifications

## Технологии

- Java 21
- Spring Boot 3.2
- Apache Kafka
- Docker