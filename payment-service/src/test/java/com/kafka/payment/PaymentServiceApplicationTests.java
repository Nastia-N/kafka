package com.kafka.payment;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(partitions = 1, topics = {"new_orders", "payed_orders"})
class PaymentServiceApplicationTests {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private ConsumerFactory<String, String> consumerFactory;

    private static final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    @Test
    void contextLoads() {
        assertThat(true).isTrue();
    }

    @Test
    void shouldProcessPaymentAndSendToPayedOrders() throws Exception {

        var consumer = consumerFactory.createConsumer("test-payment-group", "test-client");
        consumer.subscribe(List.of("payed_orders"));
        consumer.poll(Duration.ofMillis(100));

        Order order = new Order(1L, 42L, 1990, "NEW", LocalDateTime.now());
        String message = objectMapper.writeValueAsString(order);
        String key = String.valueOf(order.getUserId());

        kafkaTemplate.send("new_orders", key, message).get();

        var records = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(5));

        assertThat(records.count()).isGreaterThan(0);

        PaymentResult result = objectMapper.readValue(
                records.iterator().next().value(),
                PaymentResult.class
        );

        assertThat(result.getOrderId()).isEqualTo(1L);
        assertThat(result.getUserId()).isEqualTo(42L);
        assertThat(result.getPaymentStatus()).isEqualTo("PAYED");

        consumer.close();
    }
}