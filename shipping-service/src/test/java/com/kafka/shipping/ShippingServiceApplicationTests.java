package com.kafka.shipping;

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
@EmbeddedKafka(partitions = 1, topics = {"payed_orders", "sent_orders", "out_of_stock_orders"})
class ShippingServiceApplicationTests {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private ConsumerFactory<String, String> consumerFactory;

    private static final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    @Test
    void testContextLoads() {
        assertThat(true).isTrue();
    }

    @Test
    void shouldProcessPayedOrderAndSendToSentOrders() throws Exception {

        var consumer = consumerFactory.createConsumer("test-group-sent", "test-client-sent");
        consumer.subscribe(List.of("sent_orders"));
        consumer.poll(Duration.ofMillis(100));

        PayedOrder order = new PayedOrder(1L, 42L, 1990, "PAYED", LocalDateTime.now());
        String message = objectMapper.writeValueAsString(order);
        String key = String.valueOf(order.getUserId());

        kafkaTemplate.send("payed_orders", key, message).get();

        var records = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(5));

        assertThat(records.count()).isGreaterThan(0);

        ShippingResult result = objectMapper.readValue(
                records.iterator().next().value(),
                ShippingResult.class
        );

        assertThat(result.getOrderId()).isEqualTo(1L);
        assertThat(result.getUserId()).isEqualTo(42L);
        assertThat(result.getShippingStatus()).isEqualTo("SENT");

        consumer.close();
    }

    @Test
    void shouldSendOutOfStockForEvenOrderId() throws Exception {

        var consumer = consumerFactory.createConsumer("test-group-out", "test-client-out");
        consumer.subscribe(List.of("out_of_stock_orders"));
        consumer.poll(Duration.ofMillis(100));

        PayedOrder order = new PayedOrder(2L, 43L, 3000, "PAYED", LocalDateTime.now());
        String message = objectMapper.writeValueAsString(order);
        String key = String.valueOf(order.getUserId());

        kafkaTemplate.send("payed_orders", key, message).get();

        var records = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(5));

        assertThat(records.count()).isGreaterThan(0);

        ShippingResult result = objectMapper.readValue(
                records.iterator().next().value(),
                ShippingResult.class
        );

        assertThat(result.getOrderId()).isEqualTo(2L);
        assertThat(result.getShippingStatus()).isEqualTo("OUT_OF_STOCK");

        consumer.close();
    }
}