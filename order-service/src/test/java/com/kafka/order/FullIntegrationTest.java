package com.kafka.order;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@DirtiesContext
@EmbeddedKafka(partitions = 1, topics = {"new_orders", "payed_orders", "sent_orders"})
class FullIntegrationTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    private static final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    @Test
    void fullOrderFlow() throws Exception {
        OrderRequest request = new OrderRequest();
        request.setUserId(42L);
        request.setAmount(1990);

        ResponseEntity<Order> response = restTemplate.postForEntity("/api/orders", request, Order.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(response.getBody()).isNotNull();
        Long orderId = response.getBody().getOrderId();

        assertThat(orderId).isNotNull();
        System.out.println("Полный интеграционный тест пройден, orderId=" + orderId);
    }
}
