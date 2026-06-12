package com.kafka.order;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@DirtiesContext
@EmbeddedKafka(partitions = 1, topics = {"new_orders"})
class OrderControllerTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Test
    void createOrderShouldReturnAccepted() {
        OrderRequest request = new OrderRequest();
        request.setUserId(42L);
        request.setAmount(1990);

        ResponseEntity<Order> response = restTemplate.postForEntity(
                "/api/orders",
                request,
                Order.class
        );

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().getOrderId()).isNotNull();
        assertThat(response.getBody().getUserId()).isEqualTo(42L);
        assertThat(response.getBody().getAmount()).isEqualTo(1990);
        assertThat(response.getBody().getStatus()).isEqualTo("CREATED");
    }
}