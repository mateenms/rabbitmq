package com.ms.orderservice.controller;

import com.ms.orderservice.dto.OrderDTO;
import com.ms.orderservice.dto.OrderEvent;
import com.ms.orderservice.publisher.OrderProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST controller to handle incoming order requests.
 *
 * @author Mohammad Mateen
 */
@RestController
@RequestMapping("/api/v1")
public class OrderController {

    private final OrderProducer orderProducer;

    @Autowired
    public OrderController(OrderProducer orderProducer) {
        this.orderProducer = orderProducer;
    }

    /**
     * Accepts an Order from the client, wraps it in an OrderEvent, and publishes to RabbitMQ.
     * POST
     */
    @PostMapping("/orders")
    public ResponseEntity<String> placeOrder(@RequestBody OrderDTO order) {
        OrderEvent orderEvent = new OrderEvent();
        orderEvent.setStatus("PENDING");
        orderEvent.setMessage("Order placed successfully");
        orderEvent.setOrder(order);
        orderProducer.sendOrderEvent(orderEvent);
        return ResponseEntity.ok("Order placed and event published successfully");
    }

}
