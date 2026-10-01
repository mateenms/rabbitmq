package com.ms.notificationservice.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Event payload received from RabbitMQ when an order is placed.
 *
 * @author Mohammad Mateen
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class OrderEvent {

    private String status;
    private String message;
    private Order order;
}
