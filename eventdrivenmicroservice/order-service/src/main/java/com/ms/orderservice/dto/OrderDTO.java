package com.ms.orderservice.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * DTO representing an order placed by the client.
 * Received via REST API and wrapped inside OrderEvent before publishing to RabbitMQ.
 *
 * @author Mohammad Mateen
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class OrderDTO {

    private String orderId;
    private String name;
    private int quantity;
    private double price;

}
