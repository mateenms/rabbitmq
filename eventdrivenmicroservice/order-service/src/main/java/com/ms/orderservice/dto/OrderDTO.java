package com.ms.orderservice.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;
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

    @NotBlank(message = "Order ID is required")
    private String orderId;

    @NotBlank(message = "Item name is required")
    private String name;

    @Min(value = 1, message = "Quantity must be at least 1")
    private int quantity;

    @Positive(message = "Price must be greater than 0")
    private double price;

}
