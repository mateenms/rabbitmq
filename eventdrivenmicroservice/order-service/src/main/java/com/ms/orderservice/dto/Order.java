package com.ms.orderservice.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * DTO representing an order placed by the client.
 *
 * @author Mohammad Mateen
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Order {

    private String orderId;
    private String name;
    private int quantity;
    private double price;

}
