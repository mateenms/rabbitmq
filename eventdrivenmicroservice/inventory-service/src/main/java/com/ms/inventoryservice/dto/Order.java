package com.ms.inventoryservice.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Represents the order details received inside an OrderEvent.
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
