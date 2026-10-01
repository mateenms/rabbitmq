package com.ms.rabbitmq01.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * DTO representing a user — serialized as JSON when sent over RabbitMQ.
 *
 * @author mateen
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class User {

    private int id;
    private String firstName;
    private String lastName;
    private String email;

}
