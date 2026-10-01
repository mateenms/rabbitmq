# Inventory Service

Spring Boot microservice that listens to order events from RabbitMQ and updates stock/inventory accordingly.

---

## Tech Stack

- Java 17
- Spring Boot 4.1.1
- Spring AMQP (RabbitMQ)
- Lombok
- Jackson

---

## Prerequisites

- RabbitMQ on `localhost:5672` (default guest/guest credentials)
- Java 17+
- Maven 3.x
- **order-service must be running** to publish events

---

## How to Run

**1. Build the project**
```bash
mvn clean install
```

**2. Run the application**
```bash
mvn spring-boot:run
```

The service starts on **port 8082**.

---

## RabbitMQ Configuration

| Property | Value |
|----------|-------|
| Exchange | `order.events.exchange` |
| Queue | `inventory.update.queue` |
| Routing Key | `inventory.update.routing.key` |

---

## Event Flow

```
order.events.exchange
        ↓
inventory.update.queue
        ↓
  InventoryConsumer  →  reduces stock for the ordered item
```

---

## Project Structure

```
inventory-service/
├── src/main/java/com/ms/inventoryservice/
│   ├── InventoryServiceApplication.java
│   ├── config/
│   │   └── RabbitMQConfig.java
│   ├── consumer/
│   │   └── InventoryConsumer.java
│   └── dto/
│       ├── Order.java
│       └── OrderEvent.java
└── src/main/resources/
    └── application.yaml
```

---

## Author

**Mohammad Mateen**

Full Stack Java Developer | Microservices Architect | AWS Certified Solutions Architect

Experienced in designing and building scalable, cloud-native enterprise applications using Java, Spring Boot, and microservices architecture. Passionate about event-driven systems, distributed computing, and clean software design.

**Core Expertise:**
- Java & Spring Boot (REST APIs, Spring AMQP, Spring Security, Spring Data)
- Microservices Architecture & Event-Driven Design (RabbitMQ, Kafka)
- AWS Cloud Solutions (Certified Solutions Architect)
- Full Stack Development (React, Angular, REST, GraphQL)
- Containerization & Orchestration (Docker, Kubernetes)
- CI/CD Pipelines & DevOps practices

📧 javamateen@gmail.com
🔗 [GitHub](https://github.com/mateenms)
