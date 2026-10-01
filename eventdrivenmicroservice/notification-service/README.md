# Notification Service

Spring Boot microservice that listens to order events from RabbitMQ and sends order confirmation notifications (email/SMS).

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

The service starts on **port 8083**.

---

## RabbitMQ Configuration

| Property | Value |
|----------|-------|
| Exchange | `order.events.exchange` |
| Queue | `notification.send.queue` |
| Routing Key | `notification.send.routing.key` |

---

## Event Flow

```
order.events.exchange
        ↓
notification.send.queue
        ↓
  NotificationConsumer  →  sends order confirmation email/SMS
```

---

## Project Structure

```
notification-service/
├── src/main/java/com/ms/notificationservice/
│   ├── NotificationServiceApplication.java
│   ├── config/
│   │   └── RabbitMQConfig.java
│   ├── consumer/
│   │   └── NotificationConsumer.java
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
