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
