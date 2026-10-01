# Event-Driven Microservices with RabbitMQ

A Spring Boot event-driven microservices architecture using RabbitMQ as the message broker. When an order is placed, the order-service publishes an event that is consumed independently by inventory-service and notification-service.

---

## Architecture

```
Client (Postman)
      ↓
 order-service (port 8081)  →  order.events.exchange (TopicExchange)
                                         ↓
                        ┌────────────────┴────────────────┐
                        ↓                                 ↓
          inventory.update.queue            notification.send.queue
                        ↓                                 ↓
          inventory-service (8082)      notification-service (8083)
          reduces stock                 sends confirmation
```

---

## Services

| Service | Port | Role |
|---------|------|------|
| order-service | 8081 | Producer — receives orders, publishes events |
| inventory-service | 8082 | Consumer — updates stock on order |
| notification-service | 8083 | Consumer — sends email/SMS on order |

---

## Prerequisites

- Java 17+
- Maven 3.x
- RabbitMQ running on `localhost:5672`

### Start RabbitMQ (Docker)
```bash
docker run -d --name rabbitmq \
  -p 5672:5672 \
  -p 15672:15672 \
  rabbitmq:management
```

---

## How to Run

Start each service in a separate terminal:

**Terminal 1 — order-service**
```bash
cd order-service
mvn spring-boot:run
```

**Terminal 2 — inventory-service**
```bash
cd inventory-service
mvn spring-boot:run
```

**Terminal 3 — notification-service**
```bash
cd notification-service
mvn spring-boot:run
```

---

## Test the Flow

Send a POST request to order-service:

```
POST http://localhost:8081/api/v1/orders
Content-Type: application/json
```

```json
{
  "orderId": "ORD-001",
  "name": "iPhone 15",
  "quantity": 2,
  "price": 999.99
}
```

The service sets `status = "PENDING"` and `message = "Order placed successfully"` automatically before publishing the event.

Check the logs of inventory-service and notification-service — both will log the received event.

---

## RabbitMQ Management UI

```
http://localhost:15672
Username: guest
Password: guest
```

---

## Project Structure

```
eventdrivenmicroservice/
├── order-service/          → places orders, publishes events
├── inventory-service/      → consumes events, updates stock
├── notification-service/   → consumes events, sends notifications
└── README.md
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
