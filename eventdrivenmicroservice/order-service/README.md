# Order Service

Spring Boot microservice responsible for placing orders and publishing order events to RabbitMQ.

---

## Tech Stack

- Java 17
- Spring Boot 4.1.1
- Spring AMQP (RabbitMQ)
- Lombok
- Jackson

---

## Prerequisites

Make sure the following are running before starting:

- RabbitMQ on `localhost:5672` (default guest/guest credentials)
- Java 17+
- Maven 3.x

---

## How to Run

**1. Clone the repo**
```bash
git clone https://github.com/mateenms/rabbitmq.git
cd rabbitmq/eventdrivenmicroservice/order-service
```

**2. Build the project**
```bash
mvn clean install
```

**3. Run the application**
```bash
mvn spring-boot:run
```

The service starts on **port 8081**.

---

## API Endpoints

### Place an Order
```
POST http://localhost:8081/api/v1/orders
Content-Type: application/json
```

**Request Body (only Order fields — status and message are set by the service):**
```json
{
  "orderId": "ORD-001",
  "name": "iPhone 15",
  "quantity": 2,
  "price": 999.99
}
```

**Response:**
```
Order placed and event published successfully
```

---

## RabbitMQ Configuration

| Property | Value |
|----------|-------|
| Exchange | `order.events.exchange` |
| Order Queue | `order.placed.queue` |
| Inventory Queue | `inventory.update.queue` |
| Notification Queue | `notification.send.queue` |
| Order Routing Key | `order.placed.routing.key` |
| Inventory Routing Key | `inventory.update.routing.key` |
| Notification Routing Key | `notification.send.routing.key` |

---

## RabbitMQ Management UI

Access the RabbitMQ dashboard at:
```
http://localhost:15672
```
Username: `guest`  
Password: `guest`

---

## Project Structure

```
order-service/
├── src/main/java/com/ms/orderservice/
│   ├── OrderServiceApplication.java
│   ├── config/
│   │   └── RabbitMQConfig.java
│   ├── controller/
│   │   └── OrderController.java
│   ├── dto/
│   │   ├── Order.java        ← received from client
│   │   └── OrderEvent.java   ← wraps Order with status & message
│   └── publisher/
│       └── OrderProducer.java
└── src/main/resources/
    └── application.yaml
```

---

## Event Flow

```
POST /api/v1/orders  (body: Order)
       ↓
 OrderController  →  builds OrderEvent (status=PENDING, message=...)
       ↓
  OrderProducer  →  order.events.exchange
                          ↓
           ┌──────────────┴──────────────┐
           ↓                             ↓
  inventory.update.queue     notification.send.queue
           ↓                             ↓
   inventory-service          notification-service
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
