# RabbitMQ Learning Repository

A hands-on repository for learning and implementing **RabbitMQ** with **Spring Boot**, covering basic concepts through a full event-driven microservices architecture.

---

## Repository Structure

```
rabbitmq/
├── rabbitmq01/                  → Basic RabbitMQ concepts (string & JSON messages)
└── eventdrivenmicroservice/     → Event-driven microservices architecture
    ├── order-service/           → Producer: places orders, publishes events
    ├── inventory-service/       → Consumer: receives order events, updates stock
    └── notification-service/    → Consumer: receives order events, sends notifications
```

---

## Projects

### 1. rabbitmq01 — Basics
Learn the fundamentals:
- Queues, Exchanges, Bindings, Routing Keys
- Publishing and consuming **String** messages
- Publishing and consuming **JSON** messages
- RabbitMQ configuration with Spring AMQP

📖 See [rabbitmq01 README](rabbitmq01/HELP.md)

---

### 2. eventdrivenmicroservice — Advanced
A production-style event-driven microservices system:
- **order-service** publishes order events to RabbitMQ
- **inventory-service** consumes events to update stock
- **notification-service** consumes events to send confirmations

📖 See [eventdrivenmicroservice README](eventdrivenmicroservice/README.md)  
📐 See [Architecture & Flow](eventdrivenmicroservice/ARCHITECTURE.md)

---

## Tech Stack

| Technology | Purpose |
|-----------|---------|
| Java 17 | Programming language |
| Spring Boot 4.1.1 | Application framework |
| Spring AMQP | RabbitMQ integration |
| RabbitMQ | Message broker |
| Lombok | Boilerplate reduction |
| Jackson | JSON serialization |
| Maven | Build tool |

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

RabbitMQ Management UI: [http://localhost:15672](http://localhost:15672)  
Default credentials: `guest / guest`

---

## Author

**Mohammad Mateen**

Full Stack Java Developer | Microservices Architect | AWS Certified Solutions Architect

📧 javamateen@gmail.com  
🔗 [GitHub](https://github.com/mateenms)
