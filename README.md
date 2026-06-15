# Kafka + Go — Learning Fundamentals

Hands-on exploration of Apache Kafka core concepts using Go, building toward a production-grade event-driven architecture.

This repo documents a structured, theory-first approach to learning Kafka — each concept is studied, then immediately implemented and verified with working Go code.

## Goals

- Build a solid mental model of Kafka's core primitives (topics, partitions, offsets, brokers)
- Implement producers and consumers in Go using `segmentio/kafka-go`
- Practice production-relevant patterns: manual offset commits, idempotent consumers, consumer groups, rebalancing
- Use this foundation for a follow-up project: an AI Agent Event Pipeline (Go microservices + Kafka + LLM agents)

## Tech stack

- **Go** (1.23+)
- **Apache Kafka** (KRaft mode, no Zookeeper) via Docker Compose
- **segmentio/kafka-go** — pure Go Kafka client
- **Kafka UI** (provectuslabs) — local web dashboard for inspecting topics/messages

## Project structure

```
kafka-go-learn/
├── docker-compose.yml     # Kafka broker + Kafka UI setup (KRaft mode)
├── producer/
│   └── main.go            # Producer with key-based partition routing, acks=all
├── consumer/
│   └── main.go            # Consumer with manual offset commit
└── consumer2/
    └── main.go            # Second consumer in same group — demonstrates rebalancing
```

## Setup

### 1. Start Kafka locally

```bash
docker compose up -d
```

This starts:
- A single Kafka broker (KRaft mode) on `localhost:9092`
- Kafka UI dashboard on `http://localhost:8080`

### 2. Create the topic

```bash
docker exec -it kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --partitions 3 \
  --replication-factor 1
```

### 3. Run the producer

```bash
go run ./producer
```

Produces sample "order" events, keyed by `restaurant_id` — messages with the same key always land in the same partition (preserves per-restaurant ordering).

### 4. Run the consumer(s)

```bash
go run ./consumer
```

In a second terminal, run a second consumer in the **same consumer group** to see Kafka rebalance partitions across them:

```bash
go run ./consumer2
```

## Concepts covered

| Concept | Where it's applied |
|---|---|
| Topics, partitions, offsets | Core mental model — `orders` topic, 3 partitions |
| Partition routing via key hashing | `Balancer: &kafka.Hash{}` + `Key: []byte(restaurantID)` |
| Producer acknowledgements (acks) | `RequiredAcks: kafka.RequireAll` |
| Consumer groups | `GroupID: "order-processor"` shared across consumer/consumer2 |
| Manual offset commits | `CommitInterval: 0` + `reader.CommitMessages()` after successful processing |
| At-least-once delivery semantics | Process first, commit after — message redelivered on failure |
| Consumer group rebalancing | Adding `consumer2` to the same group redistributes partitions |
| Replication & fault tolerance (theory) | Leader/follower, ISR — documented separately |
| Graceful shutdown | Context cancellation on `Ctrl+C` (`os/signal`) |

## Reference

A detailed theory write-up (with diagrams) covering topics, partitions, offset management, consumer group rebalancing, replication, and producer acks is available in [`Kafka_Theory_Guide.docx`](./Kafka_Theory_Guide.docx).

## Next steps

This repo is Phase 1 of a larger learning track combining **Kafka**, **Go**, and **Agentic AI**. The production implementation — an AI Agent Event Pipeline (Go microservices, Kafka event bus, LLM agents as consumers/producers) — lives in a separate repo: `ai-agent-event-pipeline` (link to be added).