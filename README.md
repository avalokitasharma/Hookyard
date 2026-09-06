# Hookyard

A webhook infrastructure you can self-host.

Hookyard takes care of receiving events from your application and delivering them to your customers' HTTP endpoints. It handles retries, failed deliveries, replay, signatures, and delivery history so that your application doesn't have to.

Built in Go with PostgreSQL and Kafka.

## What it solves

Sending a webhook is easy.

Making it reliable is not.

What happens when the customer's endpoint is down? What if your process crashes after saving an event but before publishing it? What if the same event gets processed twice? How do you retry failed deliveries without hammering an unhealthy endpoint? And how do you replay an event from last week?

Hookyard is an attempt to solve those problems in a small, self-hostable system.

## What it supports

* Event ingestion with idempotency
* Webhook endpoints and subscriptions
* At-least-once delivery
* Configurable retries with backoff and jitter
* Dead letter queue (DLQ)
* Delivery replay
* Delivery and attempt history
* HMAC webhook signatures
* Horizontally scalable delivery workers
* CLI for managing Hookyard

## Architecture

Hookyard is split into a few small services:

* **API Gateway** — public API, authentication and request handling
* **Auth Service** — tenants and API keys
* **Endpoint Service** — endpoints, subscriptions and retry configuration
* **Event Service** — event storage, idempotency and event publishing
* **Delivery Service** — delivery creation, retries and workers

Kafka is used for communication between services. Each service owns its data and talks to other services through APIs or events rather than directly accessing another service's database.

PostgreSQL is used for durable state.

## Delivery model

Hookyard uses **at-least-once delivery**.

A webhook may be delivered more than once, so consumers should make their webhook handlers idempotent.

Failed requests are retried according to the endpoint's retry policy. Once all retries are exhausted, the delivery moves to the DLQ and can be replayed later.

## Reliable event ingestion

When an event is accepted, the Event Service stores the event and an outbox record in the same database transaction.

This means we don't end up in a situation where the API says "accepted" but the event was never persisted.

The outbox is then published to Kafka asynchronously.

The publisher can publish the same message more than once in a crash scenario, so consumers are designed to handle duplicates as well.

## Example

Publish an event:

```bash
hookyard event publish \
  --type order.created \
  --idempotency-key order-123 \
  --data '{"order_id":"123"}'
```

Hookyard finds the endpoints subscribed to `order.created` and creates deliveries for them.

You can then inspect the delivery:

```bash
hookyard delivery get <delivery-id>
```

Or replay it if necessary:

```bash
hookyard delivery replay <delivery-id>
```

## Running locally

You'll need:

* Go
* Docker
* Docker Compose

Start the dependencies:

```bash
docker compose up -d
```

Then run the services:

```bash
go run ./cmd/...
```

The repository contains separate services and each service can be developed and run independently.

## Tech stack

* Go
* PostgreSQL
* Kafka
* Docker

## Project status

Hookyard is being built incrementally.

The initial focus is the core path:

**ingest → persist → publish → deliver → retry → DLQ → replay**

More operational features will be added as the core system matures.

## License

MIT
