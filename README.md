# Analytics Service

gRPC service for storing domain events and returning reports to HTTP Gateway.

## Architecture Role

- Consumes Kafka topics `auth.events` and `user.events`.
- Stores normalized events in PostgreSQL.
- Provides gRPC methods for event search and reports.
- Does not receive HTTP traffic directly.

## Event Envelope

```json
{
  "event_id": "uuid",
  "user_id": "external-user-id",
  "event_type": "user.registered",
  "source_service": "auth-service",
  "payload": {
    "email": "user@example.com"
  },
  "occurred_at": "2026-05-07T12:00:00Z"
}
```

## gRPC API

- `SearchEvents`
- `GetRegistrationsReport`
- `GetLoginReport`
- `GetTopUsersReport`

## Configuration

```env
GRPC_ADDR=:50053
POSTGRES_HOST=db-analytics
POSTGRES_INTERNAL_PORT=5432
POSTGRES_USER=analytics
POSTGRES_PASSWORD=analytics_password
POSTGRES_DB=analytics
POSTGRES_SSL_MODE=disable
KAFKA_BROKERS=kafka:9092
KAFKA_TOPICS=auth.events,user.events
KAFKA_GROUP_ID=analytics-service
MIGRATIONS_DIR=migrations
```
