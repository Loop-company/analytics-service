# Analytics Service

Analytics Service принимает события из Kafka, сохраняет их в PostgreSQL и отдает отчеты HTTP Gateway по gRPC. Прямого HTTP API у сервиса нет.

## Место в архитектуре

```text
Auth Service ----\
                  +-> Kafka: auth.events,user.events -> Analytics Service -> PostgreSQL
User Service ----/                                                |
                                                                  v
                                                        HTTP Gateway (gRPC)
```

Analytics Service:

- читает Kafka topics `auth.events` и `user.events`;
- валидирует и нормализует события;
- сохраняет события в PostgreSQL;
- запускает миграции при старте;
- отдает поиск событий и отчеты через gRPC для HTTP Gateway.

## Kafka input

Сервис ожидает общую envelope-структуру:

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

Источники:

- `auth.events`: события Auth Service, например `user.logged_in`;
- `user.events`: события Auth/User Service, например `user.registered`, `user.profile_updated`, `user.settings_updated`.

Kafka consumer group по умолчанию: `analytics-service`.

## gRPC API

Контракт описан в `proto/analytics.proto`.

- `SearchEvents(user_id, event_type, source_service, from, to, limit, offset)` - поиск сохраненных событий.
- `GetRegistrationsReport(from, to)` - отчет по регистрациям за период.
- `GetLoginReport(from, to)` - отчет по логинам за период.
- `GetTopUsersReport(from, to, limit)` - пользователи с наибольшей активностью.

HTTP Gateway вызывает эти методы и возвращает результат клиенту как HTTP JSON.

## Переменные окружения

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
KAFKA_MIN_BYTES=1
KAFKA_MAX_BYTES=10000000

MIGRATIONS_DIR=migrations
SHUTDOWN_TIMEOUT_SECONDS=15
```

## Запуск

Для запуска Analytics Service через Docker Compose из корня репозитория:

```bash
docker compose up --build
```

Этот compose поднимает Analytics Service и PostgreSQL. Для полноценной обработки событий Kafka должна быть доступна по адресу из `KAFKA_BROKERS`.

Для запуска всего backend-стека используется Docker Compose в репозитории HTTP Gateway:

```bash
docker compose up --build
```
