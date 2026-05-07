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

## gRPC API

Контракт описан в `proto/analytics.proto`.

- `SearchEvents(user_id, event_type, source_service, from, to, limit, offset)` - поиск сохраненных событий.
- `GetRegistrationsReport(from, to)` - отчет по регистрациям за период.
- `GetLoginReport(from, to)` - отчет по логинам за период.
- `GetTopUsersReport(from, to, limit)` - пользователи с наибольшей активностью.

## Запуск

Docker Compose для локального запуска вынесен в репозиторий `loop_infra`.

Из корня `loop_infra`:

```bash
docker compose up --build
```

В этом репозитории остается код сервиса, `Dockerfile` и пример переменных окружения `.env.example`.
