# analytics-service

Микросервис аналитики для Loop. Сервис читает доменные события из Kafka, сохраняет их в PostgreSQL и отдает поиск/отчеты по gRPC для HTTP Gateway.

## Архитектура

- входящие Kafka topics: `auth.events`, `user.events`
- gRPC API: `analytics.v1.AnalyticsService`
- база данных: PostgreSQL
- таблицы:
  - `analytics_users`
  - `analytics_events` с внешним ключом на `analytics_users`

Сервис подключается к уже существующей Docker-сети `backend-network`. Kafka, Kafka UI и прочая инфраструктура запускаются в другом репозитории.

## Формат события Kafka

```json
{
  "event_id": "5d21d84b-8f10-4a8e-8470-56c5f2a51fbb",
  "user_id": "external-user-id",
  "event_type": "UserRegistered",
  "source_service": "auth-service",
  "payload": {
    "email": "user@example.com"
  },
  "occurred_at": "2026-05-07T12:00:00Z"
}
```

Типы событий, которые используются в отчетах:

- регистрации: `UserRegistered`, `user.registered`
- успешные входы: `UserLoggedIn`, `user.logged_in`
- неуспешные входы: `UserLoginFailed`, `user.login_failed`

## gRPC методы

- `SearchEvents`
- `GetRegistrationsReport`
- `GetLoginReport`
- `GetTopUsersReport`

## Запуск

Перед запуском должна существовать сеть `backend-network`, которую создает инфраструктурный репозиторий.

```bash
docker compose up --build
```

Настройки лежат в `.env`. Если Kafka в Docker доступна под другим именем, измени `KAFKA_BROKERS`.
