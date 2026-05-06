CREATE TABLE IF NOT EXISTS analytics_users (
    id BIGSERIAL PRIMARY KEY,
    external_user_id TEXT NOT NULL UNIQUE,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS analytics_events (
    id UUID PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES analytics_users(id) ON DELETE CASCADE,
    event_type TEXT NOT NULL,
    source_service TEXT NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    occurred_at TIMESTAMPTZ NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_analytics_events_user_id ON analytics_events(user_id);
CREATE INDEX IF NOT EXISTS idx_analytics_events_type ON analytics_events(event_type);
CREATE INDEX IF NOT EXISTS idx_analytics_events_source ON analytics_events(source_service);
CREATE INDEX IF NOT EXISTS idx_analytics_events_occurred_at ON analytics_events(occurred_at);
