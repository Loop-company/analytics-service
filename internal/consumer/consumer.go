package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/Loop-company/analytics-service/internal/entity"
	"github.com/segmentio/kafka-go"
)

type IngestUsecase interface {
	Ingest(ctx context.Context, event entity.Event) error
}

type Consumer struct {
	reader  *kafka.Reader
	usecase IngestUsecase
	logger  *slog.Logger
}

type Config struct {
	Brokers  []string
	Topics   []string
	GroupID  string
	MinBytes int
	MaxBytes int
}

func NewConsumer(cfg Config, usecase IngestUsecase, logger *slog.Logger) *Consumer {
	return &Consumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:     cfg.Brokers,
			GroupID:     cfg.GroupID,
			GroupTopics: cfg.Topics,
			MinBytes:    cfg.MinBytes,
			MaxBytes:    cfg.MaxBytes,
		}),
		usecase: usecase,
		logger:  logger,
	}
}

func (c *Consumer) Run(ctx context.Context) error {
	for {
		message, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}

		if err := c.handleMessage(ctx, message); err != nil {
			c.logger.Error("failed to process kafka message",
				slog.String("topic", message.Topic),
				slog.Int("partition", message.Partition),
				slog.Int64("offset", message.Offset),
				slog.Any("error", err),
			)
			if errors.Is(err, entity.ErrInvalidEvent) {
				if commitErr := c.reader.CommitMessages(ctx, message); commitErr != nil {
					return commitErr
				}
				continue
			}
			return err
		}

		if err := c.reader.CommitMessages(ctx, message); err != nil {
			return err
		}
	}
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}

func (c *Consumer) handleMessage(ctx context.Context, message kafka.Message) error {
	var incoming eventMessage
	if err := json.Unmarshal(message.Value, &incoming); err != nil {
		return fmt.Errorf("%w: %v", entity.ErrInvalidEvent, err)
	}

	occurredAt := incoming.OccurredAt.Time
	if occurredAt.IsZero() {
		occurredAt = message.Time
	}
	if occurredAt.IsZero() {
		occurredAt = time.Now().UTC()
	}

	payload := incoming.Payload
	if len(payload) == 0 {
		payload = json.RawMessage("{}")
	}

	event := entity.Event{
		ID:             incoming.EventID,
		ExternalUserID: incoming.UserID,
		EventType:      incoming.EventType,
		SourceService:  incoming.SourceService,
		Payload:        payload,
		OccurredAt:     occurredAt.UTC(),
		ReceivedAt:     time.Now().UTC(),
	}

	return c.usecase.Ingest(ctx, event)
}

type eventMessage struct {
	EventID       string          `json:"event_id"`
	UserID        string          `json:"user_id"`
	EventType     string          `json:"event_type"`
	SourceService string          `json:"source_service"`
	Payload       json.RawMessage `json:"payload"`
	OccurredAt    flexibleTime    `json:"occurred_at"`
}

type flexibleTime struct {
	time.Time
}

func (t *flexibleTime) UnmarshalJSON(value []byte) error {
	if string(value) == "null" {
		return nil
	}
	var raw string
	if err := json.Unmarshal(value, &raw); err != nil {
		return err
	}
	if raw == "" {
		return nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return fmt.Errorf("%w: %v", entity.ErrInvalidEvent, err)
	}
	t.Time = parsed
	return nil
}
