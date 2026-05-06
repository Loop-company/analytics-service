package service

import (
	"context"
	"encoding/json"
	"time"

	"github.com/Loop-company/analytics-service/internal/entity"
	"github.com/google/uuid"
)

type Analytics struct {
	repo entity.EventRepository
	now  func() time.Time
}

func NewAnalytics(repo entity.EventRepository) *Analytics {
	return &Analytics{repo: repo, now: time.Now}
}

func (a *Analytics) Ingest(ctx context.Context, event entity.Event) error {
	if event.ID == "" {
		event.ID = uuid.NewString()
	}
	if event.ExternalUserID == "" || event.EventType == "" || event.SourceService == "" {
		return entity.ErrInvalidEvent
	}
	if event.OccurredAt.IsZero() {
		event.OccurredAt = a.now().UTC()
	}
	if event.ReceivedAt.IsZero() {
		event.ReceivedAt = a.now().UTC()
	}
	if len(event.Payload) == 0 {
		event.Payload = []byte("{}")
	}
	if !json.Valid(event.Payload) {
		return entity.ErrInvalidEvent
	}
	return a.repo.SaveEvent(ctx, event)
}

func (a *Analytics) SearchEvents(ctx context.Context, filter entity.SearchFilter) ([]entity.Event, error) {
	if filter.Limit <= 0 || filter.Limit > 100 {
		filter.Limit = 50
	}
	if filter.Offset < 0 {
		filter.Offset = 0
	}
	return a.repo.SearchEvents(ctx, filter)
}

func (a *Analytics) RegistrationsByDay(ctx context.Context, from, to *time.Time) ([]entity.RegistrationReportItem, error) {
	return a.repo.RegistrationsByDay(ctx, from, to)
}

func (a *Analytics) LoginsByDay(ctx context.Context, from, to *time.Time) ([]entity.LoginReportItem, error) {
	return a.repo.LoginsByDay(ctx, from, to)
}

func (a *Analytics) TopUsers(ctx context.Context, from, to *time.Time, limit int32) ([]entity.TopUserReportItem, error) {
	if limit <= 0 || limit > 100 {
		limit = 10
	}
	return a.repo.TopUsers(ctx, from, to, limit)
}
