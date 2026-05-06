package entity

import (
	"context"
	"time"
)

type EventRepository interface {
	SaveEvent(ctx context.Context, event Event) error
	SearchEvents(ctx context.Context, filter SearchFilter) ([]Event, error)
	RegistrationsByDay(ctx context.Context, from, to *time.Time) ([]RegistrationReportItem, error)
	LoginsByDay(ctx context.Context, from, to *time.Time) ([]LoginReportItem, error)
	TopUsers(ctx context.Context, from, to *time.Time, limit int32) ([]TopUserReportItem, error)
}
