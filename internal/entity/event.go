package entity

import (
	"errors"
	"time"
)

var ErrInvalidEvent = errors.New("invalid analytics event")

type Event struct {
	ID             string
	ExternalUserID string
	EventType      string
	SourceService  string
	Payload        []byte
	OccurredAt     time.Time
	ReceivedAt     time.Time
}

type SearchFilter struct {
	ExternalUserID string
	EventType      string
	SourceService  string
	From           *time.Time
	To             *time.Time
	Limit          int32
	Offset         int32
}

type RegistrationReportItem struct {
	Day   time.Time
	Count int64
}

type LoginReportItem struct {
	Day          time.Time
	SuccessCount int64
	FailedCount  int64
}

type TopUserReportItem struct {
	ExternalUserID string
	EventsCount    int64
	LastActivityAt time.Time
}
