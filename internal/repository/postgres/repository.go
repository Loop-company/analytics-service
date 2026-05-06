package postgres

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/Loop-company/analytics-service/internal/entity"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Repository struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Repository {
	return &Repository{pool: pool}
}

func (r *Repository) SaveEvent(ctx context.Context, event entity.Event) error {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var userID int64
	err = tx.QueryRow(ctx, `
		INSERT INTO analytics_users (external_user_id, first_seen_at, last_seen_at)
		VALUES ($1, $2, $2)
		ON CONFLICT (external_user_id) DO UPDATE
		SET last_seen_at = GREATEST(analytics_users.last_seen_at, EXCLUDED.last_seen_at)
		RETURNING id
	`, event.ExternalUserID, event.OccurredAt).Scan(&userID)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO analytics_events (id, user_id, event_type, source_service, payload, occurred_at, received_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (id) DO NOTHING
	`, event.ID, userID, event.EventType, event.SourceService, event.Payload, event.OccurredAt, event.ReceivedAt)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (r *Repository) SearchEvents(ctx context.Context, filter entity.SearchFilter) ([]entity.Event, error) {
	query := `
		SELECT e.id::text, u.external_user_id, e.event_type, e.source_service, e.payload::text, e.occurred_at, e.received_at
		FROM analytics_events e
		JOIN analytics_users u ON u.id = e.user_id
	`
	where, args := buildPeriodWhere(filter.From, filter.To, "e.occurred_at")
	nextArg := len(args) + 1
	if filter.ExternalUserID != "" {
		where = append(where, fmt.Sprintf("u.external_user_id = $%d", nextArg))
		args = append(args, filter.ExternalUserID)
		nextArg++
	}
	if filter.EventType != "" {
		where = append(where, fmt.Sprintf("e.event_type = $%d", nextArg))
		args = append(args, filter.EventType)
		nextArg++
	}
	if filter.SourceService != "" {
		where = append(where, fmt.Sprintf("e.source_service = $%d", nextArg))
		args = append(args, filter.SourceService)
		nextArg++
	}
	if len(where) > 0 {
		query += " WHERE " + strings.Join(where, " AND ")
	}
	query += fmt.Sprintf(" ORDER BY e.occurred_at DESC LIMIT $%d OFFSET $%d", nextArg, nextArg+1)
	args = append(args, filter.Limit, filter.Offset)

	rows, err := r.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	events := make([]entity.Event, 0)
	for rows.Next() {
		var event entity.Event
		if err := rows.Scan(
			&event.ID,
			&event.ExternalUserID,
			&event.EventType,
			&event.SourceService,
			&event.Payload,
			&event.OccurredAt,
			&event.ReceivedAt,
		); err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, rows.Err()
}

func (r *Repository) RegistrationsByDay(ctx context.Context, from, to *time.Time) ([]entity.RegistrationReportItem, error) {
	query := `
		SELECT date_trunc('day', e.occurred_at) AS day, count(*) AS count
		FROM analytics_events e
	`
	where, args := buildPeriodWhere(from, to, "e.occurred_at")
	where = append(where, "e.event_type IN ('UserRegistered', 'user.registered')")
	query += " WHERE " + strings.Join(where, " AND ")
	query += " GROUP BY day ORDER BY day"

	rows, err := r.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]entity.RegistrationReportItem, 0)
	for rows.Next() {
		var item entity.RegistrationReportItem
		if err := rows.Scan(&item.Day, &item.Count); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (r *Repository) LoginsByDay(ctx context.Context, from, to *time.Time) ([]entity.LoginReportItem, error) {
	query := `
		SELECT
			date_trunc('day', e.occurred_at) AS day,
			count(*) FILTER (WHERE e.event_type IN ('UserLoggedIn', 'user.logged_in')) AS success_count,
			count(*) FILTER (WHERE e.event_type IN ('UserLoginFailed', 'user.login_failed')) AS failed_count
		FROM analytics_events e
	`
	where, args := buildPeriodWhere(from, to, "e.occurred_at")
	where = append(where, "e.event_type IN ('UserLoggedIn', 'user.logged_in', 'UserLoginFailed', 'user.login_failed')")
	query += " WHERE " + strings.Join(where, " AND ")
	query += " GROUP BY day ORDER BY day"

	rows, err := r.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]entity.LoginReportItem, 0)
	for rows.Next() {
		var item entity.LoginReportItem
		if err := rows.Scan(&item.Day, &item.SuccessCount, &item.FailedCount); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (r *Repository) TopUsers(ctx context.Context, from, to *time.Time, limit int32) ([]entity.TopUserReportItem, error) {
	query := `
		SELECT u.external_user_id, count(*) AS events_count, max(e.occurred_at) AS last_activity_at
		FROM analytics_events e
		JOIN analytics_users u ON u.id = e.user_id
	`
	where, args := buildPeriodWhere(from, to, "e.occurred_at")
	if len(where) > 0 {
		query += " WHERE " + strings.Join(where, " AND ")
	}
	query += fmt.Sprintf(" GROUP BY u.external_user_id ORDER BY events_count DESC, last_activity_at DESC LIMIT $%d", len(args)+1)
	args = append(args, limit)

	rows, err := r.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]entity.TopUserReportItem, 0)
	for rows.Next() {
		var item entity.TopUserReportItem
		if err := rows.Scan(&item.ExternalUserID, &item.EventsCount, &item.LastActivityAt); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func buildPeriodWhere(from, to *time.Time, field string) ([]string, []any) {
	where := make([]string, 0, 2)
	args := make([]any, 0, 2)
	if from != nil {
		where = append(where, fmt.Sprintf("%s >= $%d", field, len(args)+1))
		args = append(args, *from)
	}
	if to != nil {
		where = append(where, fmt.Sprintf("%s <= $%d", field, len(args)+1))
		args = append(args, *to)
	}
	return where, args
}
