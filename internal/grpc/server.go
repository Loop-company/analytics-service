package grpc

import (
	"context"
	"time"

	"github.com/Loop-company/analytics-service/internal/entity"
	analyticsv1 "github.com/Loop-company/analytics-service/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type AnalyticsUsecase interface {
	SearchEvents(ctx context.Context, filter entity.SearchFilter) ([]entity.Event, error)
	RegistrationsByDay(ctx context.Context, from, to *time.Time) ([]entity.RegistrationReportItem, error)
	LoginsByDay(ctx context.Context, from, to *time.Time) ([]entity.LoginReportItem, error)
	TopUsers(ctx context.Context, from, to *time.Time, limit int32) ([]entity.TopUserReportItem, error)
}

type Server struct {
	analyticsv1.UnimplementedAnalyticsServiceServer
	usecase AnalyticsUsecase
}

func NewServer(usecase AnalyticsUsecase) *Server {
	return &Server{usecase: usecase}
}

func (s *Server) SearchEvents(ctx context.Context, req *analyticsv1.SearchEventsRequest) (*analyticsv1.SearchEventsResponse, error) {
	from, to, err := parsePeriod(req.GetFrom(), req.GetTo())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	events, err := s.usecase.SearchEvents(ctx, entity.SearchFilter{
		ExternalUserID: req.GetUserId(),
		EventType:      req.GetEventType(),
		SourceService:  req.GetSourceService(),
		From:           from,
		To:             to,
		Limit:          req.GetLimit(),
		Offset:         req.GetOffset(),
	})
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	response := &analyticsv1.SearchEventsResponse{Events: make([]*analyticsv1.AnalyticsEvent, 0, len(events))}
	for _, event := range events {
		response.Events = append(response.Events, &analyticsv1.AnalyticsEvent{
			Id:             event.ID,
			ExternalUserId: event.ExternalUserID,
			EventType:      event.EventType,
			SourceService:  event.SourceService,
			PayloadJson:    string(event.Payload),
			OccurredAt:     formatTime(event.OccurredAt),
			ReceivedAt:     formatTime(event.ReceivedAt),
		})
	}
	return response, nil
}

func (s *Server) GetRegistrationsReport(ctx context.Context, req *analyticsv1.ReportPeriodRequest) (*analyticsv1.RegistrationsReportResponse, error) {
	from, to, err := parsePeriod(req.GetFrom(), req.GetTo())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	items, err := s.usecase.RegistrationsByDay(ctx, from, to)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	response := &analyticsv1.RegistrationsReportResponse{Items: make([]*analyticsv1.RegistrationsReportItem, 0, len(items))}
	for _, item := range items {
		response.Items = append(response.Items, &analyticsv1.RegistrationsReportItem{
			Day:   item.Day.Format(time.DateOnly),
			Count: item.Count,
		})
	}
	return response, nil
}

func (s *Server) GetLoginReport(ctx context.Context, req *analyticsv1.ReportPeriodRequest) (*analyticsv1.LoginReportResponse, error) {
	from, to, err := parsePeriod(req.GetFrom(), req.GetTo())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	items, err := s.usecase.LoginsByDay(ctx, from, to)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	response := &analyticsv1.LoginReportResponse{Items: make([]*analyticsv1.LoginReportItem, 0, len(items))}
	for _, item := range items {
		response.Items = append(response.Items, &analyticsv1.LoginReportItem{
			Day:          item.Day.Format(time.DateOnly),
			SuccessCount: item.SuccessCount,
			FailedCount:  item.FailedCount,
		})
	}
	return response, nil
}

func (s *Server) GetTopUsersReport(ctx context.Context, req *analyticsv1.TopUsersReportRequest) (*analyticsv1.TopUsersReportResponse, error) {
	from, to, err := parsePeriod(req.GetFrom(), req.GetTo())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	items, err := s.usecase.TopUsers(ctx, from, to, req.GetLimit())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	response := &analyticsv1.TopUsersReportResponse{Items: make([]*analyticsv1.TopUserReportItem, 0, len(items))}
	for _, item := range items {
		response.Items = append(response.Items, &analyticsv1.TopUserReportItem{
			ExternalUserId: item.ExternalUserID,
			EventsCount:    item.EventsCount,
			LastActivityAt: formatTime(item.LastActivityAt),
		})
	}
	return response, nil
}

func parsePeriod(fromValue, toValue string) (*time.Time, *time.Time, error) {
	from, err := parseOptionalTime(fromValue)
	if err != nil {
		return nil, nil, err
	}
	to, err := parseOptionalTime(toValue)
	if err != nil {
		return nil, nil, err
	}
	return from, to, nil
}

func parseOptionalTime(value string) (*time.Time, error) {
	if value == "" {
		return nil, nil
	}
	if parsed, err := time.Parse(time.RFC3339Nano, value); err == nil {
		parsed = parsed.UTC()
		return &parsed, nil
	}
	parsed, err := time.Parse(time.DateOnly, value)
	if err != nil {
		return nil, err
	}
	parsed = parsed.UTC()
	return &parsed, nil
}

func formatTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.UTC().Format(time.RFC3339Nano)
}
