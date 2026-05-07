package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Loop-company/analytics-service/internal/consumer"
	transportgrpc "github.com/Loop-company/analytics-service/internal/grpc"
	"github.com/Loop-company/analytics-service/internal/repository/postgres"
	"github.com/Loop-company/analytics-service/internal/service"
	analyticsv1 "github.com/Loop-company/analytics-service/proto"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	if err := run(logger); err != nil {
		logger.Error("service stopped with error", slog.Any("error", err))
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := connectPostgres(ctx, databaseURL())
	if err != nil {
		return err
	}
	defer pool.Close()

	if err := postgres.RunMigrations(ctx, pool, env("MIGRATIONS_DIR", "migrations")); err != nil {
		return err
	}

	repo := postgres.New(pool)
	analytics := service.NewAnalytics(repo)

	kafkaBrokers := splitCSV(env("KAFKA_BROKERS", "localhost:9092"))
	kafkaTopics := splitCSV(env("KAFKA_TOPICS", "auth.events,user.events"))
	kafkaGroupID := env("KAFKA_GROUP_ID", "analytics-service")
	kafkaConsumer := consumer.NewConsumer(consumer.Config{
		Brokers:  kafkaBrokers,
		Topics:   kafkaTopics,
		GroupID:  kafkaGroupID,
		MinBytes: envInt("KAFKA_MIN_BYTES", 1),
		MaxBytes: envInt("KAFKA_MAX_BYTES", 10e6),
	}, analytics, logger)
	defer kafkaConsumer.Close()

	grpcAddr := env("GRPC_ADDR", ":50053")
	listener, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		return err
	}

	grpcServer := grpc.NewServer()
	analyticsv1.RegisterAnalyticsServiceServer(grpcServer, transportgrpc.NewServer(analytics))
	healthServer := health.NewServer()
	healthpb.RegisterHealthServer(grpcServer, healthServer)
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	reflection.Register(grpcServer)

	errCh := make(chan error, 2)
	go func() {
		logger.Info("kafka consumer started",
			slog.Any("brokers", kafkaBrokers),
			slog.Any("topics", kafkaTopics),
			slog.String("group_id", kafkaGroupID),
		)
		errCh <- kafkaConsumer.Run(ctx)
	}()
	go func() {
		logger.Info("grpc server started", slog.String("addr", grpcAddr))
		errCh <- grpcServer.Serve(listener)
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Duration(envInt("SHUTDOWN_TIMEOUT_SECONDS", 15))*time.Second)
		defer cancel()
		done := make(chan struct{})
		go func() {
			healthServer.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
			grpcServer.GracefulStop()
			close(done)
		}()
		select {
		case <-done:
		case <-shutdownCtx.Done():
			grpcServer.Stop()
		}
		return nil
	case err := <-errCh:
		if errors.Is(err, grpc.ErrServerStopped) {
			return nil
		}
		return err
	}
}

func connectPostgres(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	var lastErr error
	for attempt := 0; attempt < 20; attempt++ {
		pool, err := pgxpool.New(ctx, databaseURL)
		if err == nil {
			if pingErr := pool.Ping(ctx); pingErr == nil {
				return pool, nil
			} else {
				lastErr = pingErr
			}
			pool.Close()
		} else {
			lastErr = err
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}
	return nil, lastErr
}

func databaseURL() string {
	host := env("POSTGRES_HOST")
	port := env("POSTGRES_INTERNAL_PORT")
	user := env("POSTGRES_USER")
	password := env("POSTGRES_PASSWORD")
	db := env("POSTGRES_DB")
	sslMode := env("POSTGRES_SSL_MODE")

	return "postgres://" + user + ":" + password + "@" + host + ":" + port + "/" + db + "?sslmode=" + sslMode
}

func env(key string, fallback ...string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	if len(fallback) > 0 {
		return fallback[0]
	}
	return ""
}

func envInt(key string, fallback int) int {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func splitCSV(value string) []string {
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			result = append(result, part)
		}
	}
	return result
}
