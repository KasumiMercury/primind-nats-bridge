package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-nats/v2/pkg/nats"
	"github.com/ThreeDotsLabs/watermill/message"
	nc "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	TopicRemindCancelled = "remind.cancelled"
	StreamName           = "REMIND_EVENTS"
	ConsumerGroup        = "nats-http-bridge"
)

type RemindCancelledEvent struct {
	TaskID       string    `json:"task_id"`
	UserID       string    `json:"user_id"`
	DeletedCount int64     `json:"deleted_count"`
	CancelledAt  time.Time `json:"cancelled_at"`
}

func main() {
	os.Exit(run())
}

func run() int {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		slog.Error("NATS_URL environment variable is required")
		return 1
	}

	throttlingURL := os.Getenv("THROTTLING_URL")
	if throttlingURL == "" {
		slog.Error("THROTTLING_URL environment variable is required")
		return 1
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := waitForStream(ctx, natsURL, StreamName, 30, 2*time.Second); err != nil {
		slog.Error("failed to wait for stream", slog.String("error", err.Error()))
		return 1
	}

	wmLogger := watermill.NewSlogLogger(slog.Default())

	subscriber, err := nats.NewSubscriber(
		nats.SubscriberConfig{
			URL:         natsURL,
			NatsOptions: []nc.Option{nc.Timeout(10 * time.Second)},
			JetStream: nats.JetStreamConfig{
				Disabled:      false,
				AutoProvision: false,
				DurablePrefix: ConsumerGroup,
			},
			Unmarshaler: &nats.NATSMarshaler{},
		},
		wmLogger,
	)
	if err != nil {
		slog.Error("failed to create NATS subscriber", slog.String("error", err.Error()))
		return 1
	}
	defer func() {
		if err := subscriber.Close(); err != nil {
			slog.Warn("failed to close subscriber", slog.String("error", err.Error()))
		}
	}()

	slog.Info("NATS subscriber initialized",
		slog.String("url", natsURL),
		slog.String("topic", TopicRemindCancelled),
	)

	messages, err := subscriber.Subscribe(ctx, TopicRemindCancelled)
	if err != nil {
		slog.Error("failed to subscribe", slog.String("error", err.Error()))
		return 1
	}

	httpClient := &http.Client{
		Timeout: 30 * time.Second,
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	slog.Info("starting message processing",
		slog.String("throttling_url", throttlingURL),
	)

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-messages:
				if !ok {
					return
				}
				if err := processMessage(ctx, httpClient, throttlingURL, msg); err != nil {
					slog.Error("failed to process message",
						slog.String("message_id", msg.UUID),
						slog.String("error", err.Error()),
					)
					msg.Nack()
				} else {
					msg.Ack()
				}
			}
		}
	}()

	sig := <-quit
	slog.Info("shutdown signal received", slog.String("signal", sig.String()))
	cancel()

	slog.Info("bridge exited properly")
	return 0
}

func waitForStream(ctx context.Context, natsURL string, streamName string, maxRetries int, retryInterval time.Duration) error {
	conn, err := nc.Connect(natsURL, nc.Timeout(10*time.Second))
	if err != nil {
		return fmt.Errorf("failed to connect to NATS: %w", err)
	}
	defer conn.Close()

	js, err := jetstream.New(conn)
	if err != nil {
		return fmt.Errorf("failed to create JetStream context: %w", err)
	}

	for i := 0; i < maxRetries; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		_, err := js.Stream(ctx, streamName)
		if err == nil {
			slog.Info("stream is available", slog.String("stream", streamName))
			return nil
		}

		slog.Info("waiting for stream",
			slog.String("stream", streamName),
			slog.Int("attempt", i+1),
			slog.Int("max_retries", maxRetries),
		)
		time.Sleep(retryInterval)
	}

	return fmt.Errorf("stream %s not available after %d retries", streamName, maxRetries)
}

func processMessage(ctx context.Context, client *http.Client, throttlingURL string, msg *message.Message) error {
	var event RemindCancelledEvent
	if err := json.Unmarshal(msg.Payload, &event); err != nil {
		return fmt.Errorf("failed to unmarshal event: %w", err)
	}

	slog.Debug("processing message",
		slog.String("message_id", msg.UUID),
		slog.String("task_id", event.TaskID),
	)

	endpoint := throttlingURL + "/api/v1/remind/cancel"
	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("throttling returned error status: %d", resp.StatusCode)
	}

	slog.Info("message forwarded successfully",
		slog.String("message_id", msg.UUID),
		slog.String("task_id", event.TaskID),
		slog.Int("status_code", resp.StatusCode),
	)

	return nil
}
