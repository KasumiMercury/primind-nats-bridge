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
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-nats/v2/pkg/nats"
	"github.com/ThreeDotsLabs/watermill/message"
	nc "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// FileConfig represents the JSON configuration file structure
type FileConfig struct {
	NATS   NATSConfig `json:"nats"`
	HTTP   HTTPConfig `json:"http"`
	Routes []Route    `json:"routes"`
}

// NATSConfig holds NATS connection settings
type NATSConfig struct {
	URL           string `json:"url"`
	Stream        string `json:"stream"`
	ConsumerGroup string `json:"consumer_group"`
}

// HTTPConfig holds HTTP client settings
type HTTPConfig struct {
	Timeout string `json:"timeout"`
}

// Route represents a NATS subject to HTTP endpoint mapping
type Route struct {
	Subject     string `json:"subject"`
	Method      string `json:"method"`
	Endpoint    string `json:"endpoint"`
	ContentType string `json:"content_type"`
}

// Config holds the runtime configuration
type Config struct {
	NatsURL       string
	Stream        string
	ConsumerGroup string
	HTTPTimeout   time.Duration
	Routes        []Route
}

func main() {
	os.Exit(run())
}

func run() int {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	config, err := loadConfig()
	if err != nil {
		slog.Error("failed to load configuration", slog.String("error", err.Error()))
		return 1
	}

	if len(config.Routes) == 0 {
		slog.Error("no routes configured")
		return 1
	}

	slog.Info("configuration loaded",
		slog.Int("route_count", len(config.Routes)),
		slog.String("stream", config.Stream),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := waitForStream(ctx, config.NatsURL, config.Stream, 30, 2*time.Second); err != nil {
		slog.Error("failed to wait for stream", slog.String("error", err.Error()))
		return 1
	}

	wmLogger := watermill.NewSlogLogger(slog.Default())

	subscriber, err := nats.NewSubscriber(
		nats.SubscriberConfig{
			URL:         config.NatsURL,
			NatsOptions: []nc.Option{nc.Timeout(10 * time.Second)},
			JetStream: nats.JetStreamConfig{
				Disabled:      false,
				AutoProvision: false,
				DurablePrefix: config.ConsumerGroup,
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

	httpClient := &http.Client{
		Timeout: config.HTTPTimeout,
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	var wg sync.WaitGroup

	for _, route := range config.Routes {
		slog.Info("subscribing to route",
			slog.String("subject", route.Subject),
			slog.String("endpoint", route.Endpoint),
		)

		messages, err := subscriber.Subscribe(ctx, route.Subject)
		if err != nil {
			slog.Error("failed to subscribe",
				slog.String("subject", route.Subject),
				slog.String("error", err.Error()),
			)
			return 1
		}

		wg.Go(func() {
			processMessages(ctx, httpClient, &route, messages)
		})
	}

	slog.Info("bridge started", slog.Int("routes", len(config.Routes)))

	sig := <-quit
	slog.Info("shutdown signal received", slog.String("signal", sig.String()))
	cancel()

	wg.Wait()
	slog.Info("bridge exited properly")
	return 0
}

func loadConfig() (*Config, error) {
	configPath := os.Getenv("CONFIG_PATH")
	if configPath == "" {
		configPath = "config.json"
	}

	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %s: %w", configPath, err)
	}

	var fileConfig FileConfig
	if err := json.Unmarshal(data, &fileConfig); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	natsURL := fileConfig.NATS.URL
	if envURL := os.Getenv("NATS_URL"); envURL != "" {
		natsURL = envURL
	}
	if natsURL == "" {
		return nil, fmt.Errorf("nats.url is required in config file or NATS_URL environment variable")
	}

	httpTimeout := 30 * time.Second
	if fileConfig.HTTP.Timeout != "" {
		parsed, err := time.ParseDuration(fileConfig.HTTP.Timeout)
		if err != nil {
			return nil, fmt.Errorf("invalid http.timeout: %w", err)
		}
		httpTimeout = parsed
	}

	stream := fileConfig.NATS.Stream
	if stream == "" {
		stream = "REMIND_EVENTS"
	}

	consumerGroup := fileConfig.NATS.ConsumerGroup
	if consumerGroup == "" {
		consumerGroup = "nats-http-bridge"
	}

	routes := make([]Route, 0, len(fileConfig.Routes))
	for _, r := range fileConfig.Routes {
		if r.Subject == "" || r.Endpoint == "" {
			return nil, fmt.Errorf("route requires both subject and endpoint")
		}

		method := strings.ToUpper(r.Method)
		if method == "" {
			method = "POST"
		}

		contentType := r.ContentType
		if contentType == "" {
			contentType = "application/json"
		}

		routes = append(routes, Route{
			Subject:     r.Subject,
			Method:      method,
			Endpoint:    r.Endpoint,
			ContentType: contentType,
		})
	}

	return &Config{
		NatsURL:       natsURL,
		Stream:        stream,
		ConsumerGroup: consumerGroup,
		HTTPTimeout:   httpTimeout,
		Routes:        routes,
	}, nil
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

func processMessages(ctx context.Context, client *http.Client, route *Route, messages <-chan *message.Message) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-messages:
			if !ok {
				return
			}
			if err := processMessage(ctx, client, route, msg); err != nil {
				slog.Error("failed to process message",
					slog.String("message_id", msg.UUID),
					slog.String("subject", route.Subject),
					slog.String("error", err.Error()),
				)
				msg.Nack()
			} else {
				msg.Ack()
			}
		}
	}
}

// processMessage forwards the raw payload without deserialization
func processMessage(ctx context.Context, client *http.Client, route *Route, msg *message.Message) error {
	slog.Debug("processing message",
		slog.String("message_id", msg.UUID),
		slog.String("subject", route.Subject),
		slog.Int("payload_size", len(msg.Payload)),
	)

	req, err := http.NewRequestWithContext(ctx, route.Method, route.Endpoint, bytes.NewReader(msg.Payload))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", route.ContentType)

	for key, value := range msg.Metadata {
		req.Header.Set("X-NATS-"+key, value)
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("target returned error status: %d", resp.StatusCode)
	}

	slog.Info("message forwarded successfully",
		slog.String("message_id", msg.UUID),
		slog.String("subject", route.Subject),
		slog.String("endpoint", route.Endpoint),
		slog.Int("status_code", resp.StatusCode),
	)

	return nil
}
