package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/KasumiMercury/primind-nats-bridge/internal/observability/logging"
	"github.com/KasumiMercury/primind-nats-bridge/internal/observability/tracing"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-nats/v2/pkg/nats"
	"github.com/ThreeDotsLabs/watermill/message"
	nc "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

func init() {
	// Set up W3C Trace Context propagator for trace context propagation
	tracing.SetupPropagator()
}

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

// Version is set via ldflags at build time.
var Version = "dev"

func main() {
	if err := run(); err != nil {
		os.Exit(1)
	}
}

func run() error {
	slog.SetDefault(initLogger())

	config, err := loadConfig()
	if err != nil {
		slog.Error("failed to load configuration", slog.String("error", err.Error()))

		return err
	}

	if len(config.Routes) == 0 {
		slog.Error("no routes configured")

		return errors.New("no routes configured")
	}

	slog.Info("configuration loaded",
		slog.Int("route_count", len(config.Routes)),
		slog.String("stream", config.Stream),
	)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	tp, err := initTracing(ctx)
	if err != nil {
		slog.Error("failed to initialize tracing", slog.String("error", err.Error()))

		return err
	}

	defer func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()

		if err := tp.Shutdown(shutdownCtx); err != nil {
			slog.Warn("tracing shutdown error", slog.String("error", err.Error()))
		}
	}()

	if err := waitForStream(ctx, config.NatsURL, config.Stream, 30, 2*time.Second); err != nil {
		slog.Error("failed to wait for stream", slog.String("error", err.Error()))

		return err
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

		return err
	}

	defer func() {
		if err := subscriber.Close(); err != nil {
			slog.Warn("failed to close subscriber", slog.String("error", err.Error()))
		}
	}()

	httpClient := &http.Client{
		Timeout: config.HTTPTimeout,
	}

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

			return err
		}

		wg.Go(func() {
			processMessages(ctx, httpClient, &route, messages)
		})
	}

	slog.Info("bridge started", slog.Int("routes", len(config.Routes)))

	// Start health check HTTP server
	healthServer := startHealthServer(config.NatsURL, Version)
	defer func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		if err := healthServer.Shutdown(shutdownCtx); err != nil {
			slog.Warn("health server shutdown error", slog.String("error", err.Error()))
		}
	}()

	<-ctx.Done()
	slog.Info("shutdown signal received",
		slog.String("event", "bridge.shutdown.start"),
	)

	wg.Wait()
	slog.Info("bridge exited properly",
		slog.String("event", "bridge.stop"),
	)

	return nil
}

func initLogger() *slog.Logger {
	serviceName := os.Getenv("K_SERVICE")
	if serviceName == "" {
		serviceName = os.Getenv("SERVICE_NAME")
	}
	if serviceName == "" {
		serviceName = "nats-bridge"
	}

	env := logging.EnvDev
	if e := os.Getenv("ENV"); e != "" {
		env = logging.Environment(e)
	}

	projectID := os.Getenv("GOOGLE_CLOUD_PROJECT")
	if projectID == "" {
		projectID = os.Getenv("GCLOUD_PROJECT_ID")
	}

	handler := logging.NewHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}, logging.HandlerConfig{
		ServiceInfo: logging.ServiceInfo{
			Name:     serviceName,
			Version:  Version,
			Revision: os.Getenv("K_REVISION"),
		},
		Environment:   env,
		GCPProject:    projectID,
		DefaultModule: logging.Module("message"),
	})

	return slog.New(handler)
}

func initTracing(ctx context.Context) (*tracing.Provider, error) {
	serviceName := os.Getenv("K_SERVICE")
	if serviceName == "" {
		serviceName = os.Getenv("SERVICE_NAME")
	}
	if serviceName == "" {
		serviceName = "nats-bridge"
	}

	env := logging.EnvDev
	if e := os.Getenv("ENV"); e != "" {
		env = logging.Environment(e)
	}

	tp, err := tracing.NewProvider(ctx, tracing.Config{
		ServiceName:    serviceName,
		ServiceVersion: Version,
		Environment:    string(env),
		SamplingRate:   1.0,
	})
	if err != nil {
		return nil, err
	}

	tp.SetGlobalProvider()

	return tp, nil
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
				msg.Nack()
			} else {
				msg.Ack()
			}
		}
	}
}

func processMessage(ctx context.Context, client *http.Client, route *Route, msg *message.Message) error {
	// Extract trace context from NATS message metadata
	carrier := make(map[string]string)
	for k, v := range msg.Metadata {
		carrier[k] = v
	}
	ctx = otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(carrier))

	requestID := logging.ValidateAndExtractRequestID(msg.Metadata["x-request-id"])
	ctx = logging.WithRequestID(ctx, requestID)

	tracer := otel.Tracer("github.com/KasumiMercury/primind-nats-bridge/cmd")
	ctx, span := tracer.Start(ctx, "message.forward")
	defer span.End()

	slog.DebugContext(ctx, "processing message",
		slog.String("message_id", msg.UUID),
		slog.String("subject", route.Subject),
		slog.Int("payload_size", len(msg.Payload)),
	)

	req, err := http.NewRequestWithContext(ctx, route.Method, route.Endpoint, bytes.NewReader(msg.Payload))
	if err != nil {
		slog.ErrorContext(ctx, "failed to create request",
			slog.String("event", "message.forward.fail"),
			slog.String("message_id", msg.UUID),
			slog.String("subject", route.Subject),
			slog.String("error", err.Error()),
		)
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", route.ContentType)
	req.Header.Set("x-request-id", requestID)
	if messageType := msg.Metadata.Get("message_type"); messageType != "" {
		req.Header.Set("message_type", messageType)
	} else if eventType := msg.Metadata.Get("event_type"); eventType != "" {
		req.Header.Set("message_type", eventType)
	}
	for k, v := range msg.Metadata {
		req.Header.Set("X-NATS-"+k, v)
	}

	// Inject trace context into HTTP request headers
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))

	resp, err := client.Do(req)
	if err != nil {
		slog.ErrorContext(ctx, "failed to send request",
			slog.String("event", "message.forward.fail"),
			slog.String("message_id", msg.UUID),
			slog.String("subject", route.Subject),
			slog.String("error", err.Error()),
		)
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			slog.WarnContext(ctx, "failed to close response body",
				slog.String("message_id", msg.UUID),
				slog.String("subject", route.Subject),
				slog.String("error", err.Error()),
			)
		}
	}()

	if resp.StatusCode >= 400 {
		slog.ErrorContext(ctx, "target returned error status",
			slog.String("event", "message.forward.fail"),
			slog.String("message_id", msg.UUID),
			slog.String("subject", route.Subject),
			slog.String("endpoint", route.Endpoint),
			slog.Int("status_code", resp.StatusCode),
		)
		return fmt.Errorf("target returned error status: %d", resp.StatusCode)
	}

	slog.InfoContext(ctx, "message forwarded successfully",
		slog.String("event", "message.forward"),
		slog.String("message_id", msg.UUID),
		slog.String("subject", route.Subject),
		slog.String("endpoint", route.Endpoint),
		slog.Int("status_code", resp.StatusCode),
	)

	return nil
}

// startHealthServer starts an HTTP server for health check endpoints.
func startHealthServer(natsURL, version string) *http.Server {
	mux := http.NewServeMux()

	// Liveness probe - just checks if the process is running
	mux.HandleFunc("/health/live", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(map[string]string{"status": "ok"}); err != nil {
			slog.Warn("failed to write health response", slog.String("error", err.Error()))
		}
	})

	// Readiness probe - checks NATS connectivity
	mux.HandleFunc("/health/ready", func(w http.ResponseWriter, r *http.Request) {
		response := map[string]any{
			"status":  "healthy",
			"version": version,
			"checks": map[string]any{
				"nats": map[string]string{"status": "healthy"},
			},
		}

		// Check NATS connectivity
		conn, err := nc.Connect(natsURL, nc.Timeout(2*time.Second))
		if err != nil {
			response["status"] = "unhealthy"
			response["checks"] = map[string]any{
				"nats": map[string]any{
					"status": "unhealthy",
					"error":  err.Error(),
				},
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			if err := json.NewEncoder(w).Encode(response); err != nil {
				slog.Warn("failed to write health response", slog.String("error", err.Error()))
			}
			return
		}
		conn.Close()

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(response); err != nil {
			slog.Warn("failed to write health response", slog.String("error", err.Error()))
		}
	})

	// Default health endpoint - same as readiness
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/health/ready", http.StatusTemporaryRedirect)
	})

	port := os.Getenv("HEALTH_PORT")
	if port == "" {
		port = "8080"
	}

	server := &http.Server{
		Addr:              ":" + port,
		Handler:           mux,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		slog.Info("starting health server", slog.String("port", port))
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("health server error", slog.String("error", err.Error()))
		}
	}()

	return server
}
