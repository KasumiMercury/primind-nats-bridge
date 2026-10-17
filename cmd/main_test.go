package main

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	nc "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	tcnats "github.com/testcontainers/testcontainers-go/modules/nats"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

const (
	testStream   = "SMOKE_EVENTS"
	testSubject  = "smoke.events"
	testConsumer = "smoke-bridge"
)

type forwarded struct {
	body   string
	header http.Header
}

func TestBridgeSmoke(t *testing.T) {
	ctx := t.Context()

	natsC, err := tcnats.Run(ctx, "nats:2-alpine")
	if err != nil {
		t.Fatalf("start nats: %v", err)
	}
	t.Cleanup(func() { _ = natsC.Terminate(context.Background()) })

	natsURL, err := natsC.ConnectionString(ctx)
	if err != nil {
		t.Fatal(err)
	}

	conn, err := nc.Connect(natsURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(conn.Close)

	js, err := jetstream.New(conn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: testStream, Subjects: []string{"smoke.>"}}); err != nil {
		t.Fatalf("create stream: %v", err)
	}

	received := make(chan forwarded, 10)
	var retryFailed atomic.Bool
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		received <- forwarded{body: string(body), header: r.Header.Clone()}
		if string(body) == `{"case":"retry"}` && retryFailed.CompareAndSwap(false, true) {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(endpoint.Close)

	healthPort := freePort(t)
	writeConfig(t, natsURL, endpoint.URL)
	t.Setenv("HEALTH_PORT", strconv.Itoa(healthPort))

	runCtx, cancel := context.WithCancel(ctx)
	runErr := make(chan error, 1)
	go func() { runErr <- run(runCtx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-runErr:
			if err != nil {
				t.Errorf("run returned %v", err)
			}
		case <-time.After(30 * time.Second):
			t.Error("run did not exit after cancel")
		}
	})

	healthClient := dialHealth(t, healthPort)
	waitHealth(t, healthClient, healthpb.HealthCheckResponse_SERVING)

	legacyJS, err := conn.JetStream()
	if err != nil {
		t.Fatal(err)
	}

	t.Run("forwards message and acks it", func(t *testing.T) {
		requestID := uuid.Must(uuid.NewV7()).String()
		msg := nc.NewMsg(testSubject)
		msg.Data = []byte(`{"case":"forward"}`)
		msg.Header.Set("x-request-id", requestID)
		msg.Header.Set("message_type", "smoke.created")
		if _, err := js.PublishMsg(ctx, msg); err != nil {
			t.Fatal(err)
		}

		got := next(t, received)
		if got.body != `{"case":"forward"}` {
			t.Errorf("body = %q", got.body)
		}
		for name, want := range map[string]string{
			"Content-Type":        "application/json",
			"X-Request-Id":        requestID,
			"Message_type":        "smoke.created",
			"X-NATS-message_type": "smoke.created",
			"X-NATS-x-request-id": requestID,
		} {
			if v := got.header.Get(name); v != want {
				t.Errorf("header %s = %q, want %q", name, v, want)
			}
		}

		waitSettled(t, legacyJS)
	})

	t.Run("redelivers after 5xx until acked", func(t *testing.T) {
		if _, err := js.Publish(ctx, testSubject, []byte(`{"case":"retry"}`)); err != nil {
			t.Fatal(err)
		}

		for attempt := 1; attempt <= 2; attempt++ {
			if got := next(t, received); got.body != `{"case":"retry"}` {
				t.Fatalf("attempt %d body = %q", attempt, got.body)
			}
		}

		waitSettled(t, legacyJS)
		select {
		case extra := <-received:
			t.Errorf("unexpected extra delivery %q", extra.body)
		case <-time.After(500 * time.Millisecond):
		}
	})

	t.Run("health reports NOT_SERVING after NATS stops", func(t *testing.T) {
		stopTimeout := 5 * time.Second
		if err := natsC.Stop(ctx, &stopTimeout); err != nil {
			t.Fatal(err)
		}
		waitHealth(t, healthClient, healthpb.HealthCheckResponse_NOT_SERVING)
	})
}

func writeConfig(t *testing.T, natsURL, endpoint string) {
	t.Helper()
	cfg, err := json.Marshal(FileConfig{
		NATS:   NATSConfig{URL: natsURL, Stream: testStream, ConsumerGroup: testConsumer},
		HTTP:   HTTPConfig{Timeout: "5s"},
		Routes: []Route{{Subject: testSubject, Endpoint: endpoint}},
	})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, cfg, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CONFIG_PATH", path)
	t.Setenv("NATS_URL", "")
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func dialHealth(t *testing.T, port int) healthpb.HealthClient {
	t.Helper()
	cc, err := grpc.NewClient("127.0.0.1:"+strconv.Itoa(port), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cc.Close() })
	return healthpb.NewHealthClient(cc)
}

func waitHealth(t *testing.T, client healthpb.HealthClient, want healthpb.HealthCheckResponse_ServingStatus) {
	t.Helper()
	var last string
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		resp, err := client.Check(ctx, &healthpb.HealthCheckRequest{})
		cancel()
		if err == nil && resp.GetStatus() == want {
			return
		}
		last = resp.GetStatus().String()
		if err != nil {
			last = err.Error()
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("health never reached %s, last: %s", want, last)
}

func next(t *testing.T, received <-chan forwarded) forwarded {
	t.Helper()
	select {
	case got := <-received:
		return got
	case <-time.After(15 * time.Second):
		t.Fatal("endpoint received nothing")
		return forwarded{}
	}
}

func waitSettled(t *testing.T, js nc.JetStreamContext) {
	t.Helper()
	var info *nc.ConsumerInfo
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var err error
		info, err = js.ConsumerInfo(testStream, testConsumer, nc.Context(t.Context()))
		if err != nil {
			t.Fatal(err)
		}
		if info.NumAckPending == 0 && info.NumPending == 0 {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("consumer not settled: ack_pending=%d pending=%d", info.NumAckPending, info.NumPending)
}
