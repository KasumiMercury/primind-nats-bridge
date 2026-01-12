package health

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"connectrpc.com/grpchealth"
	nc "github.com/nats-io/nats.go"
)

// Status represents the health status of a service or dependency.
type Status string

const (
	StatusHealthy   Status = "healthy"
	StatusUnhealthy Status = "unhealthy"
)

// CheckResult represents the health check result for a single dependency.
type CheckResult struct {
	Status    Status `json:"status"`
	LatencyMs int64  `json:"latency_ms,omitempty"`
	Error     string `json:"error,omitempty"`
}

// HealthStatus represents the overall health status of the service.
type HealthStatus struct {
	Status  Status                 `json:"status"`
	Version string                 `json:"version,omitempty"`
	Checks  map[string]CheckResult `json:"checks,omitempty"`
}

// Checker performs health checks on service dependencies.
type Checker struct {
	natsURL string
	version string
}

// NewChecker creates a new health checker with the given dependencies.
func NewChecker(natsURL, version string) *Checker {
	return &Checker{
		natsURL: natsURL,
		version: version,
	}
}

// Check performs health checks on all dependencies and returns the overall status.
func (c *Checker) Check(_ context.Context) *HealthStatus {
	status := &HealthStatus{
		Status:  StatusHealthy,
		Version: c.version,
		Checks:  make(map[string]CheckResult),
	}

	// NATS check
	if c.natsURL != "" {
		start := time.Now()
		conn, err := nc.Connect(c.natsURL, nc.Timeout(2*time.Second))
		if err != nil {
			status.Status = StatusUnhealthy
			status.Checks["nats"] = CheckResult{
				Status: StatusUnhealthy,
				Error:  err.Error(),
			}
		} else {
			conn.Close()
			status.Checks["nats"] = CheckResult{
				Status:    StatusHealthy,
				LatencyMs: time.Since(start).Milliseconds(),
			}
		}
	}

	return status
}

// IsHealthy returns true if all dependencies are healthy.
func (c *Checker) IsHealthy(ctx context.Context) bool {
	return c.Check(ctx).Status == StatusHealthy
}

// LiveHandler returns an HTTP handler for liveness probes.
func (c *Checker) LiveHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// ReadyHandler returns an HTTP handler for readiness probes.
func (c *Checker) ReadyHandler(w http.ResponseWriter, r *http.Request) {
	status := c.Check(r.Context())

	w.Header().Set("Content-Type", "application/json")
	if status.Status != StatusHealthy {
		w.WriteHeader(http.StatusServiceUnavailable)
	} else {
		w.WriteHeader(http.StatusOK)
	}
	json.NewEncoder(w).Encode(status)
}

// GRPCChecker implements grpchealth.Checker interface for gRPC health checking protocol.
type GRPCChecker struct {
	checker *Checker
}

// NewGRPCChecker creates a new gRPC health checker wrapping the given Checker.
func NewGRPCChecker(checker *Checker) *GRPCChecker {
	return &GRPCChecker{checker: checker}
}

// Check implements grpchealth.Checker interface.
func (g *GRPCChecker) Check(ctx context.Context, req *grpchealth.CheckRequest) (*grpchealth.CheckResponse, error) {
	if g.checker.IsHealthy(ctx) {
		return &grpchealth.CheckResponse{
			Status: grpchealth.StatusServing,
		}, nil
	}
	return &grpchealth.CheckResponse{
		Status: grpchealth.StatusNotServing,
	}, nil
}
