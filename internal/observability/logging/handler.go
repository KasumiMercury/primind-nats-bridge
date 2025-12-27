package logging

import (
	"context"
	"io"
	"log/slog"

	"go.opentelemetry.io/otel/trace"
)

type Handler struct {
	inner         slog.Handler
	serviceInfo   ServiceInfo
	env           Environment
	gcpProject    string
	defaultModule Module
}

type HandlerConfig struct {
	ServiceInfo   ServiceInfo
	Environment   Environment
	GCPProject    string // empty for non-GCP environments
	DefaultModule Module
}

func NewHandler(w io.Writer, opts *slog.HandlerOptions, cfg HandlerConfig) *Handler {
	if opts == nil {
		opts = &slog.HandlerOptions{Level: slog.LevelDebug}
	}

	originalReplaceAttr := opts.ReplaceAttr
	opts.ReplaceAttr = func(groups []string, a slog.Attr) slog.Attr {
		switch a.Key {
		case slog.TimeKey:
			a.Key = "timestamp"
		case slog.LevelKey:
			a.Key = "severity"
		case slog.MessageKey:
			a.Key = "message"
		}

		if originalReplaceAttr != nil {
			return originalReplaceAttr(groups, a)
		}

		return a
	}

	return &Handler{
		inner:         slog.NewJSONHandler(w, opts),
		serviceInfo:   cfg.ServiceInfo,
		env:           cfg.Environment,
		gcpProject:    cfg.GCPProject,
		defaultModule: cfg.DefaultModule,
	}
}

func (h *Handler) Handle(ctx context.Context, r slog.Record) error {
	hasEvent := false
	hasModule := false
	hasRequestID := false

	r.Attrs(func(a slog.Attr) bool {
		switch a.Key {
		case "event":
			hasEvent = true
		case "module":
			hasModule = true
		case "x-request-id":
			hasRequestID = true
		}

		return true
	})

	if !hasEvent {
		r.AddAttrs(slog.String("event", "log.emit"))
	}

	r.AddAttrs(
		slog.String("service.name", h.serviceInfo.Name),
		slog.String("service.version", h.serviceInfo.Version),
		slog.String("service.revision", h.serviceInfo.Revision),
		slog.String("env", string(h.env)),
	)

	if !hasRequestID {
		requestID := RequestIDFromContext(ctx)
		if requestID == "" {
			requestID = generateRequestID()
		}

		r.AddAttrs(slog.String("x-request-id", requestID))
	}

	if !hasModule {
		module := ModuleFromContext(ctx)
		if module == "" {
			module = h.defaultModule
		}

		if module != "" {
			r.AddAttrs(slog.String("module", string(module)))
		}
	}

	span := trace.SpanFromContext(ctx)
	sc := span.SpanContext()
	traceID := ""
	spanID := ""
	traceSampled := false

	if sc.IsValid() {
		traceID = sc.TraceID().String()
		spanID = sc.SpanID().String()
		traceSampled = sc.IsSampled()
	}

	r.AddAttrs(
		slog.String("trace_id", traceID),
		slog.String("span_id", spanID),
		slog.Bool("trace_sampled", traceSampled),
	)

	for _, attr := range gcpTraceAttrs(ctx, h.gcpProject) {
		r.AddAttrs(attr)
	}

	return h.inner.Handle(ctx, r)
}

func (h *Handler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

func (h *Handler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &Handler{
		inner:         h.inner.WithAttrs(attrs),
		serviceInfo:   h.serviceInfo,
		env:           h.env,
		gcpProject:    h.gcpProject,
		defaultModule: h.defaultModule,
	}
}

func (h *Handler) WithGroup(name string) slog.Handler {
	return &Handler{
		inner:         h.inner.WithGroup(name),
		serviceInfo:   h.serviceInfo,
		env:           h.env,
		gcpProject:    h.gcpProject,
		defaultModule: h.defaultModule,
	}
}
