package logging

import "context"

type contextKey string

const (
	requestIDKey contextKey = "x-request-id"
	moduleKey    contextKey = "module"
)

func WithRequestID(ctx context.Context, requestID string) context.Context {
	return context.WithValue(ctx, requestIDKey, requestID)
}

func RequestIDFromContext(ctx context.Context) string {
	v, ok := ctx.Value(requestIDKey).(string)
	if !ok {
		return ""
	}

	return v
}

func WithModule(ctx context.Context, module Module) context.Context {
	return context.WithValue(ctx, moduleKey, module)
}

func ModuleFromContext(ctx context.Context) Module {
	v, ok := ctx.Value(moduleKey).(Module)
	if !ok {
		return ""
	}

	return v
}
