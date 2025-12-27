//go:build !gcloud

package logging

import (
	"context"
	"log/slog"
)

func gcpTraceAttrs(_ context.Context, _ string) []slog.Attr {
	return nil
}
