FROM golang:1.25-alpine AS builder

RUN apk update

ARG GRPC_HEALTH_PROBE_VERSION=v0.4.28
RUN wget -qO /grpc_health_probe https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-amd64 && \
    chmod +x /grpc_health_probe

WORKDIR /app

COPY . .
RUN go mod download

ARG VERSION=dev
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w -X main.Version=${VERSION}" -o /app/main ./cmd/

FROM golang:1.25-alpine AS dev

ENV CGO_ENABLED=0
ENV GO111MODULE=auto

ARG GRPC_HEALTH_PROBE_VERSION=v0.4.28
RUN apk update && \
    apk add --no-cache bash wget && \
    wget -qO /usr/local/bin/grpc_health_probe https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-amd64 && \
    chmod +x /usr/local/bin/grpc_health_probe

WORKDIR /app

RUN go install github.com/air-verse/air@latest

CMD ["air", "-c", ".air.toml"]

FROM gcr.io/distroless/base-debian12 AS runner

COPY --from=builder /grpc_health_probe /grpc_health_probe
COPY --from=builder /app/main /

CMD ["/main"]
