FROM golang:1.25-alpine AS builder
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/log-generator ./cmd/log-generator

FROM alpine:latest
WORKDIR /app
COPY --from=builder /bin/log-generator /app/log-generator
COPY config/ /app/config/
ENTRYPOINT ["/app/log-generator"]
CMD ["--config", "/app/config/config.yaml"]
