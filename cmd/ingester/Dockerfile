FROM golang:1.24 AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o out/ingester ./cmd/ingester

FROM gcr.io/distroless/static
LABEL org.opencontainers.image.source="https://github.com/reach-will/realtime-leaderboard"
COPY --from=builder /app/out/ingester /ingester
ENTRYPOINT ["/ingester"]
