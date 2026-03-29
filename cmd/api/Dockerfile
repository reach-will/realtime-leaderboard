FROM golang:1.24 AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o out/api ./cmd/api

FROM gcr.io/distroless/static
LABEL org.opencontainers.image.source="https://github.com/reach-will/realtime-leaderboard"
COPY --from=builder /app/out/api /api
ENTRYPOINT ["/api"]
