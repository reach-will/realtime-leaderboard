KAFKA_ADDR     := localhost:9092
KAFKA_TOPIC    := leaderboard.match.completed
KAFKA_GROUP_ID := leaderboard.ingester
REDIS_ADDR     := localhost:6379

.PHONY: up down topic top10 proto api simulator ingester ps

up:
	docker compose up -d
	@echo "Prometheus: http://localhost:9090"
	@echo "Grafana:    http://localhost:3000  (admin/admin)"

down:
	docker compose down

ps:
	docker compose ps

topic:
	docker exec -it realtime-leaderboard-kafka-1 \
		/opt/kafka/bin/kafka-topics.sh \
		--create --if-not-exists \
		--topic $(KAFKA_TOPIC) \
		--bootstrap-server localhost:9092 \
		--partitions 3 \
		--replication-factor 1

top10:
	docker exec -it realtime-leaderboard-redis-1 redis-cli ZREVRANGE leaderboard:global 0 9 WITHSCORES

proto:
	buf generate

api:
	REDIS_ADDR=$(REDIS_ADDR) \
	go run -tags dev ./cmd/api
	@echo "gRPC:  localhost:50051"
	@echo "Admin: http://localhost:2113  (/metrics, /healthz)"

simulator:
	KAFKA_URL=$(KAFKA_ADDR) KAFKA_TOPIC=$(KAFKA_TOPIC) \
	go run ./cmd/simulator

ingester:
	KAFKA_URL=$(KAFKA_ADDR) KAFKA_TOPIC=$(KAFKA_TOPIC) \
	KAFKA_GROUP_ID=$(KAFKA_GROUP_ID) REDIS_ADDR=$(REDIS_ADDR) \
	go run ./cmd/ingester
	@echo "Admin: http://localhost:2112  (/metrics, /healthz)"
