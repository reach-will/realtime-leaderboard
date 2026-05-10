KAFKA_ADDR := localhost:9092
REDIS_ADDR := localhost:6379
GRPC_ADDR  := :50051

# Admin ports (ascending from :2112)
API_ADMIN_ADDR         := :2113
RATING_ADMIN_ADDR      := :2112
GAMESESSION_ADMIN_ADDR := :2114

# Kafka topics
MATCHMAKING_MATCH_FOUND_KAFKA_TOPIC     := matchmaking.match.found
GAMESESSION_MATCH_COMPLETED_KAFKA_TOPIC := gamesession.match.completed

# Consumer groups
RATING_GROUP_ID      := rating.consumer
GAMESESSION_GROUP_ID := gamesession.consumer

.PHONY: up down ps topics proto top10 api matchpairsimulator matchpairloadgen matchoutcomesimulator matchoutcomeloadgen rating gamesession

up:
	docker compose up -d
	@echo "Prometheus: http://localhost:9090"
	@echo "Grafana:    http://localhost:3000  (admin/admin)"

down:
	docker compose down

ps:
	docker compose ps

topics:
	docker exec -it realtime-leaderboard-kafka-1 \
		/opt/kafka/bin/kafka-topics.sh \
		--create --if-not-exists \
		--topic $(MATCHMAKING_MATCH_FOUND_KAFKA_TOPIC) \
		--bootstrap-server localhost:9092 \
		--partitions 3 \
		--replication-factor 1
	docker exec -it realtime-leaderboard-kafka-1 \
		/opt/kafka/bin/kafka-topics.sh \
		--create --if-not-exists \
		--topic $(GAMESESSION_MATCH_COMPLETED_KAFKA_TOPIC) \
		--bootstrap-server localhost:9092 \
		--partitions 3 \
		--replication-factor 1

top10:
	docker exec -it realtime-leaderboard-redis-1 redis-cli ZREVRANGE scores:global 0 9 WITHSCORES

proto:
	buf generate

api:
	GRPC_ADDR=$(GRPC_ADDR) REDIS_ADDR=$(REDIS_ADDR) ADMIN_ADDR=$(API_ADMIN_ADDR) \
	go run -tags dev ./cmd/api

matchpairsimulator:
	KAFKA_ADDR=$(KAFKA_ADDR) \
	MATCHMAKING_MATCH_FOUND_KAFKA_TOPIC=$(MATCHMAKING_MATCH_FOUND_KAFKA_TOPIC) \
	go run ./cmd/matchpairsimulator

matchpairloadgen:
	KAFKA_ADDR=$(KAFKA_ADDR) \
	MATCHMAKING_MATCH_FOUND_KAFKA_TOPIC=$(MATCHMAKING_MATCH_FOUND_KAFKA_TOPIC) \
	go run ./cmd/matchpairloadgen

matchoutcomesimulator:
	KAFKA_ADDR=$(KAFKA_ADDR) \
	GAMESESSION_MATCH_COMPLETED_KAFKA_TOPIC=$(GAMESESSION_MATCH_COMPLETED_KAFKA_TOPIC) \
	go run ./cmd/matchoutcomesimulator

matchoutcomeloadgen:
	KAFKA_ADDR=$(KAFKA_ADDR) \
	GAMESESSION_MATCH_COMPLETED_KAFKA_TOPIC=$(GAMESESSION_MATCH_COMPLETED_KAFKA_TOPIC) \
	go run ./cmd/matchoutcomeloadgen

gamesession:
	KAFKA_ADDR=$(KAFKA_ADDR) \
	MATCHMAKING_MATCH_FOUND_KAFKA_TOPIC=$(MATCHMAKING_MATCH_FOUND_KAFKA_TOPIC) \
	GAMESESSION_MATCH_COMPLETED_KAFKA_TOPIC=$(GAMESESSION_MATCH_COMPLETED_KAFKA_TOPIC) \
	GAMESESSION_GROUP_ID=$(GAMESESSION_GROUP_ID) \
	ADMIN_ADDR=$(GAMESESSION_ADMIN_ADDR) \
	go run ./cmd/gamesession

rating:
	KAFKA_ADDR=$(KAFKA_ADDR) \
	GAMESESSION_MATCH_COMPLETED_KAFKA_TOPIC=$(GAMESESSION_MATCH_COMPLETED_KAFKA_TOPIC) \
	RATING_KAFKA_GROUP_ID=$(RATING_GROUP_ID) \
	REDIS_ADDR=$(REDIS_ADDR) ADMIN_ADDR=$(RATING_ADMIN_ADDR) \
	go run ./cmd/rating
