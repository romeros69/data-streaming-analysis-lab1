.PHONY: build clean docker-down docker-stop docker-clean docker-ps docker-logs docker-restart test-docker-stdout test-docker-kafka

build:
	mkdir -p bin
	go build -o bin/log-generator ./cmd/log-generator

clean:
	rm -rf bin/
	go clean

# Docker команды
docker-down:
	docker-compose down

docker-stop:
	docker-compose stop

docker-ps:
	docker-compose ps

docker-logs:
	docker-compose logs -f

docker-restart:
	docker-compose restart

docker-clean:
	docker-compose down -v --remove-orphans
	docker system prune -f

test-docker-stdout:
	@docker build -t log-generator:latest . > /dev/null
	@docker run --rm -it \
		-v $(PWD)/config:/app/config:ro \
		log-generator:latest \
		--config /app/config/config.stdout.yaml

test-docker-kafka:
	@docker-compose up -d
	@sleep 20
	@echo "Kafka UI: http://localhost:8080"
	@echo "Топик: s3-gateway-logs"
