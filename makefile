BINARY_NAME=streamweaverbroker

test:
	@go test -v ./...

lint:
	@golangci-lint run

build:
	@mkdir -p bin
	@go build -o bin/$(BINARY_NAME) cmd/main.go

build-linux:
	@mkdir -p bin
	@GOOS=linux GOARCH=amd64 go build -o bin/$(BINARY_NAME)-linux pkg/main.go

build-linux-arm:
	@mkdir -p bin
	@GOOS=linux GOARCH=arm64 go build -o bin/$(BINARY_NAME)-linux-arm pkg/main.go

build-macos:
	@mkdir -p bin
	@GOOS=darwin GOARCH=amd64 go build -o bin/$(BINARY_NAME)-macos pkg/main.go

build-windows:
	@mkdir -p bin
	@GOOS=windows GOARCH=amd64 go build -o bin/$(BINARY_NAME).exe pkg/main.go

build-all: build-linux build-linux-arm build-macos build-windows

run: build
	@./bin/$(BINARY_NAME) start

clean:
	@rm -rf bin

deps:
	@go mod tidy

start_local_infra:
	@docker compose up -d

stop_local_infra:
	@docker compose down && docker compose down -v

test_local_cluster:
	@echo "Running tests on local cluster, testing cluster status..."
	@docker exec node-1 redis-cli cluster info
	@echo "Running tests on local cluster, testing cluster nodes..."
	@docker exec node-1 redis-cli cluster nodes
	@echo "Running tests on local cluster, testing data replication..."
	@docker exec node-1 redis-cli set key1 value1
	@docker exec node-3 redis-cli get key1
