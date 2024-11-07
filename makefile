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

set_redis_cluster_ip:
	@INTERFACE=$$(route get uninterrupted.tech | grep 'interface:' | awk '{print $$2}') && \
	if [ -n "$$INTERFACE" ]; then \
		export REDIS_CLUSTER_IP=$$(ipconfig getifaddr "$$INTERFACE"); \
		echo "Redis Cluster IP: $$REDIS_CLUSTER_IP"; \
	else \
		echo "Failed to determine the network interface."; \
	fi

local_infra_macos: set_redis_cluster_ip
	@docker compose up -d

stop_local_infra:
	@docker compose down && docker compose down -v
