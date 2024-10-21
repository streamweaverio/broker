BINARY_NAME=streamweaver

PROTO_DIR = proto/
PROTO_FILE = $(PROTO_DIR)broker.proto

# Output paths
OUTPUT_GO = ./outputs/go
OUTPUT_NODE = ./outputs/node

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
	@./bin/$(BINARY_NAME)

clean:
	@rm -rf bin

deps:
	@go mod tidy

# Protobuf code generation

gen_go:
	@mkdir -p $(OUTPUT_GO)
	protoc -I $(PROTO_DIR) $(PROTO_FILE) \
	--go_out=$(OUTPUT_GO) \
	--go-grpc_out=$(OUTPUT_GO)

gen_node_ts:
	@mkdir -p $(OUTPUT_NODE)
	protoc -I $(PROTO_DIR) $(PROTO_FILE) \
	--js_out=import_style=commonjs,binary:$(OUTPUT_NODE) \
	--grpc-web_out=import_style=commonjs,mode=grpcwebtext:$(OUTPUT_NODE)
