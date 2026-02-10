GO ?= go
APP ?= magent
BIN ?= bin/$(APP)

.PHONY: build test race fmt run clean

build:
	$(GO) build -o $(BIN) ./cmd/$(APP)

test:
	$(GO) test ./...

race:
	$(GO) test -race ./...

fmt:
	$(GO) fmt ./...

run:
	$(GO) run ./cmd/$(APP) -config config.example.toml

clean:
	rm -rf bin
