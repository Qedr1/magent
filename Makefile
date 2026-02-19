GO ?= go
APP ?= magent
BIN_DIR ?= bin
BIN ?= $(BIN_DIR)/$(APP)
TAG ?= $(shell git describe --tags --abbrev=0 2>/dev/null || echo v0.0.0)
BUILD_NUM ?= $(shell git rev-list --count HEAD 2>/dev/null || echo 0)
VERSION ?= $(TAG).$(BUILD_NUM)
COMMIT ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo none)
BUILD_DATE ?= $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")

# Production build flags:
# -trimpath: remove local file system paths
# -buildvcs=false: avoid embedding VCS metadata (reproducible builds)
BUILD_FLAGS ?= -trimpath -buildvcs=false
# -s -w: strip symbol table and DWARF (smaller binaries)
LDFLAGS ?= -s -w \
	-X 'main.version=$(VERSION)' \
	-X 'main.commit=$(COMMIT)' \
	-X 'main.date=$(BUILD_DATE)'

.PHONY: build build-upx test race fmt run clean

build:
	mkdir -p $(BIN_DIR)
	$(GO) build $(BUILD_FLAGS) -ldflags "$(LDFLAGS)" -o $(BIN) ./cmd/$(APP)

# Optional: pack binary with UPX if installed.
build-upx: build
	command -v upx >/dev/null 2>&1
	upx --best --lzma $(BIN)

test:
	$(GO) test ./...

race:
	$(GO) test -race ./...

fmt:
	$(GO) fmt ./...

run:
	$(GO) run ./cmd/$(APP) -config config.example.toml

clean:
	rm -rf $(BIN_DIR)
