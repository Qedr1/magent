GO ?= go
APP ?= magent
BIN_DIR ?= bin
BIN ?= $(BIN_DIR)/$(APP)

# Production build flags:
# -trimpath: remove local file system paths
# -buildvcs=false: avoid embedding VCS metadata (reproducible builds)
BUILD_FLAGS ?= -trimpath -buildvcs=false
# -s -w: strip symbol table and DWARF (smaller binaries)
LDFLAGS ?= -s -w

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
