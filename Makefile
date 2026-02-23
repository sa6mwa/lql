.DEFAULT_GOAL := all

GO ?= go
GOFLAGS ?=
CGO_ENABLED ?= 0

PREFIX ?= /usr/local
DESTDIR ?=
BINDIR ?= $(PREFIX)/bin

BIN_DIR ?= bin
BIN_NAME ?= lql
BIN_PATH ?= $(BIN_DIR)/$(BIN_NAME)
CMD_PKG ?= ./cmd/lql

LDFLAGS ?= -s -w

PKG ?= ./...
BENCH ?= .
BENCHTIME ?= 1x
FUZZTIME ?= 10s

INSTALL ?= install
RM ?= rm -f
RMDIR ?= rm -rf

FUZZ_TARGETS ?= \
	FuzzParseSelectorString \
	FuzzParseSelectorShorthand \
	FuzzParseMutationsString \
	FuzzProjectFieldsParity \
	FuzzProjectFieldsSyntaxRobustness \
	FuzzQueryStreamStdlibSyntaxParity \
	FuzzQueryStreamSelectorParity \
	FuzzQueryStreamSpoolSelectorParity \
	FuzzQueryStreamMatchedOnlyParity \
	FuzzQueryStreamCallerSinkParity \
	FuzzMutateStreamWriterParity \
	FuzzMutateStreamCallbackParity \
	FuzzMutateStreamModeRobustness \
	FuzzMutateStreamProgramParity

.PHONY: all build clean install uninstall test benchmark fuzz vet lint check test-all

all: build

build: $(BIN_PATH)

$(BIN_PATH):
	@mkdir -p $(BIN_DIR)
	CGO_ENABLED=$(CGO_ENABLED) $(GO) build $(GOFLAGS) -trimpath -ldflags '$(LDFLAGS)' -o $(BIN_PATH) $(CMD_PKG)

clean:
	$(RMDIR) $(BIN_DIR)
	$(GO) clean $(GOFLAGS) $(PKG)

install:
	$(INSTALL) -d $(DESTDIR)$(BINDIR)
	$(INSTALL) -m 0755 $(BIN_PATH) $(DESTDIR)$(BINDIR)/$(BIN_NAME)

uninstall:
	$(RM) $(DESTDIR)$(BINDIR)/$(BIN_NAME)

test:
	$(GO) test $(GOFLAGS) -v -cover $(PKG)

benchmark:
	$(GO) test $(GOFLAGS) -run '^$$' -bench '$(BENCH)' -benchmem -benchtime=$(BENCHTIME) $(PKG)

fuzz:
	@set -e; \
	for target in $(FUZZ_TARGETS); do \
		echo "==> $$target"; \
		$(GO) test $(GOFLAGS) -run '^$$' -fuzz "$$target" -fuzztime=$(FUZZTIME) .; \
	done

vet:
	$(GO) vet $(GOFLAGS) $(PKG)

lint:
	golint $(PKG)
	golangci-lint run $(PKG)

check: test vet lint

test-all: check fuzz
