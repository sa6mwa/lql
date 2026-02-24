.DEFAULT_GOAL := all

GO ?= go
GOFLAGS ?=
CGO_ENABLED ?= 0
CPU_LIMIT ?=
GO_RUN = $(if $(CPU_LIMIT),GOMAXPROCS=$(CPU_LIMIT) )$(GO)

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
BENCH_SMOKE ?= Benchmark(QueryStreamSynthetic|MutateStreamSynthetic|ProjectFieldsSynthetic|ProjectFieldsBatchSynthetic|QueryStreamCapturePolicyLowMatch|QueryStreamCapturePolicyObjectRootPruning|LQLSelectionBaseline)
BENCH_FULL ?= $(BENCH)
BENCH_LOCKD ?= BenchmarkLockdFixtures
BENCHTIME_SMOKE ?= 1x
BENCHTIME_FULL ?= $(BENCHTIME)
BENCHTIME_LOCKD ?= 1x
BASELINE_DIR ?= perf/baselines
LOCKD_BASELINE_FILE ?= $(BASELINE_DIR)/$(shell date +%Y-%m-%d)-lockd-fixture-$(shell git rev-parse --short HEAD).txt
FUZZTIME_SMOKE ?= 2s
FUZZTIME_FULL ?= $(FUZZTIME)
LOCKD_TEST_RE ?= ^TestLockd

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

FUZZ_TARGETS_SMOKE ?= \
	FuzzParseSelectorString \
	FuzzParseMutationsString \
	FuzzQueryStreamStdlibSyntaxParity \
	FuzzProjectFieldsSyntaxRobustness \
	FuzzMutateStreamModeRobustness

.PHONY: all build clean install uninstall test test-short test-cover test-lockd benchmark benchmark-smoke benchmark-full benchmark-lockd benchmark-lockd-save benchmark-lockd-compare fuzz fuzz-smoke fuzz-full vet lint check check-full test-all test-all-full ci-smoke ci-lockd ci-full help

all: build

build: $(BIN_PATH)

$(BIN_PATH):
	@mkdir -p $(BIN_DIR)
	CGO_ENABLED=$(CGO_ENABLED) $(GO_RUN) build $(GOFLAGS) -trimpath -ldflags '$(LDFLAGS)' -o $(BIN_PATH) $(CMD_PKG)

clean:
	$(RMDIR) $(BIN_DIR)
	$(GO_RUN) clean $(GOFLAGS) $(PKG)

install:
	$(INSTALL) -d $(DESTDIR)$(BINDIR)
	$(INSTALL) -m 0755 $(BIN_PATH) $(DESTDIR)$(BINDIR)/$(BIN_NAME)

uninstall:
	$(RM) $(DESTDIR)$(BINDIR)/$(BIN_NAME)

test:
	$(GO_RUN) test $(GOFLAGS) $(PKG)

test-short:
	$(GO_RUN) test $(GOFLAGS) -short $(PKG)

test-cover:
	$(GO_RUN) test $(GOFLAGS) -cover $(PKG)

test-lockd:
	$(GO_RUN) test $(GOFLAGS) -run '$(LOCKD_TEST_RE)' $(PKG)

benchmark:
	$(MAKE) benchmark-smoke

benchmark-smoke:
	$(GO_RUN) test $(GOFLAGS) -run '^$$' -bench '$(BENCH_SMOKE)' -benchmem -benchtime=$(BENCHTIME_SMOKE) $(PKG)

benchmark-full:
	$(GO_RUN) test $(GOFLAGS) -run '^$$' -bench '$(BENCH_FULL)' -benchmem -benchtime=$(BENCHTIME_FULL) $(PKG)

benchmark-lockd:
	$(GO_RUN) test $(GOFLAGS) -run '^$$' -bench '$(BENCH_LOCKD)' -benchmem -benchtime=$(BENCHTIME_LOCKD) .

benchmark-lockd-save:
	@mkdir -p $(BASELINE_DIR)
	$(GO_RUN) test $(GOFLAGS) -run '^$$' -bench '$(BENCH_LOCKD)' -benchmem -benchtime=$(BENCHTIME_LOCKD) . | tee $(LOCKD_BASELINE_FILE)
	@echo "saved lockd baseline: $(LOCKD_BASELINE_FILE)"

benchmark-lockd-compare:
	@if [ -z "$(OLD)" ] || [ -z "$(NEW)" ]; then \
		echo "usage: make benchmark-lockd-compare OLD=perf/baselines/old.txt NEW=perf/baselines/new.txt"; \
		exit 1; \
	fi
	@if ! command -v benchstat >/dev/null 2>&1; then \
		echo "benchstat not found. install with: go install golang.org/x/perf/cmd/benchstat@latest"; \
		exit 1; \
	fi
	benchstat $(OLD) $(NEW)

fuzz:
	$(MAKE) fuzz-smoke

fuzz-smoke:
	@set -e; \
	for target in $(FUZZ_TARGETS_SMOKE); do \
		echo "==> $$target"; \
		$(GO_RUN) test $(GOFLAGS) -run '^$$' -fuzz "$$target" -fuzztime=$(FUZZTIME_SMOKE) .; \
	done

fuzz-full:
	@set -e; \
	for target in $(FUZZ_TARGETS); do \
		echo "==> $$target"; \
		$(GO_RUN) test $(GOFLAGS) -run '^$$' -fuzz "$$target" -fuzztime=$(FUZZTIME_FULL) .; \
	done

vet:
	$(GO_RUN) vet $(GOFLAGS) $(PKG)

lint:
	golint $(PKG)
	golangci-lint run $(PKG)

check: test vet lint

check-full: test-cover vet lint

test-all: check fuzz benchmark

test-all-full: check-full fuzz-full benchmark-full

ci-smoke:
	$(MAKE) test-all

ci-lockd:
	$(MAKE) CPU_LIMIT=$${CPU_LIMIT:-2} test-lockd benchmark-lockd

ci-full:
	$(MAKE) CPU_LIMIT=$${CPU_LIMIT:-2} test-all-full
	$(MAKE) CPU_LIMIT=$${CPU_LIMIT:-2} test-lockd benchmark-lockd

help:
	@echo "Common targets:"
	@echo "  make (or make build)      Build $(BIN_PATH)"
	@echo "  make test                 Run fast test pass (default flags)"
	@echo "  make test-short           Run tests with -short"
	@echo "  make test-cover           Run tests with coverage"
	@echo "  make benchmark            Run smoke benchmarks"
	@echo "  make benchmark-full       Run full benchmark suite"
	@echo "  make benchmark-lockd      Run lockd fixture benchmarks"
	@echo "  make benchmark-lockd-save Run lockd benchmarks and save baseline file"
	@echo "  make benchmark-lockd-compare OLD=... NEW=...  Compare lockd baselines"
	@echo "  make fuzz                 Run smoke fuzzers"
	@echo "  make fuzz-full            Run full fuzz matrix"
	@echo "  make test-lockd           Run lockd contract/perf-guard tests"
	@echo "  make check                test + vet + lint"
	@echo "  make check-full           test-cover + vet + lint"
	@echo "  make test-all             check + smoke fuzz + smoke bench"
	@echo "  make test-all-full        check-full + full fuzz + full bench"
	@echo "  make ci-smoke             CI smoke profile"
	@echo "  make ci-lockd             CI lockd profile"
	@echo "  make ci-full              CI full profile"
	@echo ""
	@echo "Tuning:"
	@echo "  CPU_LIMIT=2 make test-all        # cap Go runtime CPU usage"
	@echo "  FUZZTIME_SMOKE=5s make fuzz      # longer smoke fuzz pass"
	@echo "  FUZZTIME_FULL=60s make fuzz-full # longer full fuzz pass"
