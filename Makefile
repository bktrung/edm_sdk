SHELL := /bin/bash
MODULE := fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk

VERSION := $(shell git describe --tags --dirty --always 2>/dev/null || echo dev)

# Keep the linter version aligned with CI.
GOLANGCI_LINT_VERSION := v2.12.2
GOLANGCI_LINT := $(CURDIR)/.tools/bin/golangci-lint

export GOPRIVATE := fgit.zapps.vn

APISURFACE := $(CURDIR)/.tools/bin/apisurface

.PHONY: build test-fast test test-chaos kpi lint verify-agnostic swap-report check-fixture check-api-surface check-api-surface-codec

## build: compile all packages with reproducible build flags.
build:
	CGO_ENABLED=0 go build -trimpath -ldflags "-X main.version=$(VERSION)" ./...

## test-fast: run short race-enabled tests and print coverage.
test-fast:
	go test -race -short -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out | tail -1

## test: run the full test suite when conformance and integration tests exist.
test:
	@echo "test: conformance and integration suites are not available yet"

## test-chaos: run the chaos test matrix when it exists.
test-chaos:
	@echo "test-chaos: chaos matrix is not available yet"

## kpi: run the performance and delivery KPI harness when it exists.
kpi:
	@echo "kpi: performance harness is not available yet"

## lint: run the pinned golangci-lint configuration.
$(GOLANGCI_LINT):
	GOBIN=$(CURDIR)/.tools/bin go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)

lint: $(GOLANGCI_LINT)
	$(GOLANGCI_LINT) run ./...

## verify-agnostic: check broker import boundaries with depguard.
verify-agnostic: $(GOLANGCI_LINT)
	$(GOLANGCI_LINT) run --enable-only depguard ./...

## swap-report: verify driver replacement when the migration suite exists.
swap-report:
	@echo "swap-report: migration suite is not available yet"

## check-fixture: validate the local fixtures used by tests and tooling.
check-fixture:
	go test ./...
	$(MAKE) check-api-surface check-api-surface-codec

## check-api-surface: verify exported symbols against the f1 public API fixture.
check-api-surface: $(APISURFACE)
	$(APISURFACE) -package f1 -fixture testdata/public-api.json

## check-api-surface-codec: verify exported symbols against the codec public API fixture.
check-api-surface-codec: $(APISURFACE)
	$(APISURFACE) -package codec -fixture testdata/public-api-codec.json

$(APISURFACE): tools/apisurface/main.go tools/apisurface/go.mod
	cd tools/apisurface && go build -o ../../.tools/bin/apisurface .
