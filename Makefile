SHELL := /bin/bash
MODULE := fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk

VERSION := $(shell git describe --tags --dirty --always 2>/dev/null || echo dev)

# Keep the linter version aligned with CI.
GOLANGCI_LINT_VERSION := v2.12.2
GOLANGCI_LINT := $(CURDIR)/.tools/bin/golangci-lint

export GOPRIVATE := fgit.zapps.vn

APISURFACE := $(CURDIR)/.tools/bin/apisurface
APIDIFF := $(CURDIR)/.tools/bin/apidiff
API_DIFF_BASELINE_DIR := $(CURDIR)/testdata/api-diff
API_DIFF_BREAKING_CHANGE ?= 0

.PHONY: build test-fast test test-chaos kpi lint verify-agnostic verify-self-contained check-cardinality swap-report check-fixture check-api-surface check-api-surface-codec check-api-surface-driver check-api-surface-f1test check-api-diff record-api-diff-baseline

## build: compile all packages with reproducible build flags.
build:
	CGO_ENABLED=0 go build -trimpath -ldflags "-X main.version=$(VERSION)" ./...

## test-fast: run short race-enabled tests and print coverage.
test-fast:
	go test -race -short -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out | tail -1

## test: run the full test suite.
test:
	go test -count=1 ./...

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

## check-cardinality: enforce the bounded metric label registry.
check-cardinality:
	go run ./tools/cardinality

## verify-self-contained: fail on citations no reader of this repository can resolve.
# Design records live elsewhere. A reader here - a broker vendor, an auditor, a
# future maintainer - cannot follow a decision or task identifier into a tree
# they do not have, so an unresolvable citation is worse than none. State the
# rule the citation stands for instead.
verify-self-contained:
	@! git grep -nIE '(ADR-[0-9]{4}|F-P[0-9]+|M1-[0-9]+)' -- . ':!Makefile' \
	  || { echo "verify-self-contained: citation above cannot be resolved from this repository"; exit 1; }
	@echo "verify-self-contained: 0 issues."

## swap-report: verify driver replacement when the migration suite exists.
swap-report:
	@echo "swap-report: migration suite is not available yet"

## check-fixture: validate the local fixtures used by tests and tooling.
check-fixture:
	go test ./...
	$(MAKE) check-api-surface check-api-surface-codec check-api-surface-driver check-api-surface-f1test check-api-diff

## check-api-surface: verify exported symbols against the f1 public API fixture.
check-api-surface: $(APISURFACE)
	$(APISURFACE) -package f1 -fixture testdata/public-api.json

## check-api-surface-codec: verify exported symbols against the codec public API fixture.
check-api-surface-codec: $(APISURFACE)
	$(APISURFACE) -package codec -fixture testdata/public-api-codec.json

## check-api-surface-driver: verify exported driver symbols against the port fixture.
check-api-surface-driver: $(APISURFACE)
	$(APISURFACE) -package driver -fixture testdata/public-api-driver.json

## check-api-surface-f1test: verify exported f1test symbols against its fixture.
check-api-surface-f1test: $(APISURFACE)
	$(APISURFACE) -package f1test -fixture testdata/public-api-f1test.json

## check-api-diff: report API changes and fail on incompatible changes.
check-api-diff: $(APIDIFF)
	@set -e; \
	for spec in \
		"f1:$(MODULE):f1.export" \
		"driver:$(MODULE)/driver:driver.export" \
		"codec:$(MODULE)/codec:codec.export"; do \
		package=$${spec%%:*}; rest=$${spec#*:}; import_path=$${rest%%:*}; baseline=$${rest#*:}; \
		report=$$(mktemp); \
		trap 'rm -f "$$report"' EXIT; \
		$(APIDIFF) "$(API_DIFF_BASELINE_DIR)/$$baseline" "$$import_path" >"$$report"; \
		cat "$$report"; \
		if grep -q '^Incompatible changes:' "$$report"; then \
			echo "api-diff: $$package has incompatible changes" >&2; \
			exit 1; \
		elif [ ! -s "$$report" ]; then \
			echo "api-diff: $$package unchanged"; \
		fi; \
		rm -f "$$report"; \
	done

## record-api-diff-baseline: refresh committed snapshots; breaking changes require explicit approval.
record-api-diff-baseline: $(APIDIFF)
	@if [ -f "$(API_DIFF_BASELINE_DIR)/f1.export" ] && \
		[ -f "$(API_DIFF_BASELINE_DIR)/driver.export" ] && \
		[ -f "$(API_DIFF_BASELINE_DIR)/codec.export" ] && \
		[ "$(API_DIFF_BREAKING_CHANGE)" != "1" ]; then \
		$(MAKE) check-api-diff; \
	elif [ "$(API_DIFF_BREAKING_CHANGE)" = "1" ]; then \
		echo "api-diff: refreshing baseline with breaking-change approval"; \
	else \
		echo "api-diff: creating initial baseline"; \
	fi
	@mkdir -p "$(API_DIFF_BASELINE_DIR)"
	$(APIDIFF) -w "$(API_DIFF_BASELINE_DIR)/f1.export" $(MODULE)
	$(APIDIFF) -w "$(API_DIFF_BASELINE_DIR)/driver.export" $(MODULE)/driver
	$(APIDIFF) -w "$(API_DIFF_BASELINE_DIR)/codec.export" $(MODULE)/codec

$(APISURFACE): tools/apisurface/main.go tools/apisurface/go.mod
	cd tools/apisurface && go build -o ../../.tools/bin/apisurface .

$(APIDIFF): tools/apidiff/go.mod tools/apidiff/go.sum
	cd tools/apidiff && go build -o ../../.tools/bin/apidiff golang.org/x/exp/cmd/apidiff
