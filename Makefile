SHELL := /bin/bash
MODULE := fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk

VERSION := $(shell git describe --tags --dirty --always 2>/dev/null || echo dev)

# Keep the linter version aligned with CI.
GOLANGCI_LINT_VERSION := v2.12.2
GOLANGCI_LINT := $(CURDIR)/.tools/bin/golangci-lint

export GOPRIVATE := fgit.zapps.vn

APISURFACE := $(CURDIR)/.tools/bin/apisurface
APIDIFF := $(CURDIR)/.tools/bin/apidiff
APIDIFF_NORMALIZE := $(CURDIR)/.tools/bin/apidiff-normalize
API_DIFF_BASELINE_DIR := $(CURDIR)/testdata/api-diff
API_DIFF_BREAKING_CHANGE ?= 0
API_DIFF_ENFORCE ?= 1

.PHONY: build test-fast test lint verify-agnostic verify-self-contained check-fixture check-api-surface check-api-surface-codec check-api-surface-driver check-api-surface-f1test check-api-diff record-api-diff-baseline

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


## lint: run the pinned golangci-lint configuration.
$(GOLANGCI_LINT):
	GOBIN=$(CURDIR)/.tools/bin go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)

lint: $(GOLANGCI_LINT)
	$(GOLANGCI_LINT) run ./...

## verify-agnostic: check broker import boundaries with depguard.
verify-agnostic: $(GOLANGCI_LINT)
	$(GOLANGCI_LINT) run --enable-only depguard ./...

## verify-self-contained: fail on citations no reader of this repository can resolve.
# Design records live elsewhere. A reader here - a broker vendor, an auditor, a
# future maintainer - cannot follow a decision or task identifier into a tree
# they do not have, so an unresolvable citation is worse than none. State the
# rule the citation stands for instead.
verify-self-contained:
	@! git grep -nIE '(ADR-[0-9]{4}|F-P[0-9]+|M1-[0-9]+)' -- . ':!Makefile' \
	  || { echo "verify-self-contained: citation above cannot be resolved from this repository"; exit 1; }
	@echo "verify-self-contained: 0 issues."

## check-fixture: validate the local fixtures used by tests and tooling.
check-fixture:
	go test -count=1 ./...
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

## check-api-diff: report API changes; fails on incompatible ones only when API_DIFF_ENFORCE=1.
##
## Enforcement is on. Intentional breaking changes require an explicit maintainer
## approval when recording a new baseline: API_DIFF_BREAKING_CHANGE=1 make
## record-api-diff-baseline
check-api-diff: $(APIDIFF) $(APIDIFF_NORMALIZE)
	@set -e; \
	for spec in \
		"f1:$(MODULE):f1.export" \
		"driver:$(MODULE)/driver:driver.export" \
		"codec:$(MODULE)/codec:codec.export"; do \
		package=$${spec%%:*}; rest=$${spec#*:}; import_path=$${rest%%:*}; baseline=$${rest#*:}; \
		report=$$(mktemp); current=$$(mktemp); normalized_old=$$(mktemp); normalized_new=$$(mktemp); \
		trap 'rm -f "$$report" "$$current" "$$normalized_old" "$$normalized_new"' EXIT; \
		$(APIDIFF) -w "$$current" "$$import_path"; \
		$(APIDIFF_NORMALIZE) "$(API_DIFF_BASELINE_DIR)/$$baseline" "$$normalized_old"; \
		$(APIDIFF_NORMALIZE) "$$current" "$$normalized_new"; \
		$(APIDIFF) "$$normalized_old" "$$normalized_new" >"$$report"; \
		cat "$$report"; \
		if grep -q '^Incompatible changes:' "$$report"; then \
			if [ "$(API_DIFF_ENFORCE)" = "1" ]; then \
				echo "api-diff: $$package has incompatible changes" >&2; \
				exit 1; \
			fi; \
			echo "api-diff: $$package has incompatible changes (not enforced; set API_DIFF_ENFORCE=1)" >&2; \
		elif [ ! -s "$$report" ]; then \
			echo "api-diff: $$package unchanged"; \
		fi; \
		rm -f "$$report" "$$current" "$$normalized_old" "$$normalized_new"; \
	done

## record-api-diff-baseline: refresh committed snapshots; breaking changes require explicit approval.
record-api-diff-baseline: $(APIDIFF) $(APIDIFF_NORMALIZE)
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
	@set -e; \
	raw=; \
	trap 'rm -f "$$raw"' EXIT; \
	for spec in \
		"$(MODULE):f1.export" \
		"$(MODULE)/driver:driver.export" \
		"$(MODULE)/codec:codec.export"; do \
		import_path=$${spec%%:*}; baseline=$${spec#*:}; \
		raw=$$(mktemp); \
		$(APIDIFF) -w "$$raw" "$$import_path"; \
		$(APIDIFF_NORMALIZE) "$$raw" "$(API_DIFF_BASELINE_DIR)/$$baseline"; \
		rm -f "$$raw"; \
	done

$(APISURFACE): tools/apisurface/main.go tools/apisurface/go.mod
	cd tools/apisurface && go build -o ../../.tools/bin/apisurface .

$(APIDIFF): tools/apidiff/go.mod tools/apidiff/go.sum
	cd tools/apidiff && go build -o ../../.tools/bin/apidiff golang.org/x/exp/cmd/apidiff

$(APIDIFF_NORMALIZE): tools/apidiff/normalize.go tools/apidiff/go.mod tools/apidiff/go.sum
	cd tools/apidiff && go build -o ../../.tools/bin/apidiff-normalize .

.PHONY: broker-up broker-down broker-reset broker-smoke test-rabbitmq

## test-rabbitmq: run the RabbitMQ driver suite against the fixture, starting it
## first. F1_REQUIRE_RABBITMQ makes an unreachable broker a failure rather than a
## skip, so this target never reports success for a suite that did not run. The
## broker is left running for repeat runs; stop it with broker-down.
test-rabbitmq: broker-up broker-smoke
	F1_REQUIRE_RABBITMQ=1 go test -race -count=1 ./drivers/rabbitmq/...

## broker-up: start the pinned local RabbitMQ fixture.
broker-up:
	docker compose -f docker/docker-compose.yml up -d

## broker-down: stop the local RabbitMQ fixture and keep its named volume.
broker-down:
	docker compose -f docker/docker-compose.yml down

## broker-reset: stop the fixture and remove its named volume.
broker-reset:
	docker compose -f docker/docker-compose.yml down -v

## broker-smoke: wait for RabbitMQ health and print its version.
broker-smoke:
	@set -e; \
	for attempt in $$(seq 1 60); do \
		health=$$(docker compose -f docker/docker-compose.yml ps --format '{{.Health}}' rabbitmq 2>/dev/null || true); \
		if [ "$$health" = "healthy" ]; then \
			docker compose -f docker/docker-compose.yml exec -T rabbitmq rabbitmq-diagnostics -q status | grep 'RabbitMQ version'; \
			exit 0; \
		fi; \
		sleep 1; \
	done; \
	echo "broker-smoke: RabbitMQ did not become healthy" >&2; \
	docker compose -f docker/docker-compose.yml ps; \
	exit 1
