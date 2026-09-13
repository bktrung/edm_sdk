SHELL := /bin/bash
MODULE := fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk

VERSION := $(shell git describe --tags --dirty --always 2>/dev/null || echo dev)

# Keep the linter version aligned with CI.
GOLANGCI_LINT_VERSION := v2.12.2
GOLANGCI_LINT := $(CURDIR)/.tools/bin/golangci-lint
export GOLANGCI_LINT_CACHE := $(CURDIR)/.cache/golangci-lint

export GOPRIVATE := fgit.zapps.vn

APISURFACE := $(CURDIR)/.tools/bin/apisurface
APIDIFF := $(CURDIR)/.tools/bin/apidiff
APIDIFF_NORMALIZE := $(CURDIR)/.tools/bin/apidiff-normalize
API_DIFF_BASELINE_DIR := $(CURDIR)/testdata/api-diff
API_DIFF_BREAKING_CHANGE ?= 0
API_DIFF_ENFORCE ?= 1
RABBITMQ_PORT ?= 5672
RABBITMQ_MANAGEMENT_PORT ?= 15672
RABBITMQ_PROJECT ?= docker
RABBITMQ_COMPOSE := RABBITMQ_PORT=$(RABBITMQ_PORT) RABBITMQ_MANAGEMENT_PORT=$(RABBITMQ_MANAGEMENT_PORT) docker compose --project-name "$(RABBITMQ_PROJECT)" -f docker/docker-compose.yml
# Every RabbitMQ test target needs the same endpoint and the same guarantee that
# an unreachable broker fails rather than skips. Sharing the prefix keeps a new
# target from being added with one of the two and not the other.
RABBITMQ_TEST_ENV = F1_RABBITMQ_ENDPOINT=$${F1_RABBITMQ_ENDPOINT:-amqp://guest:guest@localhost:$(RABBITMQ_PORT)/} F1_REQUIRE_RABBITMQ=1
KAFKA_PORT ?= 19092
KAFKA_PROJECT ?= docker
KAFKA_COMPOSE := KAFKA_PORT=$(KAFKA_PORT) docker compose --project-name "$(KAFKA_PROJECT)" -f docker/docker-compose.yml

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
	$(GOLANGCI_LINT) run --build-tags integration ./...

## verify-agnostic: check broker import boundaries with depguard.
verify-agnostic: $(GOLANGCI_LINT)
	$(GOLANGCI_LINT) run --build-tags integration --enable-only depguard ./...

## verify-self-contained: fail on citations no reader of this repository can resolve.
# Design records live elsewhere. A reader here - a broker vendor, an auditor, a
# future maintainer - cannot follow a decision or task identifier into a tree
# they do not have, so an unresolvable citation is worse than none. State the
# rule the citation stands for instead.
verify-self-contained:
	@set -o pipefail; \
	citation_pattern='(^|[^[:alnum:]_-])(ADR-[0-9]{4}|F-[0-9]{4}|A-[0-9]{4}|F-P[0-9]+|M1-[0-9]+)($$|[^[:alnum:]_-])'; \
	matches=$$(git grep -nIE "$$citation_pattern" -- . ':!Makefile'); \
	citation_status=$$?; \
	if [ "$$citation_status" -gt 1 ]; then \
		exit "$$citation_status"; \
	fi; \
	if [ "$$citation_status" -eq 0 ]; then \
		printf '%s\n' "$$matches"; \
		echo "verify-self-contained: citation above cannot be resolved from this repository"; \
		exit 1; \
	fi; \
	paths=$$(git ls-files --cached --others --exclude-standard); \
	path_status=$$?; \
	if [ "$$path_status" -ne 0 ]; then \
		exit "$$path_status"; \
	fi; \
	path_matches=$$(printf '%s\n' "$$paths" | grep -nE '(^|/)(plans|journals|reports)/'); \
	path_match_status=$$?; \
	if [ "$$path_match_status" -gt 1 ]; then \
		exit "$$path_match_status"; \
	fi; \
	if [ "$$path_match_status" -eq 0 ]; then \
		printf '%s\n' "$$path_matches"; \
		echo "verify-self-contained: design-artifact path above cannot be resolved from this repository"; \
		exit 1; \
	fi; \
	echo "verify-self-contained: 0 issues."

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

.PHONY: broker-up broker-down broker-reset broker-smoke test-rabbitmq test-rabbitmq-driver test-rabbitmq-conformance test-kafka test-infra test-kafka-conformance kafka-up kafka-down

## test-rabbitmq: run the RabbitMQ driver suite and its conformance suite
## against the selected fixture, starting it first. F1_REQUIRE_RABBITMQ makes an
## unreachable broker a failure rather than a skip, so this target never reports
## success for a suite that did not run. The broker is left running for repeat
## runs; stop it with broker-down.
test-rabbitmq: broker-up broker-smoke
	$(RABBITMQ_TEST_ENV) go test -race -count=1 ./drivers/rabbitmq/...

## test-rabbitmq-driver: run the RabbitMQ driver suite without the conformance
## suite, which test-rabbitmq-conformance runs separately. The conformance suite
## is not behind its own switch the way the Kafka one is, so CI selects the two
## halves with -skip and -run rather than with an environment variable.
test-rabbitmq-driver: broker-up broker-smoke
	$(RABBITMQ_TEST_ENV) go test -race -count=1 -skip '^TestConformance$$' ./drivers/rabbitmq/...

## test-rabbitmq-conformance: run the RabbitMQ conformance suite against the
## fixture. It requires a live RabbitMQ broker with the management API enabled,
## because the subscription path reads bindings and queue arguments through it.
test-rabbitmq-conformance: broker-up broker-smoke
	$(RABBITMQ_TEST_ENV) go test -v -count=1 -run TestConformance -timeout 20m ./drivers/rabbitmq/...

## test-kafka: run the Kafka driver suite against the fixture, starting it first.
## F1_REQUIRE_KAFKA makes an unreachable broker a failure rather than a skip.
test-kafka: kafka-up
	F1_KAFKA_ENDPOINT=$${F1_KAFKA_ENDPOINT:-localhost:$(KAFKA_PORT)} F1_REQUIRE_KAFKA=1 go test -race -count=1 ./drivers/kafka/...

## test-infra: run the tests that need a broker. Fails, never skips, when one is missing.
test-infra: kafka-up
	F1_KAFKA_ENDPOINT=$${F1_KAFKA_ENDPOINT:-localhost:$(KAFKA_PORT)} F1_ACCEPTANCE_ENDPOINT=$${F1_ACCEPTANCE_ENDPOINT:-localhost:$(KAFKA_PORT)} F1_REQUIRE_KAFKA=1 go test -count=1 -tags integration ./...

## test-kafka-conformance: run both Kafka conformance profiles against the fixture.
## This takes about 250s and requires a live Kafka broker. F1_KAFKA_CONFORMANCE
## and F1_REQUIRE_KAFKA make the conformance run explicit and required.
test-kafka-conformance: kafka-up
	F1_KAFKA_ENDPOINT=$${F1_KAFKA_ENDPOINT:-localhost:$(KAFKA_PORT)} F1_KAFKA_CONFORMANCE=1 F1_REQUIRE_KAFKA=1 go test -v -count=1 -run TestConformance -timeout 20m ./drivers/kafka/...

## kafka-up: start the Kafka fixture without starting RabbitMQ. KAFKA_PORT
## defaults to the historical port and KAFKA_PROJECT to the existing compose
## project name, docker.
kafka-up:
	$(KAFKA_COMPOSE) up -d kafka

## kafka-down: stop the Kafka service for the selected project.
kafka-down:
	$(KAFKA_COMPOSE) stop kafka

## broker-up: start only the RabbitMQ service for the selected project.
broker-up:
	$(RABBITMQ_COMPOSE) up -d rabbitmq

## broker-down: stop only the selected RabbitMQ service and keep its named volume.
broker-down:
	$(RABBITMQ_COMPOSE) stop rabbitmq

## broker-reset: stop RabbitMQ and remove only its selected project's volume.
broker-reset:
	@set -e; \
	$(RABBITMQ_COMPOSE) rm -sfv rabbitmq; \
	volumes=$$(docker volume ls -q --filter label=com.docker.compose.project=$(RABBITMQ_PROJECT) --filter label=com.docker.compose.volume=rabbitmq_data); \
	if [ -n "$$volumes" ]; then docker volume rm $$volumes; fi

## broker-smoke: wait for RabbitMQ health and print its version.
broker-smoke:
	@set -e; \
	for attempt in $$(seq 1 60); do \
		health=$$($(RABBITMQ_COMPOSE) ps --format '{{.Health}}' rabbitmq 2>/dev/null || true); \
		if [ "$$health" = "healthy" ]; then \
			$(RABBITMQ_COMPOSE) exec -T rabbitmq rabbitmq-diagnostics -q status | grep 'RabbitMQ version'; \
			exit 0; \
		fi; \
		sleep 1; \
	done; \
	echo "broker-smoke: RabbitMQ did not become healthy" >&2; \
	$(RABBITMQ_COMPOSE) ps; \
	exit 1
