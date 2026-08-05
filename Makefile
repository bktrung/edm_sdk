SHELL := /bin/bash
MODULE := fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk

VERSION := $(shell git describe --tags --dirty --always 2>/dev/null || echo dev)

# Pinned here and in .gitlab-ci.yml (SETUP-06), identically - or CI is green
# on a different linter than the one that ran locally.
GOLANGCI_LINT_VERSION := v2.12.2
GOLANGCI_LINT := $(CURDIR)/.tools/bin/golangci-lint

export GOPRIVATE := fgit.zapps.vn

.PHONY: build test-fast test test-chaos kpi lint verify-agnostic swap-report check-fixture check-api-surface

## build: compile everything. CGO disabled, reproducible (-trimpath), version
## stamped via -ldflags -X. No cmd/ exists yet, so there is nothing for the
## stamp to land on - a green build here is not proof the stamp works.
build:
	CGO_ENABLED=0 go build -trimpath -ldflags "-X main.version=$(VERSION)" ./...

## test-fast: unit tests only. No Docker. Must stay under 10s forever
## (doc 15 §8.5) - the moment it does not, that is a P1 against the suite.
## Also proves the coverage profile/report mechanism (doc 11 §2); no
## packages carry a threshold yet, so none is gated here (SETUP-06) -
## each lands its own threshold in the task that creates the package.
test-fast:
	go test -race -short -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out | tail -1

## test: unit + conformance (3 drivers x 2 profiles, M1-15) + integration
## (testcontainers, doc 15 §8). Neither exists yet.
test:
	@echo "test: conformance and integration suites land in M1 (see plan/01-M1-port-and-conformance.md)"

## test-chaos: nightly chaos matrix (doc 11 §4). Not built yet.
test-chaos:
	@echo "test-chaos: chaos matrix lands with M1 lifecycle work (doc 11 §4)"

## kpi: f1bench KPI harness - loss, duplication, fairness, lag (doc 11 §5).
## Not built yet.
kpi:
	@echo "kpi: f1bench harness lands in M1 (doc 11 §5, cmd/f1bench)"

## lint: golangci-lint at a pinned version, config in .golangci.yml (doc 15 §3).
$(GOLANGCI_LINT):
	GOBIN=$(CURDIR)/.tools/bin go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)

lint: $(GOLANGCI_LINT)
	$(GOLANGCI_LINT) run ./...

## verify-agnostic: the mechanical proof of broker-agnosticism (doc 00 §5) -
## the import-boundary rules from doc 02 §1, run standalone so this gate
## does not depend on the rest of the lint set passing.
verify-agnostic: $(GOLANGCI_LINT)
	$(GOLANGCI_LINT) run --enable-only depguard ./...

## swap-report: driver-flip evidence, business packages untouched (doc 12 §7).
## Not built yet.
swap-report:
	@echo "swap-report: lands with the migration work (doc 12 §7, plan/05-M5-migration.md)"

## check-fixture: testdata/envelope-attributes.json and testdata/public-api.json
## must both stay byte-identical to mq-sdk-docs' generated copies (F-P45 hole
## 3, F-P46's BLK-10) - `make check-api`'s own `git diff --exit-code` there
## only guards that repo's files, so nothing before this caught either drifting
## apart. One target, not two: a second forgettable target is how the fixture
## nearly drifted the first time. Skips, rather than failing, when mq-sdk-docs
## is not checked out alongside this repo - that layout is a convenience for
## local dev, not a CI guarantee this target should assume.
check-fixture:
	@if [ -d ../mq-sdk-docs/docs/generated ]; then \
		diff -u ../mq-sdk-docs/docs/generated/envelope-attributes.json testdata/envelope-attributes.json && \
		diff -u ../mq-sdk-docs/docs/generated/public-api.json testdata/public-api.json && \
		echo "check-fixture: testdata/*.json match mq-sdk-docs"; \
	else \
		echo "check-fixture: skipped - ../mq-sdk-docs not checked out alongside this repo"; \
	fi

## check-api-surface: F-P46/BLK-10, the code-to-doc direction `make check-api`
## itself cannot run, since it never reads this repo's code. Asserts package
## f1's actual exported symbols are a SUBSET of testdata/public-api.json - not
## equality, since most of that fixture is for later milestones and "declared
## but not yet implemented" is expected before freeze. go doc granularity only
## (types, funcs, methods, consts, vars) - struct fields are not covered here;
## envelope-attributes.json already covers the wire attributes at that level.
check-api-surface:
	cd tools/apisurface && go build -o ../../.tools/bin/apisurface .
	./.tools/bin/apisurface -fixture testdata/public-api.json
