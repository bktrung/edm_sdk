# Contributing to F1

## Start here

F1 is a broker-agnostic event-driven messaging SDK. Read [README.md](README.md), then [ARCHITECTURE.md](ARCHITECTURE.md), then [the source-reading guide](docs/development/source-reading-guide.md), and finally [AGENTS.md](AGENTS.md).

## Before you open a merge request

Run this minimum local gate set before you open a merge request:

```sh
make build
go vet ./...
gofmt -l .
make format-check
make lint
make vulncheck
make verify-agnostic
make verify-self-contained
make check-doc-source-links
make check-observer-events
make check-api-surface
make check-api-surface-f1otel
make check-api-diff
make otlp-boundary
make test-fast
```

No CI runner executes the pipeline, so these local runs are the evidence a reviewer will ask for. See [Testing strategy](docs/development/testing.md) for broker-backed runs and port variables.

## Public API changes

Read [API compatibility](docs/development/api-compatibility.md) before changing a public API. Run `make check-api-diff`; record compatible additions with `make record-api-diff-baseline`. A breaking change needs explicit approval recorded in the commit that makes it.

## Changelog

User-visible changes add a bullet under an `## [Unreleased]` section at the top of [CHANGELOG.md](CHANGELOG.md), creating the section if it is absent, in Keep a Changelog form.

## Commits

Follow the [commit style in AGENTS.md](AGENTS.md#commit-style).
