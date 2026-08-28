# Agent rules

F1 is a Go SDK that puts a broker-agnostic port between a service and its message transport.
For what it is and how it is laid out, read `README.md`, `ARCHITECTURE.md` and
`docs/reading-guide.md`. This file is rules only.

## Gates

Run these before handing work back. The Makefile owns all of them.

- `make build`, `make test-fast`, `make test`, `make lint`
- `make verify-agnostic`, `make verify-self-contained`
- `make check-fixture`, `make check-api-surface`, `make check-api-diff`
- `make test-rabbitmq` needs Docker and a running broker

CI is GitLab on `golang:1.26.1` and runs `build`, `test-fast`, `test`, `lint` and
`verify-agnostic`. The rest are local only, so run them yourself. Changes arrive as merge
requests.

## Three things that will otherwise cost a cycle

**No broker named in business-facing code.** `verify-agnostic` is depguard. Broker clients and
`drivers/` may be imported only under `driver/`, `drivers/`, `f1test/`, `cmd/` and `examples/`.
Everything else names no broker.

**The exported surface is recorded.** `check-api-surface` compares exported symbols against the
`testdata/public-api*.json` fixtures, and `check-api-diff` against a recorded baseline. A
deliberate surface change updates the baseline in the same change and says so. That is never a
reason to skip the gate.

**A new `// TENTATIVE` marker needs an issue link.** The `tentative-check` CI job scans the
merge-request diff and rejects one that carries no `#<n>` or `issues/<n>`.

## Environment

- `make test-rabbitmq` needs a broker, and a broker on a shared machine may belong to someone
  else. **Never run `make broker-reset`** against one you did not start: it is
  `docker compose down -v`, and it destroys volumes.
- The Go toolchain is pinned in `go.mod`. Use it rather than a system Go.

## The frozen contract

Headers prefixed `f1` are a wire contract between services in the estate. Other services decode
messages this SDK encodes, including messages already sitting in queues. Renaming an `f1` header,
removing one, or changing what one means breaks consumers this repository cannot see, and every
gate here will still pass. Adding a new header is safe. Changing an existing one needs a
conversation first.

## Hard rules

- Do not weaken, skip, or delete a test to make a gate pass.
- ASCII only in source, comments, documentation and commit messages.
- Conventional commit prefix, one or two lines, no trailers.
- Do not commit generated artifacts, credentials, or `.env` files.
