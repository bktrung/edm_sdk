# Contributing

Start with `README.md` for what F1 is, `ARCHITECTURE.md` for the boundaries, and
`docs/reading-guide.md` for the order to read the code in.

`AGENTS.md` holds the rules that apply to every change, including the wire contract that no gate
can protect. Read it before your first commit, whether or not you work with a coding agent.

## Set up

The Go toolchain is pinned in `go.mod`; use it rather than a system Go. Then:

```sh
make build
make test-fast
```

`make test-fast` runs the short race-enabled suite and prints total coverage. Nothing beyond the
Go toolchain is needed for it.

## Working with a broker

The RabbitMQ suite needs the local fixture:

```sh
make broker-up
make test-rabbitmq
make broker-down
```

`make broker-down` keeps the named volume. `make broker-reset` does not: it is
`docker compose down -v` and it destroys the data. On a shared machine the broker you find
running may belong to someone else, so never reset one you did not start yourself.

## Before you open a merge request

CI is GitLab and runs `build`, `test-fast`, `test`, `lint` and `verify-agnostic`. Run those
locally first, plus the gates CI does not run:

```sh
make lint
make verify-agnostic
make verify-self-contained
make check-api-surface
```

If a gate fails, fix the change rather than the gate; `AGENTS.md` explains what each one is
protecting and why the API baseline is updated in the same change that moves the surface.

## Commits and merge requests

- Conventional commit prefix, one or two lines, no trailers.
- ASCII only, in source, comments, documentation and commit messages.
- Keep a merge request to one subject. A change that moves the exported surface says so.
- Do not commit generated artifacts, credentials, or `.env` files.

## Reporting something you cannot fix

Open an issue describing the observed behavior, the smallest reproduction you have, and which
driver it involves. If you leave a `// TENTATIVE` marker in the code, link that issue on the same
line: CI rejects a new marker without one.
