# Architecture

This root-level file is a stable entrypoint for repository and tooling links.
The canonical architecture map is
[`docs/development/architecture.md`](docs/development/architecture.md).

Use the canonical development route for current package ownership, dependency
boundaries, runtime responsibilities, and invariants:

- [Architecture map](docs/development/architecture.md)
- [Publish flow](docs/development/publish-flow.md)
- [Consume flow](docs/development/consume-flow.md)
- [Driver contract](docs/development/driver-contract.md)
- [Driver conformance](docs/development/driver-conformance.md)
- [Source-reading guide](docs/development/source-reading-guide.md)

The source and tests remain authoritative for implementation behavior. Start at
the architecture map, then follow the source links for the boundary you are
changing.
