# Getting started

This route is for a Go service author. It covers the smallest supported setup;
use the [runtime guide](../README.md) and [architecture map](../../ARCHITECTURE.md)
when you need implementation detail.

## Install

F1 is hosted on the repository's private Go module host. Set `GOPRIVATE`
before downloading the module so the Go tool does not query the public proxy or
checksum database:

```sh
go env -w GOPRIVATE=fgit.zapps.vn
go get fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk
```

Use the Go version declared in [`go.mod`](../../go.mod).

## Choose a driver

The application selects the concrete driver at the composition boundary. The
service-facing code uses the root `f1` package; it does not use a broker client
directly.

The repository currently includes:

- `drivers/rabbitmq` for RabbitMQ.
- `drivers/inmem` for deterministic tests and local in-process use.
- `drivers/kafka` as a scaffold for the Kafka adapter; its `Open` method currently returns
  `driver.ErrUnsupported`.

Kafka is also represented in shared configuration and port types, and a local Kafka fixture is
defined in [`docker/docker-compose.yml`](../../docker/docker-compose.yml). It is not a usable runtime
driver yet.

## Load configuration and connect

Start from [`examples/config.yaml`](../../examples/config.yaml), then provide the
service's broker endpoint, topology policy, and subscription settings. Load the
file before constructing the client:

```go
ctx := context.Background()
cfg, err := f1.LoadConfig("config.yaml")
if err != nil {
    return err
}

client, err := f1.New(ctx, cfg,
    f1.WithDriver(rabbitmq.Driver{}),
    f1.WithPublishTopics("orders.created"),
)
if err != nil {
    return err
}
defer client.Close(context.Background())
```

`f1.New` opens the driver eagerly and returns startup errors before publishing
or consuming. `WithPublishTopics` is needed when F1 should ensure publisher
topology; production topology should normally be provisioned separately and
verified rather than auto-created. See [`config.go`](../../config.go) and
[`options.go`](../../options.go) for the owning definitions.

## Run the local example

The root README contains the complete RabbitMQ fixture quickstart. It builds
separate publisher and consumer services from `examples/` so you can see the
composition boundary in a runnable form.

Next:

- [Publish events](publishing-events.md)
- [Consume events](consuming-events.md)
- [Handle failures](handling-failures.md)
