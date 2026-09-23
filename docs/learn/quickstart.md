# Quickstart

F1 ships an in-memory driver that runs the whole publish and consume flow with no broker, container,
port, or credentials. From the repository root, create `inmem-first-run/main.go` with this program:

```go
package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"time"

	f1 "fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/drivers/inmem"
)

type Greeting struct {
	Text string `json:"text"`
}

func main() {
	ctx := context.Background()
	client, err := f1.New(ctx, f1.Config{
		Env:     "example",
		Service: "in-memory-first-run",
		Broker:  f1.BrokerConfig{Driver: "inmem"},
		Topology: f1.TopologyConfig{
			AutoCreate: true,
		},
	},
		f1.WithDriver(inmem.New()),
		f1.WithPublishTopics("greetings.created"),
		f1.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
	)
	if err != nil {
		panic(err)
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := client.Close(closeCtx); err != nil {
			panic(err)
		}
	}()

	handled := make(chan struct{})
	runner, err := client.Subscribe(ctx, f1.Subscription{
		Name:           "greeter",
		Topics:         []string{"greetings.created"},
		Concurrency:    1,
		Prefetch:       1,
		Priorities:     []f1.Priority{f1.PriorityMedium},
		Retry:          f1.RetryConfig{MaxAttempts: 1},
		HandlerTimeout: time.Second,
		Handlers: map[string]f1.Handler{
			"greetings.created.v1": f1.HandlerFunc(func(_ context.Context, event *f1.Event) error {
				var greeting Greeting
				if err := event.Decode(&greeting); err != nil {
					return err
				}
				fmt.Printf("received: %s\n", greeting.Text)
				close(handled)
				return nil
			}),
		},
	})
	if err != nil {
		panic(err)
	}

	runDone := make(chan error, 1)
	go func() { runDone <- runner.Run(ctx) }()
	if _, err := client.Publisher().Publish(ctx, "greetings.created.v1", Greeting{Text: "hello from in-memory driver"}); err != nil {
		panic(err)
	}

	<-handled
	drainCtx, cancelDrain := context.WithTimeout(context.Background(), time.Second)
	defer cancelDrain()
	if err := runner.Drain(drainCtx); err != nil {
		panic(err)
	}
	if err := <-runDone; err != nil {
		panic(err)
	}
}
```

Run it:

```sh
$ go run ./inmem-first-run/main.go
received: hello from in-memory driver
```

## Go further

- [Getting started](/learn/getting-started) - connect a driver in a service and plan shutdown.
- [Message](/basics/message) - learn how payloads, envelopes, and events relate.
- [Publisher and subscriber](/basics/pubsub) - move from the in-memory run to subscriptions.
