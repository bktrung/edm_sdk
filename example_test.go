package f1_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"time"

	f1 "fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk"
	//nolint:depguard // the example runs the client on the in-memory driver, which needs no broker.
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/drivers/inmem"
)

func Example() {
	ctx := context.Background()
	client, err := f1.New(ctx, f1.Config{
		Env:     "example",
		Service: "godoc",
		Broker:  f1.BrokerConfig{Driver: "inmem"},
		Topology: f1.TopologyConfig{
			AutoCreate: true,
		},
	},
		f1.WithDriver(inmem.New()),
		f1.WithPublishTopics("events.created"),
		f1.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
	)
	if err != nil {
		panic(err)
	}

	handled := make(chan struct{})
	runner, err := client.Subscribe(ctx, f1.Subscription{
		Name:           "example-consumer",
		Topics:         []string{"events.created"},
		Concurrency:    1,
		Prefetch:       1,
		Priorities:     []f1.Priority{f1.PriorityMedium},
		Retry:          f1.RetryConfig{MaxAttempts: 1},
		HandlerTimeout: time.Second,
		Handlers: map[string]f1.Handler{
			"events.created.v1": f1.HandlerFunc(func(_ context.Context, event *f1.Event) error {
				fmt.Println(string(event.Raw()))
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
	if _, err := client.Publisher().Publish(ctx, "events.created.v1", struct {
		Message string `json:"message"`
	}{Message: "hello"}); err != nil {
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

	closeCtx, cancelClose := context.WithTimeout(context.Background(), time.Second)
	defer cancelClose()
	if err := client.Close(closeCtx); err != nil {
		panic(err)
	}

	// Output: {"message":"hello"}
}
