package acceptance_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	f1 "fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/codec"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/drivers/inmem"
)

func TestAcceptanceFlowReconcilesHandledRetryAndDeadLetteredMessages(t *testing.T) {
	const (
		topic     = "acceptance.flow"
		eventType = "acceptance.flow.v1"
		published = 10
	)

	ctx := t.Context()
	client, err := f1.New(ctx, acceptanceFlowConfig(),
		f1.WithDriver(inmem.Driver{}),
		f1.WithCodec(codec.JSON{}),
		f1.WithTopology(f1.TopologyDeclare),
		f1.WithPublishTopics(topic),
		f1.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	closed := false
	t.Cleanup(func() {
		if !closed {
			_ = client.Close(context.Background())
		}
	})

	var handled atomic.Int64
	var retried atomic.Int64
	var deadLettered atomic.Int64
	var outcomesOnce sync.Once
	outcomes := make(chan struct{})
	observeOutcome := func() {
		if handled.Load() == 9 && deadLettered.Load() == 1 {
			outcomesOnce.Do(func() { close(outcomes) })
		}
	}
	deadLetters := make(chan f1.DeadLettered, 1)
	runner, err := client.Subscribe(ctx, f1.Subscription{
		Name:            "acceptance-consumer",
		Topics:          []string{topic},
		Concurrency:     1,
		Prefetch:        2,
		Priorities:      []f1.Priority{f1.PriorityMedium},
		Retry:           f1.RetryConfig{MaxAttempts: 2, Tiers: []time.Duration{10 * time.Millisecond}},
		HandlerTimeout:  time.Second,
		UnmatchedPolicy: f1.DeadLetter,
		OnDeadLetter: func(_ context.Context, dead f1.DeadLettered) {
			deadLettered.Add(1)
			deadLetters <- dead
			observeOutcome()
		},
		Handlers: map[string]f1.Handler{
			eventType: f1.HandlerFunc(func(_ context.Context, event *f1.Event) error {
				var payload acceptanceMessage
				if err := event.Decode(&payload); err != nil {
					return f1.Terminal(err)
				}
				switch payload.Outcome {
				case "retry":
					if event.Attempt() == 1 {
						return errors.New("planned transient failure")
					}
					retried.Add(1)
				case "dead":
					return f1.Terminal(errors.New("planned permanent failure"))
				}
				handled.Add(1)
				observeOutcome()
				return nil
			}),
		},
	})
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- runner.Run(ctx) }()

	for sequence := 1; sequence <= published; sequence++ {
		outcome := "handled"
		switch sequence {
		case 4:
			outcome = "retry"
		case 5:
			outcome = "dead"
		}
		if _, err := client.Publisher().Publish(ctx, eventType, acceptanceMessage{
			Sequence: sequence,
			Outcome:  outcome,
			Text:     fmt.Sprintf("acceptance-%03d", sequence),
		}, f1.WithTopic(topic)); err != nil {
			t.Fatalf("Publish(sequence=%d) error = %v", sequence, err)
		}
	}

	waitCtx, waitCancel := context.WithTimeout(context.Background(), 5*time.Second)
	select {
	case <-outcomes:
		waitCancel()
	case <-waitCtx.Done():
		waitCancel()
		runnerStatus := "running"
		select {
		case runErr := <-runDone:
			runnerStatus = fmt.Sprintf("stopped with %v", runErr)
		default:
		}
		t.Fatalf("acceptance outcomes did not reconcile: handled=%d retried=%d dead-lettered=%d runner=%s: %v", handled.Load(), retried.Load(), deadLettered.Load(), runnerStatus, waitCtx.Err())
	}

	if got := retried.Load(); got != 1 {
		t.Fatalf("retried = %d, want 1", got)
	}
	dead := <-deadLetters
	if dead.Reason != f1.ReasonTerminal {
		t.Fatalf("dead-letter reason = %s, want %s", dead.Reason, f1.ReasonTerminal)
	}
	if got := handled.Load() + deadLettered.Load(); got != published {
		t.Fatalf("final outcomes = %d, want %d", got, published)
	}

	closeErr := client.Close(context.Background())
	closed = true
	if closeErr != nil {
		t.Fatalf("Close() error = %v", closeErr)
	}
	runnerWaitCtx, runnerWaitCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer runnerWaitCancel()
	select {
	case runErr := <-runDone:
		if runErr != nil && !errors.Is(runErr, context.Canceled) {
			t.Fatalf("runner.Run() error = %v", runErr)
		}
	case <-runnerWaitCtx.Done():
		t.Fatal("runner.Run() did not stop after Close")
	}
}

type acceptanceMessage struct {
	Sequence int    `json:"sequence"`
	Outcome  string `json:"outcome"`
	Text     string `json:"text"`
}

func acceptanceFlowConfig() f1.Config {
	return f1.Config{
		Env:     "test",
		Service: "acceptance-flow",
		Broker: f1.BrokerConfig{
			Driver:          "inmem",
			ConnectTimeout:  5 * time.Second,
			DefaultPrefetch: 8,
		},
		Topology: f1.TopologyConfig{
			AutoCreate: true,
			Priorities: []f1.Priority{f1.PriorityMedium},
		},
		Codec: f1.CodecConfig{
			Default:        "json",
			MaxHeaderBytes: f1.CoreMaxHeaderBytes,
			MaxBodyBytes:   1 << 20,
		},
		Lifecycle: f1.LifecycleConfig{
			DrainTimeout:          5 * time.Second,
			HandlerGrace:          time.Second,
			CloseTimeout:          time.Second,
			RebalanceDrainTimeout: time.Second,
		},
	}
}
