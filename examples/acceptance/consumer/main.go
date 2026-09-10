// Package main runs the acceptance consumer service.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	f1 "fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/codec"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/drivers/kafka"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/drivers/rabbitmq"
)

const eventType = "acceptance.message.v1"

type message struct {
	Sequence int    `json:"sequence"`
	Outcome  string `json:"outcome"`
	Text     string `json:"text"`
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	service := envString("F1_ACCEPTANCE_CONSUMER_SERVICE", "acceptance-consumer")
	topic := envString("F1_ACCEPTANCE_TOPIC", "acceptance.message")
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	var handled atomic.Int64
	var retried atomic.Int64
	var deadLettered atomic.Int64

	cfg := serviceConfig(service)
	driverOption, err := configuredDriverOption(cfg)
	if err != nil {
		fmt.Printf("CONSUMER_START_ERROR error=%v\n", err)
		return
	}
	client, err := f1.New(ctx, cfg,
		driverOption,
		f1.WithCodec(codec.JSON{}),
		f1.WithTopology(f1.TopologyDeclare),
		f1.WithLogger(logger),
	)
	if err != nil {
		fmt.Printf("CONSUMER_START_ERROR error=%v\n", err)
		return
	}
	limits := client.Limits()
	logger.Info("consumer limits", "driver", limits.Driver, "broker", limits.Broker, "features", limits.Features)

	runner, err := client.Subscribe(ctx, f1.Subscription{
		Name:            envString("F1_ACCEPTANCE_SUBSCRIPTION", "acceptance-consumer"),
		Topics:          []string{topic},
		Concurrency:     1,
		Prefetch:        16,
		Priorities:      []f1.Priority{f1.PriorityHigh, f1.PriorityNormal, f1.PriorityLow},
		Retry:           f1.RetryConfig{MaxAttempts: 2, Tiers: []time.Duration{500 * time.Millisecond}},
		HandlerTimeout:  2 * time.Second,
		UnmatchedPolicy: f1.DeadLetter,
		OnDeadLetter: func(_ context.Context, dead f1.DeadLettered) {
			deadLettered.Add(1)
			fmt.Printf("DEAD_LETTER id=%s reason=%s attempt=%d error=%v\n", dead.Envelope.ID, dead.Reason, dead.Attempt, dead.LastErr)
		},
		Handlers: map[string]f1.Handler{
			eventType: f1.HandlerFunc(func(_ context.Context, event *f1.Event) error {
				var payload message
				if decodeErr := event.Decode(&payload); decodeErr != nil {
					return f1.Terminal(decodeErr)
				}
				switch payload.Outcome {
				case "retry":
					if event.Attempt() == 1 {
						fmt.Printf("RETRY id=%s sequence=%d attempt=%d\n", event.ID(), payload.Sequence, event.Attempt())
						return errors.New("planned transient failure")
					}
					retried.Add(1)
				case "dead":
					return f1.Terminal(errors.New("planned permanent failure"))
				}
				handled.Add(1)
				fmt.Printf("HANDLED id=%s sequence=%d attempt=%d\n", event.ID(), payload.Sequence, event.Attempt())
				return nil
			}),
		},
	})
	if err != nil {
		fmt.Printf("CONSUMER_SUBSCRIBE_ERROR error=%v\n", err)
		_ = client.Close(context.Background())
		return
	}

	runErr := make(chan error, 1)
	go func() { runErr <- runner.Run(ctx) }()
	<-ctx.Done()

	closeCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	closeErr := client.Close(closeCtx)
	if err := <-runErr; err != nil && !errors.Is(err, context.Canceled) {
		fmt.Printf("CONSUMER_RUN_ERROR error=%v\n", err)
	}
	fmt.Printf("CONSUMER_SUMMARY handled=%d retried=%d dead_lettered=%d\n", handled.Load(), retried.Load(), deadLettered.Load())
	fmt.Printf("CONSUMER_CLOSE error=%v\n", closeErr)
}

func serviceConfig(service string) f1.Config {
	driverName := envString("F1_ACCEPTANCE_DRIVER", "rabbitmq")
	return f1.Config{
		Env:     envString("F1_ACCEPTANCE_ENV", "acceptance"),
		Service: service,
		Broker: f1.BrokerConfig{
			Driver:          driverName,
			Endpoints:       []string{envString("F1_ACCEPTANCE_ENDPOINT", defaultEndpoint(driverName))},
			ConnectTimeout:  5 * time.Second,
			DefaultPrefetch: 16,
		},
		Topology: f1.TopologyConfig{
			AutoCreate: true,
			Priorities: []f1.Priority{f1.PriorityHigh, f1.PriorityNormal, f1.PriorityLow},
		},
		Codec: f1.CodecConfig{
			Default:        "json",
			ContentMode:    "binary",
			MaxHeaderBytes: 8192,
			MaxBodyBytes:   1 << 20,
		},
		Lifecycle: f1.LifecycleConfig{
			DrainTimeout: 10 * time.Second,
			HandlerGrace: 1 * time.Second,
			FlushTimeout: 5 * time.Second,
			CloseTimeout: 5 * time.Second,
		},
	}
}

func configuredDriverOption(cfg f1.Config) (f1.Option, error) {
	switch cfg.Broker.Driver {
	case "kafka":
		return f1.WithDriver(kafka.Driver{}), nil
	case "rabbitmq":
		return f1.WithDriver(rabbitmq.Driver{}), nil
	default:
		return nil, fmt.Errorf("unsupported acceptance driver %q", cfg.Broker.Driver)
	}
}

func defaultEndpoint(driverName string) string {
	if driverName == "kafka" {
		return "localhost:19092"
	}
	return "amqp://guest:guest@localhost:5672/"
}

func envString(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
