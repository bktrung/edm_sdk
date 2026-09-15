// Package main runs the acceptance consumer service.
package main

import (
	"context"
	"encoding/json"
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
	subscription := envString("F1_ACCEPTANCE_SUBSCRIPTION", "acceptance-consumer")
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
	// The slog line is the operator-facing form; the harness reads this one. An
	// encode failure is not fatal: the app keeps consuming, and the harness
	// reports the missing line.
	if encodedLimits, encodeErr := json.Marshal(limitsReportOf(limits)); encodeErr != nil {
		fmt.Printf("LIMITS_ERROR error=%v\n", encodeErr)
	} else {
		fmt.Printf("LIMITS %s\n", encodedLimits)
	}

	runner, err := client.Subscribe(ctx, f1.Subscription{
		Name:            subscription,
		Topics:          []string{topic},
		Concurrency:     1,
		Prefetch:        16,
		Priorities:      []f1.Priority{f1.PriorityHigh, f1.PriorityMedium, f1.PriorityLow},
		Retry:           f1.RetryConfig{MaxAttempts: 2, Tiers: []time.Duration{500 * time.Millisecond}},
		HandlerTimeout:  2 * time.Second,
		UnmatchedPolicy: f1.DeadLetter,
		OnDeadLetter: func(_ context.Context, dead f1.DeadLettered) {
			deadLettered.Add(1)
			var deadBody message
			_ = json.Unmarshal(dead.Body, &deadBody)
			fmt.Printf("DEAD_LETTER id=%s key=%s sequence=%d reason=%s attempt=%d destination=%s error=%v\n",
				dead.Envelope.ID, dead.Envelope.IdempotencyKey, deadBody.Sequence, dead.Reason, dead.Attempt, dead.Destination, dead.LastErr)
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
				fmt.Printf("HANDLED id=%s key=%s sequence=%d attempt=%d destination=%s\n",
					event.ID(), event.IdempotencyKey(), payload.Sequence, event.Attempt(), topic)
				return nil
			}),
		},
	})
	if err != nil {
		fmt.Printf("CONSUMER_SUBSCRIBE_ERROR error=%v\n", err)
		_ = client.Close(context.Background())
		return
	}

	// The driver-flip harness reads this line and then waits for the subscription
	// itself: this prints when Subscribe returns, and the destinations are
	// created later, at the top of the runner's loop. A publish that arrives in
	// between is refused with ErrDestinationMissing on a broker that fans out at
	// publish time rather than held for the subscriber that is about to appear.
	fmt.Printf("CONSUMER_READY driver=%s topic=%s subscription=%s\n", cfg.Broker.Driver, topic, subscription)

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
			Priorities: []f1.Priority{f1.PriorityHigh, f1.PriorityMedium, f1.PriorityLow},
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

// limitsReport is the machine-readable form of Client.Limits() that the
// driver-flip harness compares between two driver runs. The slog line above it
// stays because it is the operator-facing form.
type limitsReport struct {
	Driver   string          `json:"driver"`
	Broker   string          `json:"broker"`
	Features []featureReport `json:"features"`
}

type featureReport struct {
	Feature string `json:"feature"`
	Mode    string `json:"mode"`
	Detail  string `json:"detail,omitempty"`
}

func limitsReportOf(limits f1.Limits) limitsReport {
	report := limitsReport{Driver: limits.Driver, Broker: limits.Broker}
	for _, status := range limits.Features {
		report.Features = append(report.Features, featureReport{
			Feature: status.Feature,
			Mode:    featureModeName(status.Mode),
			Detail:  status.Detail,
		})
	}
	return report
}

// featureModeName names a FeatureMode for the machine-readable report. It is
// local because the core's equivalent is unexported.
func featureModeName(mode f1.FeatureMode) string {
	switch mode {
	case f1.FeatureNative:
		return "native"
	case f1.FeatureEmulated:
		return "emulated"
	default:
		return "unavailable"
	}
}
