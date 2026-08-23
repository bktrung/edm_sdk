// Package main runs the acceptance publisher service.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	f1 "fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/codec"
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

	service := envString("F1_ACCEPTANCE_PUBLISHER_SERVICE", "acceptance-publisher")
	topic := envString("F1_ACCEPTANCE_TOPIC", "acceptance.message")
	count := envInt("F1_ACCEPTANCE_COUNT", 10)
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	client, err := f1.New(ctx, serviceConfig(service),
		f1.WithDriver(rabbitmq.Driver{}),
		f1.WithCodec(codec.JSON{}),
		f1.WithTopology(f1.TopologyDeclare),
		f1.WithPublishTopics(topic),
		f1.WithLogger(logger),
	)
	if err != nil {
		fmt.Printf("PUBLISHER_START_ERROR error=%v\n", err)
		return
	}

	published := 0
	for sequence := 1; sequence <= count; sequence++ {
		if ctx.Err() != nil {
			break
		}

		outcome := "handled"
		switch sequence {
		case 4:
			outcome = "retry"
		case 5:
			outcome = "dead"
		}
		payload := message{Sequence: sequence, Outcome: outcome, Text: fmt.Sprintf("acceptance-%03d", sequence)}
		id, publishErr := client.Publisher().Publish(ctx, eventType, payload,
			f1.WithTopic(topic),
			f1.WithSubject(payload.Text),
			f1.WithKey(payload.Text),
			f1.WithIdempotencyKey(payload.Text),
		)
		if publishErr != nil {
			fmt.Printf("PUBLISH_ERROR sequence=%d error=%v\n", sequence, publishErr)
		} else {
			published++
			fmt.Printf("PUBLISH_OK sequence=%d id=%s\n", sequence, id)
		}
	}

	fmt.Printf("PUBLISH_SUMMARY published=%d requested=%d\n", published, count)
	<-ctx.Done()

	closeCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	closeErr := client.Close(closeCtx)
	fmt.Printf("PUBLISHER_CLOSE error=%v\n", closeErr)
}

func serviceConfig(service string) f1.Config {
	return f1.Config{
		Env:     envString("F1_ACCEPTANCE_ENV", "acceptance"),
		Service: service,
		Broker: f1.BrokerConfig{
			Driver:          "rabbitmq",
			Endpoints:       []string{envString("F1_ACCEPTANCE_ENDPOINT", "amqp://guest:guest@localhost:5672/")},
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

func envString(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func envInt(key string, fallback int) int {
	value, err := strconv.Atoi(envString(key, strconv.Itoa(fallback)))
	if err != nil || value < 1 {
		return fallback
	}
	return value
}
