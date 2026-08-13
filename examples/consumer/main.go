// Package main runs the RabbitMQ consumer quickstart service.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	f1 "fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/drivers/rabbitmq"
)

const subscriptionName = "quickstart"

type consumerPayload struct {
	Message string `json:"message"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	configPath := flag.String("config", "examples/config.yaml", "path to the F1 YAML configuration")
	flag.Parse()

	ctx := context.Background()
	cfg, err := f1.LoadConfig(*configPath)
	if err != nil {
		return err
	}
	client, err := f1.New(ctx, cfg,
		// The driver is selected in main so the service code only uses the SDK API.
		f1.WithDriver(rabbitmq.Driver{}),
	)
	if err != nil {
		return err
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = client.Close(closeCtx)
	}()

	runner, err := client.Subscribe(ctx, f1.Subscription{
		Name: subscriptionName,
		Handlers: map[string]f1.Handler{
			"quickstart.message.v1": f1.HandlerFunc(handleMessage),
			"quickstart.audit.v1":   f1.HandlerFunc(handleAudit),
		},
		OnDeadLetter: func(_ context.Context, message f1.DeadLettered) {
			fmt.Printf("dead-lettered event type=%s reason=%s error=%v\n", message.Envelope.Type, message.Reason, message.LastErr)
		},
	})
	if err != nil {
		return err
	}

	runDone := make(chan error, 1)
	go func() { runDone <- runner.Run(ctx) }()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)
	fmt.Println("consumer ready; waiting for messages")
	select {
	case err := <-runDone:
		return err
	case <-signals:
		drainCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if err := runner.Drain(drainCtx); err != nil {
			return fmt.Errorf("drain: %w", err)
		}
		fmt.Println("consumer drained cleanly")
		if err := <-runDone; err != nil {
			return err
		}
		return nil
	}
}

func handleMessage(_ context.Context, event *f1.Event) error {
	var payload consumerPayload
	if err := event.Decode(&payload); err != nil {
		return f1.Terminal(fmt.Errorf("decode message payload: %w", err))
	}
	if payload.Message == "" {
		return f1.Terminal(errors.New("message field is empty"))
	}
	fmt.Printf("handled message: %s\n", payload.Message)
	return nil
}

func handleAudit(_ context.Context, event *f1.Event) error {
	var payload consumerPayload
	if err := event.Decode(&payload); err != nil {
		return f1.Terminal(fmt.Errorf("decode audit payload: %w", err))
	}
	if payload.Message == "" {
		return f1.Terminal(errors.New("audit message field is empty"))
	}
	fmt.Printf("handled audit: %s\n", payload.Message)
	return nil
}
