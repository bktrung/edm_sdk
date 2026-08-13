package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	f1 "fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/drivers/rabbitmq"
)

const publisherTopic = "quickstart.message"

type publisherPayload struct {
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
	waitForConsumer := flag.Bool("wait", false, "wait for Enter before publishing")
	flag.Parse()

	ctx, stop := signalContext()
	defer stop()
	cfg, err := f1.LoadConfig(*configPath)
	if err != nil {
		return err
	}
	client, err := f1.New(ctx, cfg,
		// The driver is selected in main so the service code only uses the SDK API.
		f1.WithDriver(rabbitmq.Driver{}),
		f1.WithPublishTopics(publisherTopic),
	)
	if err != nil {
		return err
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = client.Close(closeCtx)
	}()

	if *waitForConsumer {
		fmt.Println("publisher ready; press Enter after the consumer is ready")
		if err := waitForEnter(ctx); err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return fmt.Errorf("wait for publish: %w", err)
		}
	}
	if err := ctx.Err(); err != nil {
		return nil
	}
	id, err := client.Publisher().Publish(ctx, "quickstart.message.v1", publisherPayload{Message: "hello from publisher"})
	if err != nil {
		return fmt.Errorf("publish: %w", err)
	}
	fmt.Printf("published message id=%s\n", id)
	return nil
}

func signalContext() (context.Context, context.CancelFunc) {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	return ctx, cancel
}

func waitForEnter(ctx context.Context) error {
	result := make(chan error, 1)
	go func() {
		_, err := bufio.NewReader(os.Stdin).ReadString('\n')
		if errors.Is(err, io.EOF) {
			err = nil
		}
		result <- err
	}()
	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
