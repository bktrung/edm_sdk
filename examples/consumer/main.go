// Package main runs the RabbitMQ consumer quickstart service.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	f1 "fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/drivers/rabbitmq"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/f1otel"

	//nolint:depguard // application-owned providers are configured in this composition root.
	"go.opentelemetry.io/otel/propagation"
	//nolint:depguard // application-owned providers are configured in this composition root.
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	//nolint:depguard // application-owned providers are configured in this composition root.
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
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

func run() (runErr error) {
	configPath := flag.String("config", "examples/config.yaml", "path to the F1 YAML configuration")
	readyAddr := flag.String("ready-addr", ":8081", "address for the readiness endpoint")
	flag.Parse()

	ctx := context.Background()
	cfg, err := f1.LoadConfig(*configPath)
	if err != nil {
		return err
	}
	// Exporters attach through these application-owned provider options:
	// sdkmetric.WithReader and sdktrace.WithBatcher.
	meterProvider := sdkmetric.NewMeterProvider()
	tracerProvider := sdktrace.NewTracerProvider()
	defer func() {
		providerCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		runErr = errors.Join(runErr, meterProvider.Shutdown(providerCtx), tracerProvider.Shutdown(providerCtx))
		cancel()
	}()

	observer, err := f1otel.New(
		f1otel.WithMeterProvider(meterProvider),
		f1otel.WithTracerProvider(tracerProvider),
		f1otel.WithPropagator(propagation.TraceContext{}),
	)
	if err != nil {
		return err
	}
	client, err := f1.New(ctx, cfg,
		// The driver is selected in main so the service code only uses the SDK API.
		f1.WithDriver(rabbitmq.Driver{}),
		f1.WithObserver(observer),
	)
	if err != nil {
		return err
	}

	// Shutdown budgets are 5s readiness + 15s drain + 5s close + 2s telemetry flush = 27s,
	// below the pod's terminationGracePeriodSeconds (30s by default).
	// Liveness must not depend on broker reachability.
	server := &http.Server{Addr: *readyAddr, Handler: readinessHandler(client), ReadHeaderTimeout: 5 * time.Second}
	serverErr := make(chan error, 1)
	go func() {
		if err := server.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()
	defer func() {
		serverCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = server.Shutdown(serverCtx)
		cancel()
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		runErr = errors.Join(runErr, client.Close(closeCtx))
		cancel()
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
	// Run declares topology and attaches the consumer after this point, so the
	// subscription is starting here, not yet ready to receive.
	fmt.Println("consumer starting; press Ctrl+C to stop")
	select {
	case err := <-runDone:
		return err
	case err := <-serverErr:
		return fmt.Errorf("readiness server: %w", err)
	case <-signals:
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := server.Shutdown(shutdownCtx); err != nil {
			cancel()
			return fmt.Errorf("readiness shutdown: %w", err)
		}
		cancel()
		drainCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
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

func readinessHandler(client *f1.Client) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := client.Health(ctx); err != nil {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
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
