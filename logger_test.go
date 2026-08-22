package f1

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func captureProcessDefault(t *testing.T) *bytes.Buffer {
	t.Helper()
	previous := slog.Default()
	var output bytes.Buffer
	slog.SetDefault(slog.New(slog.NewTextHandler(&output, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return &output
}

func newLoggerConsumerRunner(t *testing.T, logger *slog.Logger, handler func(context.Context, *Event, error)) (*Client, *Runner, *dispatchConsumer) {
	t.Helper()
	consumer := newDispatchConsumer()
	options := []Option{
		WithDriver(&dispatchDriver{conn: &dispatchConn{consumer: consumer, producer: &dispatchProducer{}, admin: &dispatchAdmin{}}}),
	}
	if logger != nil {
		options = append(options, WithLogger(logger))
	}
	if handler != nil {
		options = append(options, WithErrorHandler(handler))
	}
	client, err := New(context.Background(), testClientConfig(t), options...)
	if err != nil {
		t.Fatal(err)
	}
	runner := &Runner{client: client, subscription: Subscription{Name: "orders"}, consumer: consumer}
	return client, runner, consumer
}

func triggerLoggerConsumerError(t *testing.T, runner *Runner, consumer *dispatchConsumer) {
	t.Helper()
	consumer.errs <- errors.New("connection lost")
	if err := consumeRunnerErrors(runner, context.Background()); err != nil {
		t.Fatalf("consumeRunnerErrors() error = %v", err)
	}
}

func TestNoLoggerLeavesCapabilityInfoOffProcessDefault(t *testing.T) {
	defaultOutput := captureProcessDefault(t)
	client, err := New(context.Background(), testClientConfig(t), WithDriver(&testDriver{conn: &testConn{}}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	if got := defaultOutput.String(); got != "" {
		t.Fatalf("process default capability output = %q, want empty", got)
	}
}

func TestWithLoggerReceivesCapabilityOutputWithoutProcessDefault(t *testing.T) {
	defaultOutput := captureProcessDefault(t)
	var configuredOutput bytes.Buffer
	client, err := New(context.Background(), testClientConfig(t),
		WithDriver(&testDriver{conn: &testConn{}}),
		WithLogger(slog.New(slog.NewTextHandler(&configuredOutput, nil))),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	if got := configuredOutput.String(); !strings.Contains(got, "f1 capability unavailable") {
		t.Fatalf("configured logger output = %q, want capability message", got)
	}
	if got := defaultOutput.String(); got != "" {
		t.Fatalf("process default output = %q, want empty", got)
	}
}

func TestNoLoggerConsumerErrorUsesProcessDefault(t *testing.T) {
	defaultOutput := captureProcessDefault(t)
	client, runner, consumer := newLoggerConsumerRunner(t, nil, nil)
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	triggerLoggerConsumerError(t, runner, consumer)
	if got := defaultOutput.String(); !strings.Contains(got, "f1 consumer error") {
		t.Fatalf("process default output = %q, want consumer error", got)
	}
}

func TestWithLoggerConsumerErrorUsesConfiguredLogger(t *testing.T) {
	defaultOutput := captureProcessDefault(t)
	var configuredOutput bytes.Buffer
	client, runner, consumer := newLoggerConsumerRunner(t, slog.New(slog.NewTextHandler(&configuredOutput, nil)), nil)
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	triggerLoggerConsumerError(t, runner, consumer)
	if got := configuredOutput.String(); !strings.Contains(got, "f1 consumer error") {
		t.Fatalf("configured logger output = %q, want consumer error", got)
	}
	if got := defaultOutput.String(); got != "" {
		t.Fatalf("process default output = %q, want empty", got)
	}
}

func TestWithErrorHandlerConsumerErrorUsesHandlerNotProcessDefault(t *testing.T) {
	defaultOutput := captureProcessDefault(t)
	recorder := newErrorHandlerRecorder()
	client, runner, consumer := newLoggerConsumerRunner(t, nil, recorder.handle)
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	triggerLoggerConsumerError(t, runner, consumer)
	recorder.waitForCall(t, time.Second)
	if got := defaultOutput.String(); got != "" {
		t.Fatalf("process default output = %q, want empty with an error handler", got)
	}
}

func newLoggerHandoffRunner(t *testing.T, logger *slog.Logger, handler func(context.Context, *Event, error)) (*Client, *Runner) {
	t.Helper()
	options := []Option{WithDriver(&testDriver{conn: &testConn{}})}
	if logger != nil {
		options = append(options, WithLogger(logger))
	}
	if handler != nil {
		options = append(options, WithErrorHandler(handler))
	}
	client, err := New(context.Background(), testClientConfig(t), options...)
	if err != nil {
		t.Fatal(err)
	}
	runner := &Runner{client: client, subscription: Subscription{Name: "orders"}}
	return client, runner
}

func TestSuccessorHandoffReleaseLogsWithoutErrorHandler(t *testing.T) {
	defaultOutput := captureProcessDefault(t)
	consumer := newDispatchConsumer()
	client, runner := newLoggerHandoffRunner(t, nil, nil)
	runner.consumer = consumer
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	failSuccessorHandoff(runner, context.Background(), "retry", nil, errors.New("temporary"))
	if got := defaultOutput.String(); !strings.Contains(got, "consumer released") {
		t.Fatalf("process default output = %q, want release message", got)
	}
}

func TestSuccessorHandoffReleaseFailureLogsWithoutErrorHandler(t *testing.T) {
	defaultOutput := captureProcessDefault(t)
	consumer := &unsupportedReleaseConsumer{}
	client, runner := newLoggerHandoffRunner(t, nil, nil)
	runner.consumer = consumer
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	failSuccessorHandoff(runner, context.Background(), "retry", nil, errors.New("temporary"))
	if got := defaultOutput.String(); !strings.Contains(got, "failed to release consumer") {
		t.Fatalf("process default output = %q, want release failure", got)
	}
}

func TestSuccessorHandoffWithErrorHandlerUsesHandlerNotProcessDefault(t *testing.T) {
	defaultOutput := captureProcessDefault(t)
	recorder := newErrorHandlerRecorder()
	consumer := newDispatchConsumer()
	client, runner := newLoggerHandoffRunner(t, nil, recorder.handle)
	runner.consumer = consumer
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	failSuccessorHandoff(runner, context.Background(), "retry", nil, errors.New("temporary"))
	recorder.waitForCall(t, time.Second)
	if got := defaultOutput.String(); got != "" {
		t.Fatalf("process default output = %q, want empty with an error handler", got)
	}
}
