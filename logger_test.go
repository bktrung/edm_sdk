package f1

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

type logSink struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

var _ io.Writer = (*logSink)(nil)

func (s *logSink) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buffer.Write(p)
}

func (s *logSink) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buffer.String()
}

func captureProcessDefault(t *testing.T) *logSink {
	t.Helper()
	previous := slog.Default()
	var output logSink
	slog.SetDefault(slog.New(slog.NewTextHandler(&output, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return &output
}

func TestLogSinkIsSafeForConcurrentUse(t *testing.T) {
	var sink logSink
	const (
		writerCount     = 8
		writesPerWriter = 2000
	)

	start := make(chan struct{})
	writersDone := make(chan struct{})
	readerDone := make(chan struct{})
	var writers sync.WaitGroup
	writers.Add(writerCount)
	for range writerCount {
		go func() {
			defer writers.Done()
			<-start
			for range writesPerWriter {
				_, _ = sink.Write([]byte("log\n"))
				runtime.Gosched()
			}
		}()
	}
	go func() {
		defer close(readerDone)
		<-start
		for {
			select {
			case <-writersDone:
				return
			default:
				_ = sink.String()
				runtime.Gosched()
			}
		}
	}()
	close(start)
	go func() {
		writers.Wait()
		close(writersDone)
	}()
	<-readerDone
	if got, want := strings.Count(sink.String(), "log\n"), writerCount*writesPerWriter; got != want {
		t.Fatalf("sink line count = %d, want %d", got, want)
	}
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
	var configuredOutput logSink
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
	var configuredOutput logSink
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

type retiredCloseProducer struct {
	err error
}

func (*retiredCloseProducer) Publish(context.Context, ...driver.OutboundMessage) error { return nil }
func (p *retiredCloseProducer) Close(context.Context) error                            { return p.err }

func TestRetiredCloseFailuresUseLastResortLogger(t *testing.T) {
	cases := []struct {
		name             string
		configuredLogger bool
	}{
		{name: "without logger"},
		{name: "with logger", configuredLogger: true},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			defaultOutput := captureProcessDefault(t)
			var configuredOutput logSink
			options := []Option{WithDriver(&testDriver{conn: &testConn{}})}
			if testCase.configuredLogger {
				options = append(options, WithLogger(slog.New(slog.NewTextHandler(&configuredOutput, nil))))
			}
			client, err := New(context.Background(), testClientConfig(t), options...)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = client.Close(context.Background()) })
			client.mu.Lock()
			// The retirement the supervisor performs runs after the swap: the
			// epoch the old connection was installed under is no longer the
			// client's.
			retiredEpoch := client.current.epoch
			client.current.epoch++
			client.mu.Unlock()
			client.retireConnection(
				context.Background(),
				&retiredCloseProducer{err: errors.New("retired producer close failed")},
				&testConn{closeErr: errors.New("retired connection close failed")},
				retiredEpoch,
			)
			if testCase.configuredLogger {
				if got := configuredOutput.String(); !strings.Contains(got, "retired producer close failed") || !strings.Contains(got, "retired connection close failed") {
					t.Fatalf("configured output = %q, want both retired close failures", got)
				}
				if got := defaultOutput.String(); got != "" {
					t.Fatalf("process default output = %q, want empty", got)
				}
				return
			}
			if got := defaultOutput.String(); !strings.Contains(got, "retired producer close failed") || !strings.Contains(got, "retired connection close failed") {
				t.Fatalf("process default output = %q, want both retired close failures", got)
			}
		})
	}
}
