package f1

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/lifecycle"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/version"
)

type lifecycleObserver struct {
	mu       sync.Mutex
	starts   []StartEvent
	finishes []FinishEvent
	points   []PointEvent
}

func (o *lifecycleObserver) Start(_ context.Context, event StartEvent) (context.Context, Token) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.starts = append(o.starts, event)
	token := Token{Kind: event.Kind, Start: event.At, Handle: uint64(len(o.starts))}
	return context.Background(), token
}

func (o *lifecycleObserver) Finish(_ Token, event FinishEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.finishes = append(o.finishes, event)
}

func (o *lifecycleObserver) Record(event PointEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.points = append(o.points, event)
}

func (o *lifecycleObserver) snapshot() (starts []StartEvent, finishes []FinishEvent, points []PointEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	starts = append([]StartEvent(nil), o.starts...)
	finishes = append([]FinishEvent(nil), o.finishes...)
	points = append([]PointEvent(nil), o.points...)
	return starts, finishes, points
}

func lifecyclePointsByKind(points []PointEvent, kind ObserverKind) []PointEvent {
	var out []PointEvent
	for _, p := range points {
		if p.Kind == kind {
			out = append(out, p)
		}
	}
	return out
}

func TestObserverDriverSelected(t *testing.T) {
	t.Parallel()
	rec := &lifecycleObserver{}
	cfg := testClientConfig(t)
	cfg.Broker.Endpoints = []string{"amqp://user:pw@h:5672/vh"}
	client, err := New(context.Background(), cfg,
		WithDriver(&testDriver{conn: &testConn{}}),
		WithObserver(rec),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	_, _, points := rec.snapshot()
	selected := lifecyclePointsByKind(points, ObserverDriverSelected)
	if len(selected) != 1 {
		t.Fatalf("driver_selected count = %d, want 1 (points=%v)", len(selected), points)
	}
	got := selected[0]
	if got.DriverName != "test" {
		t.Fatalf("DriverName = %q, want test", got.DriverName)
	}
	if got.ServerAddress != "h" || got.ServerPort != 5672 {
		t.Fatalf("server = %q,%d want h,5672", got.ServerAddress, got.ServerPort)
	}
	if got.SDKVersion != version.SDK() || got.SDKVersion == "" {
		t.Fatalf("SDKVersion = %q, want the SDK version %q", got.SDKVersion, version.SDK())
	}
}

type lifecycleFailAdmin struct{}

func (lifecycleFailAdmin) EnsureTopology(context.Context, driver.TopologySpec) (driver.TopologyDiff, error) {
	return driver.TopologyDiff{}, errors.New("topology boom")
}

func (lifecycleFailAdmin) DescribeTopology(context.Context, []string) (driver.TopologyState, error) {
	return driver.TopologyState{}, driver.ErrUnsupported
}

func (lifecycleFailAdmin) Purge(context.Context, string) (int64, error) {
	return 0, driver.ErrUnsupported
}

func (lifecycleFailAdmin) Prune(context.Context, []string) ([]driver.PruneResult, error) {
	return nil, driver.ErrUnsupported
}

type lifecycleFailConn struct {
	*testConn
	admin driver.Admin
}

func (c *lifecycleFailConn) Admin() driver.Admin { return c.admin }

type lifecycleFailDriver struct {
	conn driver.Conn
}

func (d *lifecycleFailDriver) Name() string { return "test" }

func (d *lifecycleFailDriver) Capabilities() driver.Capabilities { return driver.Capabilities{} }

func (d *lifecycleFailDriver) Open(context.Context, driver.Config) (driver.Conn, error) {
	return d.conn, nil
}

func TestObserverDriverSelectedTopologyErrorRecordsNone(t *testing.T) {
	t.Parallel()
	rec := &lifecycleObserver{}
	cfg := testClientConfig(t)
	cfg.Broker.Endpoints = []string{"amqp://user:pw@h:5672/vh"}
	failConn := &lifecycleFailConn{testConn: &testConn{}, admin: lifecycleFailAdmin{}}
	_, err := New(context.Background(), cfg,
		WithDriver(&lifecycleFailDriver{conn: failConn}),
		WithObserver(rec),
		WithPublishTopics("orders.created"),
	)
	if err == nil {
		t.Fatal("New() = nil, want topology error")
	}
	_, _, points := rec.snapshot()
	if len(points) != 0 {
		t.Fatalf("points after failed New = %d, want 0", len(points))
	}
}

func newLifecycleReconnectClient(t *testing.T, d *reconnectTestDriver, c clock.Clock, maxAttempts int, rec Observer) *Client {
	t.Helper()
	cfg := testClientConfig(t)
	cfg.Broker.Endpoints = []string{"amqp://user:pw@h:5672/vh"}
	cfg.Broker.MaxReconnectAttempts = maxAttempts
	cfg.Lifecycle.DrainTimeout = 500 * time.Millisecond
	cfg.Lifecycle.HandlerGrace = 100 * time.Millisecond
	cfg.Lifecycle.CloseTimeout = 100 * time.Millisecond
	client, err := New(context.Background(), cfg,
		WithDriver(d),
		WithObserver(rec),
		withClock(c),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	return client
}

func TestObserverReconnectLostAndRestored(t *testing.T) {
	t.Parallel()
	fake := clock.NewFake(time.Unix(0, 0))
	recorded := &recordingClock{Fake: fake}
	d := &reconnectTestDriver{created: make(chan *reconnectTestConsumer, 8)}
	rec := &lifecycleObserver{}
	client := newLifecycleReconnectClient(t, d, recorded, 0, rec)
	client.reconnectRandom = func() float64 { return 1 }
	cause := &driver.Error{Driver: d.Name(), Op: "test", K: driver.KindTransient, Err: errors.New("boom")}
	wantClass := errorClassOf(cause)
	if err := client.requestReconnect(cause, 0); err != nil {
		t.Fatal(err)
	}
	advanceReconnect(t, recorded, 500*time.Millisecond, 1)
	if err := client.awaitRebuild(context.Background(), nil, 0, nil); err != nil {
		t.Fatalf("awaitRebuild() = %v, want nil", err)
	}
	waitLifecycleCondition(t, "reconnect events did not arrive", func() bool {
		_, _, points := rec.snapshot()
		return len(lifecyclePointsByKind(points, ObserverConnectionLost)) == 1 && len(lifecyclePointsByKind(points, ObserverConnectionRestored)) == 1
	})
	_, _, points := rec.snapshot()
	lost := lifecyclePointsByKind(points, ObserverConnectionLost)
	restored := lifecyclePointsByKind(points, ObserverConnectionRestored)
	if len(lost) != 1 {
		t.Fatalf("connection_lost count = %d, want 1", len(lost))
	}
	if len(restored) != 1 {
		t.Fatalf("connection_restored count = %d, want 1", len(restored))
	}
	if lost[0].ErrorClass != wantClass {
		t.Fatalf("lost class = %q, want %q", lost[0].ErrorClass, wantClass)
	}
	if lost[0].ServerAddress != "h" || lost[0].ServerPort != 5672 {
		t.Fatalf("lost server = %q,%d want h,5672", lost[0].ServerAddress, lost[0].ServerPort)
	}
	if restored[0].ServerAddress != "h" || restored[0].ServerPort != 5672 {
		t.Fatalf("restored server = %q,%d want h,5672", restored[0].ServerAddress, restored[0].ServerPort)
	}
	if restored[0].Downtime != 500*time.Millisecond {
		t.Fatalf("Downtime = %v, want 500ms", restored[0].Downtime)
	}
}

func TestObserverReconnectStaleRecordsNeither(t *testing.T) {
	t.Parallel()
	fake := clock.NewFake(time.Unix(0, 0))
	recorded := &recordingClock{Fake: fake}
	d := &reconnectTestDriver{created: make(chan *reconnectTestConsumer, 8)}
	rec := &lifecycleObserver{}
	client := newLifecycleReconnectClient(t, d, recorded, 0, rec)
	client.reconnectRandom = func() float64 { return 1 }
	cause := errors.New("stale test cause")
	if err := client.requestReconnect(cause, 0); err != nil {
		t.Fatal(err)
	}
	advanceReconnect(t, recorded, 500*time.Millisecond, 1)
	if err := client.awaitRebuild(context.Background(), nil, 0, nil); err != nil {
		t.Fatal(err)
	}
	waitLifecycleCondition(t, "reconnect events did not arrive", func() bool {
		_, _, points := rec.snapshot()
		return len(lifecyclePointsByKind(points, ObserverConnectionLost)) == 1 && len(lifecyclePointsByKind(points, ObserverConnectionRestored)) == 1
	})
	_, _, before := rec.snapshot()
	beforeLost := len(lifecyclePointsByKind(before, ObserverConnectionLost))
	beforeRestored := len(lifecyclePointsByKind(before, ObserverConnectionRestored))
	opens := d.OpenCount()
	client.mu.Lock()
	epoch := client.current.epoch
	client.mu.Unlock()
	if epoch < 2 {
		t.Fatalf("epoch = %d, want at least 2 after reconnect", epoch)
	}
	select {
	case client.reconnectRequests <- reconnectRequest{cause: errors.New("stale"), epoch: epoch - 1}:
	default:
		t.Fatal("stale request channel blocked")
	}
	deadline := clock.NewReal().Timer(500 * time.Millisecond)
	defer deadline.Stop()
	for {
		select {
		case <-deadline.C:
			goto done
		default:
		}
		client.mu.Lock()
		queued := len(client.reconnectRequests)
		client.mu.Unlock()
		if queued == 0 {
			break
		}
		tick := clock.NewReal().Timer(time.Millisecond)
		select {
		case <-tick.C:
			tick.Stop()
		case <-deadline.C:
			tick.Stop()
			goto done
		}
	}
done:
	// Give the supervisor a moment to drop the stale request without recording.
	linger := clock.NewReal().Timer(100 * time.Millisecond)
	select {
	case <-linger.C:
	case <-clock.NewReal().Timer(time.Second).C:
	}
	_, _, after := rec.snapshot()
	if got := len(lifecyclePointsByKind(after, ObserverConnectionLost)); got != beforeLost {
		t.Fatalf("connection_lost after stale = %d, want %d", got, beforeLost)
	}
	if got := len(lifecyclePointsByKind(after, ObserverConnectionRestored)); got != beforeRestored {
		t.Fatalf("connection_restored after stale = %d, want %d", got, beforeRestored)
	}
	if got := d.OpenCount(); got != opens {
		t.Fatalf("Open count after stale = %d, want %d", got, opens)
	}
}

func TestObserverReconnectExhausted(t *testing.T) {
	t.Parallel()
	fake := clock.NewFake(time.Unix(0, 0))
	recorded := &recordingClock{Fake: fake}
	d := &reconnectTestDriver{created: make(chan *reconnectTestConsumer, 8)}
	rec := &lifecycleObserver{}
	client := newLifecycleReconnectClient(t, d, recorded, 1, rec)
	client.reconnectRandom = func() float64 { return 1 }
	d.setFailOpens(1)
	cause := errors.New("exhaust cause")
	if err := client.requestReconnect(cause, 0); err != nil {
		t.Fatal(err)
	}
	advanceReconnect(t, recorded, 500*time.Millisecond, 1)
	if err := client.awaitRebuild(context.Background(), nil, 0, nil); err == nil {
		t.Fatal("awaitRebuild() = nil, want exhaustion")
	}
	_, _, points := rec.snapshot()
	lost := lifecyclePointsByKind(points, ObserverConnectionLost)
	restored := lifecyclePointsByKind(points, ObserverConnectionRestored)
	if len(lost) != 1 {
		t.Fatalf("connection_lost count = %d, want 1", len(lost))
	}
	if len(restored) != 0 {
		t.Fatalf("connection_restored count = %d, want 0", len(restored))
	}
}

func newLifecycleDrainRunner(t *testing.T, rec Observer, drainTimeout time.Duration) (*Runner, *Client) {
	t.Helper()
	fake := clock.NewFake(time.Unix(0, 0))
	cfg := testClientConfig(t)
	client, err := New(context.Background(), cfg,
		WithDriver(&dispatchDriver{conn: &dispatchConn{producer: &dispatchProducer{}, consumer: newDispatchConsumer()}}),
		WithObserver(rec),
		withClock(fake))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	client.config.Lifecycle.DrainTimeout = drainTimeout
	runner := &Runner{
		client:       client,
		subscription: Subscription{Name: "orders", Topics: []string{"orders.created"}, Priorities: []Priority{PriorityHigh}},
		inflight:     newInflightRegistry(),
		lifecycle:    lifecycle.New(),
		done:         make(chan struct{}),
		drainStarted: make(chan struct{}),
		started:      true,
	}
	if err := runner.lifecycle.Transition(lifecycle.Ready); err != nil {
		t.Fatal(err)
	}
	return runner, client
}

func TestObserverDrain(t *testing.T) {
	t.Parallel()
	rec := &lifecycleObserver{}
	timeout := 5 * time.Second
	runner, _ := newLifecycleDrainRunner(t, rec, timeout)
	id := runner.inflight.Add()
	drainDone := make(chan error, 1)
	go func() { drainDone <- runner.Drain(context.Background()) }()
	waitLifecycleCondition(t, "drain start did not arrive", func() bool {
		starts, _, _ := rec.snapshot()
		for _, s := range starts {
			if s.Kind == ObserverDrain {
				return true
			}
		}
		return false
	})
	starts, _, _ := rec.snapshot()
	var start *StartEvent
	for i := range starts {
		if starts[i].Kind == ObserverDrain {
			start = &starts[i]
			break
		}
	}
	if start == nil {
		t.Fatal("no drain start")
	}
	if start.Drain.InFlight != 1 || start.Drain.Timeout != timeout {
		t.Fatalf("drain start = %+v, want InFlight 1 Timeout %v", start.Drain, timeout)
	}
	runner.inflight.Remove(id)
	close(runner.done)
	select {
	case err := <-drainDone:
		if err != nil {
			t.Fatalf("Drain() = %v, want nil", err)
		}
	case <-clock.NewReal().Timer(2 * time.Second).C:
		t.Fatal("Drain did not return")
	}
	_, finishes, _ := rec.snapshot()
	var finish *FinishEvent
	for i := range finishes {
		if finishes[i].Kind == ObserverDrain {
			finish = &finishes[i]
			break
		}
	}
	if finish == nil {
		t.Fatal("no drain finish: finish did not arrive before Drain returned")
	}
	if finish.Outcome != ObserverOutcomeOK {
		t.Fatalf("drain outcome = %q, want ok", finish.Outcome)
	}
	if finish.Drain.Drained != 1 || finish.Drain.Remaining != 0 {
		t.Fatalf("drain finish = %+v, want Drained 1 Remaining 0", finish.Drain)
	}
}

func TestObserverDrainNotStarted(t *testing.T) {
	t.Parallel()
	rec := &lifecycleObserver{}
	runner, _ := newLifecycleDrainRunner(t, rec, 5*time.Second)
	runner.mu.Lock()
	runner.started = false
	runner.mu.Unlock()
	if err := runner.Drain(context.Background()); err != nil {
		t.Fatalf("Drain() = %v, want nil", err)
	}
	starts, finishes, _ := rec.snapshot()
	for _, s := range starts {
		if s.Kind == ObserverDrain {
			t.Fatalf("drain start recorded for never-started runner")
		}
	}
	for _, f := range finishes {
		if f.Kind == ObserverDrain {
			t.Fatalf("drain finish recorded for never-started runner")
		}
	}
}

func TestObserverDrainContextEndsFirst(t *testing.T) {
	t.Parallel()
	rec := &lifecycleObserver{}
	runner, _ := newLifecycleDrainRunner(t, rec, 5*time.Second)
	runner.inflight.Add()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := runner.Drain(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Drain() = %v, want context canceled", err)
	}
	_, finishes, _ := rec.snapshot()
	var finish *FinishEvent
	for i := range finishes {
		if finishes[i].Kind == ObserverDrain {
			finish = &finishes[i]
			break
		}
	}
	if finish == nil {
		t.Fatal("no drain finish for cancelled context")
	}
	if finish.Outcome != ObserverOutcomeError {
		t.Fatalf("drain outcome = %q, want error", finish.Outcome)
	}
	if finish.ErrorClass != errorClassOf(context.Canceled) {
		t.Fatalf("drain class = %q, want %q", finish.ErrorClass, errorClassOf(context.Canceled))
	}
}

func waitLifecycleCondition(t *testing.T, msg string, cond func() bool) {
	t.Helper()
	deadline := clock.NewReal().Timer(2 * time.Second)
	defer deadline.Stop()
	ticker := clock.NewReal().Ticker(time.Millisecond)
	defer ticker.Stop()
	for {
		if cond() {
			return
		}
		select {
		case <-deadline.C:
			t.Fatal(msg)
		case <-ticker.C:
		}
	}
}
