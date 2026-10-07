package f1

import (
	"context"
	"errors"
	"strings"
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

// bindingObserver is a lifecycleObserver that also implements ObserverBinder
// with one exclusive attachment, the shape an adapter with per-Client state
// needs. Its unbind is deliberately not idempotent, so a double release shows
// up in unbinds instead of being absorbed.
type bindingObserver struct {
	lifecycleObserver
	bindErr error
	// unbindStarted and unbindRelease, when set, hold unbind open so a test
	// can act while the release is in progress.
	unbindStarted chan struct{}
	unbindRelease chan struct{}

	bindMu                sync.Mutex
	bound                 bool
	binds                 int
	unbinds               int
	boundAtDriverSelected []bool
}

func (o *bindingObserver) BindClient() (func(), error) {
	o.bindMu.Lock()
	defer o.bindMu.Unlock()
	if o.bindErr != nil {
		return nil, o.bindErr
	}
	if o.bound {
		return nil, errBindingObserverBound
	}
	o.bound = true
	o.binds++
	return func() {
		if o.unbindStarted != nil {
			close(o.unbindStarted)
			<-o.unbindRelease
		}
		o.bindMu.Lock()
		defer o.bindMu.Unlock()
		o.bound = false
		o.unbinds++
	}, nil
}

func (o *bindingObserver) Record(event PointEvent) {
	if event.Kind == ObserverDriverSelected {
		o.bindMu.Lock()
		o.boundAtDriverSelected = append(o.boundAtDriverSelected, o.bound)
		o.bindMu.Unlock()
	}
	o.lifecycleObserver.Record(event)
}

func (o *bindingObserver) state() (bound bool, binds, unbinds int) {
	o.bindMu.Lock()
	defer o.bindMu.Unlock()
	return o.bound, o.binds, o.unbinds
}

func (o *bindingObserver) requireState(t *testing.T, wantBound bool, wantBinds, wantUnbinds int) {
	t.Helper()
	bound, binds, unbinds := o.state()
	if bound != wantBound || binds != wantBinds || unbinds != wantUnbinds {
		t.Fatalf("binding = bound %v, binds %d, unbinds %d; want bound %v, binds %d, unbinds %d",
			bound, binds, unbinds, wantBound, wantBinds, wantUnbinds)
	}
}

var errBindingObserverBound = errors.New("observer already bound")

type openErrDriver struct {
	err error
}

func (openErrDriver) Name() string                      { return "test" }
func (openErrDriver) Capabilities() driver.Capabilities { return driver.Capabilities{} }
func (d openErrDriver) Open(context.Context, driver.Config) (driver.Conn, error) {
	return nil, d.err
}

func TestObserverBinderRefusalOpensNothing(t *testing.T) {
	t.Parallel()
	errRefused := errors.New("observer refused")
	rec := &bindingObserver{bindErr: errRefused}
	fakeDriver := &testDriver{conn: &testConn{}}
	client, err := New(context.Background(), testClientConfig(t), WithDriver(fakeDriver), WithObserver(rec))
	if client != nil {
		t.Cleanup(func() { _ = client.Close(context.Background()) })
		t.Fatal("New() returned a Client for a refused observer binding")
	}
	if !errors.Is(err, errRefused) || !strings.Contains(err.Error(), "f1: bind observer: ") {
		t.Fatalf("New() error = %v, want the wrapped refusal", err)
	}
	if fakeDriver.opened {
		t.Fatal("New() opened the driver after the observer refused its binding")
	}
	starts, finishes, points := rec.snapshot()
	if len(starts)+len(finishes)+len(points) != 0 {
		t.Fatalf("observer events after refused binding = %d starts, %d finishes, %d points; want none", len(starts), len(finishes), len(points))
	}
}

func TestObserverBinderBoundBeforeDriverSelected(t *testing.T) {
	t.Parallel()
	rec := &bindingObserver{}
	client, err := New(context.Background(), testClientConfig(t), WithDriver(&testDriver{conn: &testConn{}}), WithObserver(rec))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	rec.requireState(t, true, 1, 0)
	rec.bindMu.Lock()
	seen := append([]bool(nil), rec.boundAtDriverSelected...)
	rec.bindMu.Unlock()
	if len(seen) != 1 || !seen[0] {
		t.Fatalf("bound at driver_selected = %v, want [true]", seen)
	}
}

func TestObserverBinderNotCalledWhenValidationFails(t *testing.T) {
	t.Parallel()
	missingCodec := testClientConfig(t)
	missingCodec.Codec.Default = "missing"
	missingEnv := testClientConfig(t)
	missingEnv.Env = ""
	tests := []struct {
		name string
		cfg  Config
		opts []Option
	}{
		{name: "option error", cfg: testClientConfig(t), opts: []Option{WithLogger(nil)}},
		{name: "invalid config", cfg: missingEnv},
		{name: "unregistered codec", cfg: missingCodec},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rec := &bindingObserver{}
			opts := append([]Option{WithDriver(&testDriver{conn: &testConn{}}), WithObserver(rec)}, test.opts...)
			if _, err := New(context.Background(), test.cfg, opts...); err == nil {
				t.Fatal("New() = nil, want a validation error")
			}
			rec.requireState(t, false, 0, 0)
		})
	}
}

func TestObserverBinderRollsBackFailedNew(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		driver driver.Driver
	}{
		{name: "driver open error", driver: openErrDriver{err: errors.New("open boom")}},
		{name: "nil connection", driver: nilConnectionDriver{}},
		{name: "topology without admin", driver: &testDriver{conn: &testConn{}}},
		{name: "topology error", driver: &lifecycleFailDriver{conn: &lifecycleFailConn{testConn: &testConn{}, admin: lifecycleFailAdmin{}}}},
		{name: "topology error and close error", driver: &lifecycleFailDriver{conn: &lifecycleFailConn{testConn: &testConn{closeErr: errors.New("close boom")}, admin: lifecycleFailAdmin{}}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rec := &bindingObserver{}
			client, err := New(context.Background(), testClientConfig(t),
				WithDriver(test.driver), WithObserver(rec), WithPublishTopics("orders.created"))
			if err == nil {
				_ = client.Close(context.Background())
				t.Fatal("New() = nil, want a startup error")
			}
			rec.requireState(t, false, 1, 1)
			if _, _, points := rec.snapshot(); len(lifecyclePointsByKind(points, ObserverDriverSelected)) != 0 {
				t.Fatalf("driver_selected recorded by a failed New: %v", points)
			}

			// The rollback must leave the observer attachable, not only count
			// a callback.
			next, err := New(context.Background(), testClientConfig(t), WithDriver(&testDriver{conn: &testConn{}}), WithObserver(rec))
			if err != nil {
				t.Fatalf("New() after a failed New error = %v, want the observer released", err)
			}
			rec.requireState(t, true, 2, 1)
			if err := next.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			rec.requireState(t, false, 2, 2)
		})
	}
}

func TestObserverBinderReleasedOnTerminalClose(t *testing.T) {
	t.Parallel()
	rec := &bindingObserver{}
	client, err := New(context.Background(), testClientConfig(t), WithDriver(&testDriver{conn: &testConn{}}), WithObserver(rec))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := New(context.Background(), testClientConfig(t), WithDriver(&testDriver{conn: &testConn{}}), WithObserver(rec)); !errors.Is(err, errBindingObserverBound) {
		t.Fatalf("second New() error = %v, want the observer's refusal", err)
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	rec.requireState(t, false, 1, 1)
	if err := client.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	rec.requireState(t, false, 1, 1)

	next, err := New(context.Background(), testClientConfig(t), WithDriver(&testDriver{conn: &testConn{}}), WithObserver(rec))
	if err != nil {
		t.Fatalf("New() after Close error = %v, want the observer released", err)
	}
	t.Cleanup(func() { _ = next.Close(context.Background()) })
	rec.requireState(t, true, 2, 1)
}

func TestObserverWithoutBinderIsShareable(t *testing.T) {
	t.Parallel()
	rec := &lifecycleObserver{}
	for range 2 {
		client, err := New(context.Background(), testClientConfig(t), WithDriver(&testDriver{conn: &testConn{}}), WithObserver(rec))
		if err != nil {
			t.Fatalf("New() error = %v, want an observer without ObserverBinder to be shareable", err)
		}
		t.Cleanup(func() { _ = client.Close(context.Background()) })
	}
	if _, _, points := rec.snapshot(); len(lifecyclePointsByKind(points, ObserverDriverSelected)) != 2 {
		t.Fatalf("driver_selected points = %v, want one per Client", points)
	}
}
