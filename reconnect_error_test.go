package f1

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

// TestUnservedRequestReleasesTheWaitersWithItsOwnError pins what a caller
// waiting on a rebuild no supervisor will serve receives. The request is
// recorded and then released because the supervisor is already leaving, and the
// release carries the error the request was refused with, so a caller that
// reads the attempt's outcome reads that error rather than waiting for a
// rebuild that is not coming.
func TestUnservedRequestReleasesTheWaitersWithItsOwnError(t *testing.T) {
	d := &reconnectTestDriver{created: make(chan *reconnectTestConsumer, 1)}
	client, cancel := newReconnectSupervisorTestClient()
	client.mu.Lock()
	client.current = currentConnection{conn: &reconnectTestConn{driver: d, admin: &reconnectTestAdmin{}}, epoch: 7}
	epoch, parked := client.epochLocked()
	client.mu.Unlock()
	cancel()

	if err := client.requestReconnect(errors.New("orders consumer failed"), epoch); !errors.Is(err, context.Canceled) {
		t.Fatalf("requestReconnect() = %v, want the supervisor's context error", err)
	}
	select {
	case <-parked:
	default:
		t.Fatal("the unserved request did not release the waiter parked on the attempt")
	}
	if got := client.awaitRebuild(context.Background(), nil, epoch, nil); !errors.Is(got, context.Canceled) {
		t.Fatalf("awaitRebuild() = %v, want the error the unserved request was released with", got)
	}
}

// TestUnservedRequestLeavesAReplacedClaimAlone pins the epoch that guards the
// release: a request that names a connection a swap has already replaced
// belongs to a change that has ended, and clearing its claim would report the
// client live while the attempt the swap started is still rebuilding it.
//
// The request is held in its send until the supervisor's context ends, so the
// swap can be installed in that window and the release still runs against the
// epoch the request named.
func TestUnservedRequestLeavesAReplacedClaimAlone(t *testing.T) {
	d := &reconnectTestDriver{created: make(chan *reconnectTestConsumer, 1)}
	client, cancel := newReconnectSupervisorTestClient()
	client.mu.Lock()
	client.current = currentConnection{conn: &reconnectTestConn{driver: d, admin: &reconnectTestAdmin{}}, epoch: 7}
	client.mu.Unlock()
	// A request already queued holds the channel full, which is what keeps the
	// request under test inside its send.
	client.reconnectRequests <- reconnectRequest{cause: errors.New("queued"), epoch: 6}

	requestErr := make(chan error, 1)
	go func() { requestErr <- client.requestReconnect(errors.New("orders consumer failed"), 7) }()
	waitReconnectCondition(t, func() bool {
		client.mu.Lock()
		defer client.mu.Unlock()
		return client.conn == connReconnecting
	})
	// The swap that answers the request: a later incarnation is installed while
	// the request is still in flight, so the connection it names is no longer
	// the client's when the release runs.
	client.mu.Lock()
	client.current = currentConnection{conn: &reconnectTestConn{driver: d, admin: &reconnectTestAdmin{}}, epoch: 8}
	client.mu.Unlock()
	cancel()

	select {
	case err := <-requestErr:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("requestReconnect() = %v, want the supervisor's context error", err)
		}
	case <-clock.NewReal().Timer(2 * time.Second).C:
		t.Fatal("the request did not return after the supervisor's context ended")
	}
	if err := client.Health(context.Background()); err == nil || !strings.Contains(err.Error(), "client is reconnecting") {
		t.Fatalf("Health() after the unserved request = %v, want the attempt in flight to still be reported", err)
	}
}

// TestReconnectAttemptRefusedByAClosingClientClosesItsConnection pins the
// connection an attempt gives up when the client begins closing while the
// attempt is inside the driver: the connection it opened is handed back, so a
// shutdown cannot leave a live connection that nothing owns, and the attempt
// installs no replacement.
func TestReconnectAttemptRefusedByAClosingClientClosesItsConnection(t *testing.T) {
	fake := clock.NewFake(time.Unix(910, 0))
	recorded := &recordingClock{Fake: fake}
	gate := &attemptGate{}
	d := &attemptDriver{reconnectTestDriver: &reconnectTestDriver{created: make(chan *reconnectTestConsumer, 1)}, gate: gate}
	client := newAttemptTestClient(t, d, recorded)
	client.reconnectRandom = func() float64 { return 0 }

	entered, release := gate.armed()
	client.mu.Lock()
	epoch := client.current.epoch
	client.mu.Unlock()
	if err := client.requestReconnect(errors.New("transient"), epoch); err != nil {
		t.Fatal(err)
	}
	advanceReconnect(t, recorded, 0, 1)
	select {
	case <-entered:
	case <-clock.NewReal().Timer(2 * time.Second).C:
		t.Fatal("the replacement attempt never reached the driver")
	}

	closed := make(chan error, 1)
	go func() { closed <- client.Close(context.Background()) }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close() while an attempt was in the driver = %v, want nil", err)
		}
	case <-clock.NewReal().Timer(2 * time.Second).C:
		t.Fatal("Close did not return while an attempt was held in the driver")
	}
	release()
	waitReconnectCondition(t, func() bool { return !client.isReconnecting() })

	opened := d.opened()
	if len(opened) != 2 {
		t.Fatalf("driver opened %d connections, want the client's own and the refused replacement", len(opened))
	}
	if !opened[1].closed() {
		t.Fatal("the connection the refused attempt opened was left open")
	}
	contexts := opened[1].contexts()
	if len(contexts) != 1 {
		t.Fatalf("the refused replacement was closed %d times, want once", len(contexts))
	}
	if contexts[0] != nil {
		t.Fatalf("the refused replacement was closed on a context that had already ended: %v", contexts[0])
	}
	client.mu.Lock()
	swapped := client.current.epoch
	client.mu.Unlock()
	if swapped != epoch {
		t.Fatalf("epoch after the refused attempt = %d, want %d: a closing client installs no replacement", swapped, epoch)
	}
}

// TestReconnectAttemptWithFailedTopologyClosesItsConnection pins the connection
// an attempt gives up when its replacement cannot be prepared: the connection
// it opened is handed back before the attempt backs off for the next one, so a
// failing topology step cannot leave a live connection behind per attempt.
func TestReconnectAttemptWithFailedTopologyClosesItsConnection(t *testing.T) {
	fake := clock.NewFake(time.Unix(920, 0))
	recorded := &recordingClock{Fake: fake}
	d := &attemptDriver{
		reconnectTestDriver: &reconnectTestDriver{created: make(chan *reconnectTestConsumer, 1)},
		adminFor: func(open int) driver.Admin {
			if open > 1 {
				return lifecycleFailAdmin{}
			}
			return nil
		},
	}
	client := newAttemptTestClient(t, d, recorded, WithPublishTopics("orders.created"))
	client.reconnectRandom = func() float64 { return 0 }

	client.mu.Lock()
	epoch := client.current.epoch
	client.mu.Unlock()
	if err := client.requestReconnect(errors.New("transient"), epoch); err != nil {
		t.Fatal(err)
	}
	advanceReconnect(t, recorded, 0, 1)
	// The attempt's topology step failed and the loop is in its second backoff,
	// which it reaches only after the connection it opened was handed back.
	waitReconnectCondition(t, func() bool { return recorded.sleepCount() >= 2 })

	opened := d.opened()
	if len(opened) != 2 {
		t.Fatalf("driver opened %d connections, want the client's own and the failed replacement", len(opened))
	}
	if !opened[1].closed() {
		t.Fatal("the connection whose topology step failed was left open")
	}
}

// TestRetiredConnectionIsClosedOnAContextTheSupervisorCannotCancel pins the
// context the swap closes the connection it replaced on: a shutdown that ends
// the supervisor must not end that close, or a driver whose close returns early
// on a cancelled context leaves the connection up. The connection is closed
// exactly once, by the retirement.
func TestRetiredConnectionIsClosedOnAContextTheSupervisorCannotCancel(t *testing.T) {
	fake := clock.NewFake(time.Unix(930, 0))
	recorded := &recordingClock{Fake: fake}
	gate := &attemptGate{}
	d := &attemptDriver{reconnectTestDriver: &reconnectTestDriver{created: make(chan *reconnectTestConsumer, 1)}, gate: gate}
	client := newAttemptTestClient(t, d, recorded)
	client.reconnectRandom = func() float64 { return 0 }

	entered, release := gate.armed()
	client.mu.Lock()
	epoch := client.current.epoch
	client.mu.Unlock()
	if err := client.requestReconnect(errors.New("transient"), epoch); err != nil {
		t.Fatal(err)
	}
	advanceReconnect(t, recorded, 0, 1)
	select {
	case <-entered:
	case <-clock.NewReal().Timer(2 * time.Second).C:
		t.Fatal("the replacement attempt never reached the driver")
	}
	// The supervisor's context ends while the attempt is inside the driver, so
	// the swap that follows retires the old connection with the shutdown
	// already under way. The lifecycle is untouched, so the swap still runs.
	client.supervisorCancel()
	release()
	select {
	case <-client.supervisorDone:
	case <-clock.NewReal().Timer(2 * time.Second).C:
		t.Fatal("the reconnect supervisor did not exit")
	}

	opened := d.opened()
	if len(opened) != 2 {
		t.Fatalf("driver opened %d connections, want the client's own and the replacement", len(opened))
	}
	retired := opened[0]
	if !retired.closed() {
		t.Fatal("the replaced connection was not closed")
	}
	contexts := retired.contexts()
	if len(contexts) != 1 {
		t.Fatalf("the replaced connection was closed %d times, want once", len(contexts))
	}
	if contexts[0] != nil {
		t.Fatalf("the replaced connection was closed on a context that had already ended: %v", contexts[0])
	}
	client.mu.Lock()
	swapped := client.current.epoch
	client.mu.Unlock()
	if swapped != epoch+1 {
		t.Fatalf("epoch after the swap = %d, want %d", swapped, epoch+1)
	}
}

// attemptGate holds one driver call of a reconnect attempt open, so a test can
// move the client's state while the attempt is inside the driver. An unarmed
// gate lets the call through, so a test arms it after the client's own
// connection has already been opened, and an armed gate applies to one call.
type attemptGate struct {
	mu      sync.Mutex
	entered chan struct{}
	release chan struct{}
}

// armed returns the signal that a held call has entered and the release that
// lets it through. release is idempotent.
func (g *attemptGate) armed() (<-chan struct{}, func()) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.entered = make(chan struct{})
	g.release = make(chan struct{})
	entered, release := g.entered, g.release
	var once sync.Once
	return entered, func() { once.Do(func() { close(release) }) }
}

// hold signals the entered channel and blocks until the release runs, and does
// nothing at all when the gate is unarmed.
func (g *attemptGate) hold() {
	g.mu.Lock()
	entered, release := g.entered, g.release
	g.entered, g.release = nil, nil
	g.mu.Unlock()
	if entered == nil {
		return
	}
	close(entered)
	<-release
}

// attemptConn is the connection the reconnect test driver opened, with the two
// things these tests have to see on top of it: the admin the attempt is given,
// which a test can make refuse topology, and every context Close was handed,
// which is how a test tells a close that could reach the driver from one that
// could not.
type attemptConn struct {
	driver.Conn
	raw   *reconnectTestConn
	admin driver.Admin
	mu    sync.Mutex
	// closeContexts is the error each Close was handed, in call order.
	closeContexts []error
}

func (c *attemptConn) Admin() driver.Admin {
	if c.admin != nil {
		return c.admin
	}
	return c.Conn.Admin()
}

func (c *attemptConn) Close(ctx context.Context) error {
	c.mu.Lock()
	c.closeContexts = append(c.closeContexts, ctx.Err())
	c.mu.Unlock()
	return c.Conn.Close(ctx)
}

// closed reports whether the connection the driver opened is closed.
func (c *attemptConn) closed() bool { return c.raw.closed.Load() }

// contexts returns the error each Close was handed, in call order.
func (c *attemptConn) contexts() []error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]error(nil), c.closeContexts...)
}

// attemptDriver is the reconnect test driver with the seams a reconnect error
// path needs: a gate that holds the next attempt inside its driver call, and an
// admin chosen per open, so a replacement can refuse topology while the
// client's own connection does not.
type attemptDriver struct {
	*reconnectTestDriver
	gate     *attemptGate
	adminFor func(open int) driver.Admin

	mu    sync.Mutex
	conns []*attemptConn
}

func (d *attemptDriver) Open(ctx context.Context, cfg driver.Config) (driver.Conn, error) {
	conn, err := d.reconnectTestDriver.Open(ctx, cfg)
	if err != nil || conn == nil {
		return conn, err
	}
	// The driver this wraps opens a reconnect test connection on every call,
	// so the assertion cannot fail and is here to reach the flag it carries.
	opened := &attemptConn{Conn: conn, raw: conn.(*reconnectTestConn)}
	if d.adminFor != nil {
		opened.admin = d.adminFor(d.OpenCount())
	}
	d.mu.Lock()
	d.conns = append(d.conns, opened)
	gate := d.gate
	d.mu.Unlock()
	if gate != nil {
		gate.hold()
	}
	return opened, nil
}

// opened returns the connections this driver handed out, in open order.
func (d *attemptDriver) opened() []*attemptConn {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]*attemptConn(nil), d.conns...)
}

// newAttemptTestClient builds a client on d with the lifecycle budgets the
// reconnect tests use, so a wrapper driver can be driven through the same seams
// as the driver it wraps.
func newAttemptTestClient(t *testing.T, d *attemptDriver, c clock.Clock, extra ...Option) *Client {
	t.Helper()
	cfg := testClientConfig(t)
	cfg.Lifecycle.DrainTimeout = 500 * time.Millisecond
	cfg.Lifecycle.CloseTimeout = 100 * time.Millisecond
	options := []Option{
		WithDriver(d),
		WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
	}
	options = append(options, extra...)
	if c != nil {
		options = append(options, withClock(c))
	}
	client, err := New(context.Background(), cfg, options...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	return client
}
