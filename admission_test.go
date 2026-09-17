package f1

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

// This file pins admission, which is the one place the client decides whether a
// kind of work may proceed. The table below writes its rows as combinations of
// the flags the call sites read, not as the states admit derives from them: a
// row that named a state would only prove admit agrees with itself, while a row
// that names the flags proves it agrees with the sites it replaced.

// admitTestExhaustion is the error a reconnect that spent its budget retains,
// in the shape the supervisor builds it. The table compares error text, which
// is what a caller observes and what the existing tests pin.
var admitTestExhaustion = &driver.Error{
	Driver: "admit-table",
	Op:     "reconnect",
	K:      driver.KindFatal,
	Err:    errors.New("reconnect attempts exhausted"),
}

// admitTestConn is a driver.Conn that is never called. The table only asks
// whether the client holds a connection, so satisfying the interface is the
// whole requirement.
type admitTestConn struct{ driver.Conn }

// admitTestPublishReconnecting is the text of the error the publish sites
// return while an attempt is rebuilding the connection. Their reconnecting
// error is a driver error, so it carries the driver's name and the operation,
// which is what distinguishes it from Health's plain one.
const admitTestPublishReconnecting = "f1/admit-table: publish: f1: client is reconnecting (transient)"

// admitTestConsumeReconnecting is the same error for the consumer a runner has
// just opened: the site that admits it releases the consumer on a non-nil
// result, so the operation it names is the one that call performs.
const admitTestConsumeReconnecting = "f1/admit-table: consume: f1: client is reconnecting (transient)"

// admitTestDriver is a driver.Driver that is never opened. The reconnecting
// error names the driver it came from, so the table client needs one.
type admitTestDriver struct{}

func (admitTestDriver) Name() string                      { return "admit-table" }
func (admitTestDriver) Capabilities() driver.Capabilities { return driver.Capabilities{} }
func (admitTestDriver) Open(context.Context, driver.Config) (driver.Conn, error) {
	return nil, errors.New("admit table never opens a connection")
}

// admitFlags is one combination of the client flags admission reads, written
// out one field at a time.
type admitFlags struct {
	connected        bool
	closed           bool
	closing          bool
	shutdownStarted  bool
	producerTeardown bool
	reconnectFailed  bool
	reconnecting     bool
}

func (f admitFlags) client() *Client {
	client := &Client{
		options:          clientOptions{driver: admitTestDriver{}},
		closed:           f.closed,
		closing:          f.closing,
		shutdownStarted:  f.shutdownStarted,
		producerTeardown: f.producerTeardown,
		reconnecting:     f.reconnecting,
	}
	if f.connected {
		client.current = currentConnection{conn: &admitTestConn{}, epoch: f.epochValue()}
	}
	if f.reconnectFailed {
		client.reconnectErr = admitTestExhaustion
	}
	return client
}

// epochValue is the incarnation a client in this combination is on. A client
// that came through New holds the connection it opened on incarnation 1, and
// the epoch moves only in the critical section that installs a connection, so
// a connected client is on 1 and one with no connection is on 0.
func (f admitFlags) epochValue() uint64 {
	if !f.connected {
		return 0
	}
	return 1
}

// name renders the combination as the flags that are set, for a failure message
// that says which row went red.
func (f admitFlags) name() string {
	names := make([]string, 0, 7)
	if f.connected {
		names = append(names, "connected")
	} else {
		names = append(names, "no-connection")
	}
	for _, set := range []struct {
		name string
		on   bool
	}{
		{"closed", f.closed},
		{"closing", f.closing},
		{"shutdown-started", f.shutdownStarted},
		{"producer-torn-down", f.producerTeardown},
		{"connection-failed", f.reconnectFailed},
		{"reconnecting", f.reconnecting},
	} {
		if set.on {
			names = append(names, set.name)
		}
	}
	return strings.Join(names, "+")
}

// reachable reports whether client.go and reconnect.go can leave a live client
// in this combination.
//
//   - No connection: New refuses a nil connection and the swap refuses one too,
//     and no code clears c.current.conn, so a client that came through New
//     holds one for its whole life.
//   - closing, or the producer teardown, without shutdownStarted: beginClose
//     sets closing and shutdownStarted in one critical section, and the teardown
//     is recorded only from the publish-idle wait, which Close runs after it.
//   - closed with closing, or without shutdownStarted: finishClose is the only
//     writer of closed, and it sets it with closing already clear and
//     shutdownStarted already set.
//
// The remaining combinations include the transient ones, such as a retained
// reconnect decision while the flags still say an attempt is running: the
// supervisor records the exhaustion error inside reconnectOnce and clears
// reconnecting when the attempt ends, so another goroutine can read both.
func (f admitFlags) reachable() bool {
	switch {
	case !f.connected:
		return false
	case f.closed && f.closing:
		return false
	case f.closed && !f.shutdownStarted:
		return false
	case f.closing && !f.shutdownStarted:
		return false
	case f.producerTeardown && !f.shutdownStarted:
		return false
	default:
		return true
	}
}

// reachableAdmitFlags enumerates every combination of the six flags other than
// the connection and keeps the ones a live client can be in.
func reachableAdmitFlags() []admitFlags {
	flags := make([]admitFlags, 0, 64)
	for mask := range 1 << 6 {
		candidate := admitFlags{
			connected:        true,
			closed:           mask&1 != 0,
			closing:          mask&2 != 0,
			shutdownStarted:  mask&4 != 0,
			producerTeardown: mask&8 != 0,
			reconnectFailed:  mask&16 != 0,
			reconnecting:     mask&32 != 0,
		}
		if candidate.reachable() {
			flags = append(flags, candidate)
		}
	}
	return flags
}

var admitKinds = []workKind{
	workPublishEntry,
	workPublish,
	workSubscribe,
	workRun,
	workConsumerAdmission,
	workReconnect,
	workHealth,
}

func admitKindName(kind workKind) string {
	switch kind {
	case workPublishEntry:
		return "publish-entry"
	case workPublish:
		return "publish"
	case workSubscribe:
		return "subscribe"
	case workRun:
		return "run"
	case workConsumerAdmission:
		return "consumer-admission"
	case workReconnect:
		return "reconnect"
	case workHealth:
		return "health"
	default:
		return "unknown-kind"
	}
}

// admitWants is one row's expected answer for each kind.
type admitWants struct {
	publishEntry      string
	publish           string
	subscribe         string
	run               string
	consumerAdmission string
	reconnect         string
	health            string
}

func (w admitWants) forKind(kind workKind) string {
	switch kind {
	case workPublishEntry:
		return w.publishEntry
	case workPublish:
		return w.publish
	case workSubscribe:
		return w.subscribe
	case workRun:
		return w.run
	case workConsumerAdmission:
		return w.consumerAdmission
	case workReconnect:
		return w.reconnect
	case workHealth:
		return w.health
	default:
		return ""
	}
}

func admitErrorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// TestAdmitReproducesEachCallSitePredicate is the phase's table: every
// combination the flags can take, each of the six kinds, and the answer the
// site that decides that kind returns today. An empty want admits the work.
func TestAdmitReproducesEachCallSitePredicate(t *testing.T) {
	t.Parallel()

	const (
		closedText    = "f1: client is closed"
		closingText   = "f1: client is closing"
		noConnText    = "f1: client is not connected"
		reconnectText = "f1: client is reconnecting"
	)
	exhausted := admitTestExhaustion.Error()

	// A state the client can reach is in the upper group; the two below it are
	// the ambiguous and the hand-built combinations, and their comments say
	// which site answers them differently. claim is the incarnation the caller
	// captured, or zero for a caller that captured no connection; the fixture
	// gives a connected client incarnation 1, so claim 2 is one the client has
	// left.
	states := []struct {
		name  string
		flags admitFlags
		claim uint64
		want  admitWants
	}{
		{
			name:  "ready-live",
			flags: admitFlags{connected: true},
			want:  admitWants{},
		},
		{
			// The caller captured the connection the client is still on, so
			// every kind answers what it answers with no claim at all.
			name:  "ready-live-current-claim",
			flags: admitFlags{connected: true},
			claim: 1,
			want:  admitWants{},
		},
		{
			// The connection was replaced under the caller. Both publish sites
			// refuse with the reconnecting error, which is the text the
			// same-connection check returned for this case, and the consumer
			// admission releases the consumer it has just opened.
			name:  "ready-live-stale-claim",
			flags: admitFlags{connected: true},
			claim: 2,
			want: admitWants{
				publish:           admitTestPublishReconnecting,
				consumerAdmission: admitTestConsumeReconnecting,
			},
		},
		{
			// The ordering the connection axis carries: a retained reconnect
			// decision is read before the claim, so a publish reports the
			// exhaustion its caller can act on rather than a swap that never
			// happened. The consumer admission asks the other question, so it
			// releases the consumer whose connection was replaced.
			name:  "ready-live-connection-failed-stale-claim",
			flags: admitFlags{connected: true, reconnectFailed: true},
			claim: 2,
			want: admitWants{
				publish:           exhausted,
				consumerAdmission: admitTestConsumeReconnecting,
				reconnect:         exhausted,
				health:            exhausted,
			},
		},
		{
			// A Close is draining and the caller's connection was replaced: a
			// successor publish is admitted past the lifecycle and producer
			// clauses and refused by the claim, which is the order the sites
			// read them in.
			name:  "draining-live-stale-claim",
			flags: admitFlags{connected: true, closing: true, shutdownStarted: true},
			claim: 2,
			want: admitWants{
				publishEntry:      closedText,
				publish:           admitTestPublishReconnecting,
				subscribe:         closingText,
				run:               closingText,
				consumerAdmission: admitTestConsumeReconnecting,
				reconnect:         closingText,
				health:            closingText,
			},
		},
		{
			// The lifecycle clause is read first, so a Close that finished
			// reports its own text even though the caller's connection was
			// replaced, while the consumer admission still releases a consumer
			// that belongs to the replaced connection.
			name:  "closed-live-stale-claim",
			flags: admitFlags{connected: true, closed: true, shutdownStarted: true},
			claim: 2,
			want: admitWants{
				publishEntry:      closedText,
				publish:           closedText,
				subscribe:         closedText,
				run:               closingText,
				consumerAdmission: admitTestConsumeReconnecting,
				reconnect:         closingText,
				health:            closedText,
			},
		},
		{
			name:  "ready-live-connection-failed",
			flags: admitFlags{connected: true, reconnectFailed: true},
			want: admitWants{
				publish: exhausted, reconnect: exhausted, health: exhausted,
			},
		},
		{
			name:  "ready-live-reconnecting",
			flags: admitFlags{connected: true, reconnecting: true},
			want: admitWants{
				publish:           admitTestPublishReconnecting,
				consumerAdmission: admitTestConsumeReconnecting,
				health:            reconnectText,
			},
		},
		{
			// The window inside reconnectOnce after the exhaustion error is
			// recorded: the attempt is still the client's, and the error is
			// already retained.
			name:  "ready-live-reconnecting-connection-failed",
			flags: admitFlags{connected: true, reconnecting: true, reconnectFailed: true},
			want: admitWants{
				publish:           exhausted,
				consumerAdmission: admitTestConsumeReconnecting,
				reconnect:         exhausted,
				health:            exhausted,
			},
		},
		{
			// A Close that is running: the shared producer still stands until
			// the publish-idle wait sees zero in flight.
			name:  "draining-live",
			flags: admitFlags{connected: true, closing: true, shutdownStarted: true},
			want: admitWants{
				publishEntry: closedText,
				subscribe:    closingText,
				run:          closingText,
				reconnect:    closingText,
				health:       closingText,
			},
		},
		{
			name:  "draining-live-producer-torn-down",
			flags: admitFlags{connected: true, closing: true, shutdownStarted: true, producerTeardown: true},
			want: admitWants{
				publishEntry: closedText,
				publish:      closedText,
				subscribe:    closingText,
				run:          closingText,
				reconnect:    closingText,
				health:       closingText,
			},
		},
		{
			name:  "draining-live-connection-failed",
			flags: admitFlags{connected: true, closing: true, shutdownStarted: true, reconnectFailed: true},
			want: admitWants{
				publishEntry: closedText,
				publish:      exhausted,
				subscribe:    closingText,
				run:          closingText,
				reconnect:    closingText,
				health:       exhausted,
			},
		},
		{
			// A Close that failed part way: shutdown has begun, no Close
			// attempt is running, and the producer may or may not have been
			// torn down before the bound expired. Close itself proceeds from
			// here, which is what makes Aborted different from Draining.
			name:  "aborted-live",
			flags: admitFlags{connected: true, shutdownStarted: true},
			want: admitWants{
				publishEntry: closedText,
				subscribe:    closingText,
				run:          closingText,
				reconnect:    closingText,
				health:       closingText,
			},
		},
		{
			// The failed Close after the producer was torn down: the producer
			// fact survives the attempt that recorded it while closing does
			// not, so a successor publish is still refused.
			name:  "aborted-live-producer-torn-down",
			flags: admitFlags{connected: true, shutdownStarted: true, producerTeardown: true},
			want: admitWants{
				publishEntry: closedText,
				publish:      closedText,
				subscribe:    closingText,
				run:          closingText,
				reconnect:    closingText,
				health:       closingText,
			},
		},
		{
			name:  "aborted-live-connection-failed",
			flags: admitFlags{connected: true, shutdownStarted: true, reconnectFailed: true},
			want: admitWants{
				publishEntry: closedText,
				publish:      exhausted,
				subscribe:    closingText,
				run:          closingText,
				reconnect:    closingText,
				health:       exhausted,
			},
		},
		{
			name:  "closed-live",
			flags: admitFlags{connected: true, closed: true, shutdownStarted: true},
			want: admitWants{
				publishEntry: closedText,
				publish:      closedText,
				subscribe:    closedText,
				run:          closingText,
				reconnect:    closingText,
				health:       closedText,
			},
		},
		{
			name:  "closed-live-producer-torn-down",
			flags: admitFlags{connected: true, closed: true, shutdownStarted: true, producerTeardown: true},
			want: admitWants{
				publishEntry: closedText,
				publish:      closedText,
				subscribe:    closedText,
				run:          closingText,
				reconnect:    closingText,
				health:       closedText,
			},
		},
		{
			name:  "closed-live-connection-failed",
			flags: admitFlags{connected: true, closed: true, shutdownStarted: true, reconnectFailed: true},
			want: admitWants{
				publishEntry: closedText,
				publish:      closedText,
				subscribe:    closedText,
				run:          closingText,
				reconnect:    closingText,
				health:       closedText,
			},
		},
		{
			// Hand-built, and unreachable through New: a client never holds
			// both no connection and a retained decision about one. The
			// connection axis reports the absent connection first, so Health
			// answers with it, while the site reads the retained decision
			// before the connection and would answer with the decision. The
			// exhaustive test below excludes the combination for that reason.
			name:  "no-connection",
			flags: admitFlags{},
			want: admitWants{
				publishEntry: closedText,
				publish:      closedText,
				subscribe:    closedText,
				run:          closingText,
				health:       noConnText,
			},
		},
		{
			name:  "no-connection-shutdown-started",
			flags: admitFlags{shutdownStarted: true},
			want: admitWants{
				publishEntry: closedText,
				publish:      closedText,
				subscribe:    closedText,
				run:          closingText,
				reconnect:    closingText,
				health:       noConnText,
			},
		},
	}

	for _, state := range states {
		for _, kind := range admitKinds {
			t.Run(state.name+"/"+admitKindName(kind), func(t *testing.T) {
				t.Parallel()
				client := state.flags.client()
				want := state.want.forKind(kind)
				got := admitErrorText(client.admit(kind, state.claim))
				if got != want {
					t.Fatalf("admit(%s) with %s and claim %d = %q, want %q",
						admitKindName(kind), state.flags.name(), state.claim, got, want)
				}
			})
		}
	}
}

// TestAdmitAgreesWithTheSitesOnEveryReachableFlagCombination runs the same
// claim over every combination the flags can take rather than the states a
// person thought to write down, and over every connection a caller can claim to
// hold in that combination: none, the one the client is on, and one it has left.
// The expected answer is transcribed from the sites as they read the flags
// today, in the order they read them, so this test is a comparison against the
// code that was replaced and not a restatement of admit.
func TestAdmitAgreesWithTheSitesOnEveryReachableFlagCombination(t *testing.T) {
	t.Parallel()

	for _, flags := range reachableAdmitFlags() {
		for _, kind := range admitKinds {
			for _, claim := range flags.claims() {
				t.Run(flags.name()+"/"+admitKindName(kind)+"/"+claimName(flags, claim), func(t *testing.T) {
					t.Parallel()
					client := flags.client()
					want := admitSiteAnswer(kind, flags, claim)
					got := admitErrorText(client.admit(kind, claim))
					if got != want {
						t.Fatalf("admit(%s) with %s and claim %d = %q, want %q, the answer the sites give for these flags",
							admitKindName(kind), flags.name(), claim, got, want)
					}
				})
			}
		}
	}
}

// claims enumerates the connections a caller can hold against a client in this
// combination. A client with no connection has no incarnation for a caller to
// have captured, so the only claim it can be asked about is the zero one, the
// sites that never captured a connection pass; a connected one is asked about
// that claim, the incarnation it is on, and one it has left.
func (f admitFlags) claims() []uint64 {
	if !f.connected {
		return []uint64{0}
	}
	return []uint64{0, f.epochValue(), f.epochValue() + 1}
}

// claimName renders a claim against the client's incarnation, so a failing
// subtest says which of the three a caller held.
func claimName(f admitFlags, claim uint64) string {
	switch {
	case claim == 0:
		return "no-claim"
	case claim == f.epochValue():
		return "current-claim"
	default:
		return "stale-claim"
	}
}

// admitSiteAnswer transcribes what the sites admit replaced answer for one
// combination of flags and one connection the caller captured. Each branch is
// the predicate one of them ran, in the order it ran it:
//
//   - the application publish's entry gate: closed, shutdownStarted, no
//     connection;
//   - the shared publish admission: closed, no connection, producer torn down,
//     then the retained reconnect error, then a running attempt, then the
//     caller's connection no longer being the client's. It never read
//     shutdownStarted, which is what admits a successor publish while Close is
//     draining;
//   - Subscribe's gates: closed or no connection, then shutdownStarted;
//   - Runner.Run's gate: closed, shutdownStarted, no connection;
//   - the consumer admission: a running attempt, then the caller's connection
//     no longer being the client's. It does not read the retained reconnect
//     error, which reported a connection that is still the one the client
//     holds;
//   - requestReconnect's gate: closed or shutdownStarted, then the retained
//     reconnect error;
//   - Health: closed, then the retained reconnect error, then no connection,
//     then a running attempt, then shutdownStarted.
func admitSiteAnswer(kind workKind, f admitFlags, claim uint64) string {
	switch kind {
	case workPublishEntry:
		if f.closed || f.shutdownStarted || !f.connected {
			return "f1: client is closed"
		}
	case workPublish:
		if f.closed || !f.connected || f.producerTeardown {
			return "f1: client is closed"
		}
		if f.reconnectFailed {
			return admitTestExhaustion.Error()
		}
		if f.reconnecting {
			return admitTestPublishReconnecting
		}
		if claim != 0 && claim != f.epochValue() {
			return admitTestPublishReconnecting
		}
	case workSubscribe:
		if f.closed || !f.connected {
			return "f1: client is closed"
		}
		if f.shutdownStarted {
			return "f1: client is closing"
		}
	case workRun:
		if f.closed || f.shutdownStarted || !f.connected {
			return "f1: client is closing"
		}
	case workConsumerAdmission:
		if f.reconnecting {
			return admitTestConsumeReconnecting
		}
		if claim != 0 && claim != f.epochValue() {
			return admitTestConsumeReconnecting
		}
	case workReconnect:
		if f.closed || f.shutdownStarted {
			return "f1: client is closing"
		}
		if f.reconnectFailed {
			return admitTestExhaustion.Error()
		}
	case workHealth:
		if f.closed {
			return "f1: client is closed"
		}
		if f.reconnectFailed {
			return admitTestExhaustion.Error()
		}
		if !f.connected {
			return "f1: client is not connected"
		}
		if f.reconnecting {
			return "f1: client is reconnecting"
		}
		if f.shutdownStarted {
			return "f1: client is closing"
		}
	}
	return ""
}

// TestReconnectSwapMovesTheEpochAndReleasesTheWaiterParkedBeforeIt pins the two
// halves of a swap: the epoch names the new connection, and the wake releases
// the waiters parked on the old one. The attempt is driven directly rather than
// through the supervisor, because the point of the test is what a waiter can
// observe between the swap and the end of the attempt.
func TestReconnectSwapMovesTheEpochAndReleasesTheWaiterParkedBeforeIt(t *testing.T) {
	fake := clock.NewFake(time.Unix(600, 0))
	recorded := &recordingClock{Fake: fake}
	d := &reconnectTestDriver{created: make(chan *reconnectTestConsumer, 8)}
	client := newReconnectTestClient(t, d, recorded, 5)
	client.reconnectRandom = func() float64 { return 1 }
	d.setFailOpens(1)

	client.mu.Lock()
	epoch, parked := client.epochLocked()
	client.mu.Unlock()
	if epoch != 1 {
		t.Fatalf("epoch on the connection New opened = %d, want 1", epoch)
	}

	client.mu.Lock()
	client.reconnecting = true
	client.mu.Unlock()

	attemptDone := make(chan error, 1)
	go func() { attemptDone <- client.reconnectOnce(context.Background(), errors.New("transient")) }()
	advanceReconnect(t, recorded, 500*time.Millisecond, 1)
	advanceReconnect(t, recorded, time.Second, 2)

	// The swap has happened and the attempt has not ended: reconnectOnce
	// returned the new connection, and finishReconnect is what ends an attempt.
	//
	// The epoch is read the way a woken waiter reads it: on the wake itself, and
	// with nothing else ordering this read behind the reconnect goroutine. A
	// read taken after reconnectOnce returned would be ordered behind the swap
	// by the join, and would not see an epoch that moves after the wake.
	select {
	case <-parked:
	case <-clock.NewReal().Timer(time.Second).C:
		t.Fatal("the waiter parked before the swap was not released by it")
	}
	client.mu.Lock()
	swapped, next := client.epochLocked()
	client.mu.Unlock()
	if swapped != 2 {
		t.Fatalf("epoch when the swap released the waiter = %d, want 2", swapped)
	}
	select {
	case <-next:
		t.Fatal("a waiter parked after the swap was released before the attempt ended")
	default:
	}
	if err := <-attemptDone; err != nil {
		t.Fatalf("reconnectOnce() = %v, want nil after a successful swap", err)
	}

	client.finishReconnect(nil)
	select {
	case <-next:
	case <-clock.NewReal().Timer(time.Second).C:
		t.Fatal("the end of the attempt did not release the waiter parked after the swap")
	}
	client.mu.Lock()
	attemptErr := client.attemptErr
	client.mu.Unlock()
	if attemptErr != nil {
		t.Fatalf("attempt error after a successful swap = %v, want nil", attemptErr)
	}
}

// TestReconnectExhaustionReleasesTheWaiterWithTheEpochUnchanged pins the wake
// that a failed attempt owes its waiters: the epoch stays where it was, so the
// waiter reads the connection state and finds the retained exhaustion error
// rather than waiting for a connection that is not coming.
func TestReconnectExhaustionReleasesTheWaiterWithTheEpochUnchanged(t *testing.T) {
	fake := clock.NewFake(time.Unix(700, 0))
	recorded := &recordingClock{Fake: fake}
	d := &reconnectTestDriver{created: make(chan *reconnectTestConsumer, 8)}
	client := newReconnectTestClient(t, d, recorded, 1)
	client.reconnectRandom = func() float64 { return 1 }
	d.setFailOpens(1)

	client.mu.Lock()
	epoch, parked := client.epochLocked()
	client.mu.Unlock()
	if err := client.requestReconnect(errors.New("transient"), epoch); err != nil {
		t.Fatal(err)
	}
	advanceReconnect(t, recorded, 500*time.Millisecond, 1)

	select {
	case <-parked:
	case <-clock.NewReal().Timer(2 * time.Second).C:
		t.Fatal("the end of a failed attempt did not release the waiter parked before it")
	}
	client.mu.Lock()
	after, _ := client.epochLocked()
	state := client.connStateLocked()
	attemptErr := client.attemptErr
	reconnectErr := client.reconnectErr
	client.mu.Unlock()
	if after != epoch {
		t.Fatalf("epoch after an exhausted budget = %d, want %d: no swap happened", after, epoch)
	}
	if state != connFailed {
		t.Fatalf("connection state after an exhausted budget = %s, want failed", connStateName(state))
	}
	if reconnectErr == nil || !errors.Is(attemptErr, reconnectErr) {
		t.Fatalf("attempt error after an exhausted budget = %v, want the retained reconnect error %v", attemptErr, reconnectErr)
	}
	if kind, classified := driver.Classify(attemptErr); !classified || kind != driver.KindFatal {
		t.Fatalf("attempt error after an exhausted budget = %v, want a classified fatal error", attemptErr)
	}
}

// TestSupervisorExitReleasesTheWaiterParkedWithNoAttemptInFlight pins the wake
// the supervisor owes when it stops: a waiter parked with nothing in flight has
// no attempt end to wait for, and the loop leaving is what it must observe.
func TestSupervisorExitReleasesTheWaiterParkedWithNoAttemptInFlight(t *testing.T) {
	client, cancel := newReconnectSupervisorTestClient()
	client.mu.Lock()
	epoch, parked := client.epochLocked()
	client.mu.Unlock()

	cancel()
	go client.reconnectSupervisor()
	select {
	case <-parked:
	case <-clock.NewReal().Timer(time.Second).C:
		t.Fatal("the supervisor's exit did not release the waiter")
	}
	select {
	case <-client.supervisorDone:
	case <-clock.NewReal().Timer(time.Second).C:
		t.Fatal("reconnect supervisor did not exit")
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.current.epoch != epoch {
		t.Fatalf("epoch after the supervisor exited = %d, want %d", client.current.epoch, epoch)
	}
	if !errors.Is(client.attemptErr, context.Canceled) {
		t.Fatalf("attempt error after the supervisor exited = %v, want context canceled", client.attemptErr)
	}
}

// TestSubscribeIsAdmittedWhileTheConnectionIsFailed pins the one connection
// state a subscribe is admitted in besides a live one. A reconnect that spent
// its budget leaves the connection failed and the error retained, and a
// subscription is still created: the runner waits for a connection before it
// opens a consumer, so a subscribe does not need one at the moment it is made.
func TestSubscribeIsAdmittedWhileTheConnectionIsFailed(t *testing.T) {
	fake := clock.NewFake(time.Unix(800, 0))
	recorded := &recordingClock{Fake: fake}
	d := &reconnectTestDriver{created: make(chan *reconnectTestConsumer, 8)}
	client := newReconnectTestClient(t, d, recorded, 1)
	client.reconnectRandom = func() float64 { return 1 }
	d.setFailOpens(1)

	client.mu.Lock()
	epoch := client.current.epoch
	client.mu.Unlock()
	if err := client.requestReconnect(errors.New("transient"), epoch); err != nil {
		t.Fatal(err)
	}
	advanceReconnect(t, recorded, 500*time.Millisecond, 1)
	waitReconnectCondition(t, func() bool {
		client.mu.Lock()
		defer client.mu.Unlock()
		return client.connStateLocked() == connFailed
	})

	runner, err := client.Subscribe(context.Background(), Subscription{
		Name:           "orders",
		Topics:         []string{"orders.created"},
		Priorities:     []Priority{PriorityHigh},
		HandlerTimeout: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Subscribe() while the connection is failed = %v, want the subscription", err)
	}
	if runner == nil {
		t.Fatal("Subscribe() while the connection is failed returned no runner")
	}
}

// TestSubscribeIsRefusedWhileCloseIsDraining pins the subscribe gate in the
// state a Close in flight leaves. The producer still stands, so a publish is
// admitted, and a subscription is still refused: nothing would drain the runner
// it created.
func TestSubscribeIsRefusedWhileCloseIsDraining(t *testing.T) {
	client, runner, _, release := newBudgetTestClient(t)
	client.mu.Lock()
	client.runners[runner] = struct{}{}
	client.mu.Unlock()

	closeDone := make(chan error, 1)
	go func() { closeDone <- client.Close(context.Background()) }()
	waitClientClosing(t, client)

	_, err := client.Subscribe(context.Background(), Subscription{
		Name:           "late",
		Topics:         []string{"orders.created"},
		Priorities:     []Priority{PriorityHigh},
		HandlerTimeout: 10 * time.Millisecond,
	})
	if err == nil || !strings.Contains(err.Error(), "client is closing") {
		t.Fatalf("Subscribe() while Close is draining = %v, want refusal", err)
	}

	release()
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close() after the drain was released = %v, want nil", err)
		}
	case <-clock.NewReal().Timer(2 * time.Second).C:
		t.Fatal("Close did not return after the drain was released")
	}
}

func connStateName(state connState) string {
	switch state {
	case connNone:
		return "none"
	case connFailed:
		return "failed"
	case connReconnecting:
		return "reconnecting"
	case connLive:
		return "live"
	default:
		return "unknown-state"
	}
}
