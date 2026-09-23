package f1

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/lifecycle"
)

// This file pins admission, which is the one place the client decides whether a
// kind of work may proceed. The table below writes its rows as combinations of
// the client's stored state, because that is what the client keeps: a row that
// named the flags the call sites used to read would name facts the client no
// longer stores. The expected answers are the ones the sites gave for those
// combinations before the state was stored, transcriptions and all, so the table
// still pins that admission did not move while its inputs did.

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

// admitState is one combination of the client's stored state, written out one
// stored value at a time: the lifecycle, the connection axis, whether the client
// holds a connection at all, the reconnect decision a failed connection carries,
// and the producer fact.
//
// The connection axis is the client's own stored value, so a row names an
// attempt with connReconnecting or leaves it out for a live connection; the
// fixture stores connLive in that case rather than the axis's zero value,
// because the client never stores connNone. A connection that failed is not
// stored as an axis value either: it is the retained reconnectErr the failed
// connection carries, which is what a refused caller is returned.
type admitState struct {
	lifecycle        lifecycle.State
	conn             connState
	connected        bool
	reconnectErr     error
	producerTeardown bool
}

func (s admitState) client() *Client {
	conn := s.conn
	if conn != connReconnecting {
		conn = connLive
	}
	client := &Client{
		options:          clientOptions{driver: admitTestDriver{}},
		lifecycle:        lifecycleIn(s.lifecycle),
		conn:             conn,
		producerTeardown: s.producerTeardown,
		reconnectErr:     s.reconnectErr,
	}
	if s.connected {
		client.current = currentConnection{conn: &admitTestConn{}, epoch: s.epochValue()}
	}
	return client
}

// epochValue is the incarnation a client in this combination is on. A client
// that came through New holds the connection it opened on incarnation 1, and
// the epoch moves only in the critical section that installs a connection, so
// a connected client is on 1 and one with no connection is on 0.
func (s admitState) epochValue() uint64 {
	if !s.connected {
		return 0
	}
	return 1
}

// name renders the combination as the stored values that are set, for a failure
// message that says which row went red.
func (s admitState) name() string {
	names := []string{s.lifecycle.String()}
	if s.connected {
		names = append(names, "connected")
	} else {
		names = append(names, "no-connection")
	}
	if s.conn == connReconnecting {
		names = append(names, "reconnecting")
	}
	for _, set := range []struct {
		name string
		on   bool
	}{
		{"connection-failed", s.reconnectErr != nil},
		{"producer-torn-down", s.producerTeardown},
	} {
		if set.on {
			names = append(names, set.name)
		}
	}
	return strings.Join(names, "+")
}

// reachable reports whether a client that came through New can be in this
// combination.
//
//   - No connection: New refuses a nil connection and the swap refuses one too,
//     and no code clears c.current.conn, so a client that came through New
//     holds one for its whole life. The table keeps the combination anyway: it
//     is the state a hand-built client is in, and the exhaustive test below
//     leaves it out for that reason.
//   - The producer teardown with a Ready lifecycle: the teardown is recorded
//     only from the publish-idle wait, which Close runs after it has entered the
//     drain, and the lifecycle never returns to Ready from there.
//
// Everything else is reachable. The three parts are independent facts, so they
// combine freely: a retained reconnect decision can be read while an attempt is
// still the client's, a Close can be draining while an attempt is winding down,
// and a Close that failed after the producer was torn down leaves both facts set.
func (s admitState) reachable() bool {
	switch {
	case !s.connected:
		return false
	case s.producerTeardown && s.lifecycle == lifecycle.Ready:
		return false
	default:
		return true
	}
}

// reachableAdmitStates enumerates every combination of the stored state a live
// client can be in: the four lifecycle states, a live or reconnecting
// connection, a retained reconnect decision or none, and the producer fact.
func reachableAdmitStates() []admitState {
	var states []admitState
	for _, life := range []lifecycle.State{lifecycle.Ready, lifecycle.Draining, lifecycle.Aborted, lifecycle.Closed} {
		for _, conn := range []connState{connLive, connReconnecting} {
			for _, failed := range []bool{false, true} {
				for _, tornDown := range []bool{false, true} {
					candidate := admitState{lifecycle: life, conn: conn, connected: true, producerTeardown: tornDown}
					if failed {
						candidate.reconnectErr = admitTestExhaustion
					}
					if candidate.reachable() {
						states = append(states, candidate)
					}
				}
			}
		}
	}
	return states
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

// TestAdmitReproducesEachCallSitePredicate is the table: every stored
// combination a person wrote down, each of the seven kinds, and the answer the
// site that decides that kind returned for it. An empty want admits the work.
func TestAdmitReproducesEachCallSitePredicate(t *testing.T) {
	t.Parallel()

	const (
		closedText    = "f1: client is closed"
		closingText   = "f1: client is closing"
		noConnText    = "f1: client is not connected"
		reconnectText = "f1: client is reconnecting"
	)
	exhausted := admitTestExhaustion.Error()

	// The rows a live client can be in come first; the last one is the state a
	// hand-built client is in and no path through New reaches. claim is the
	// incarnation the caller captured, or zero for a caller that captured no
	// connection; the fixture gives a connected client incarnation 1, so claim 2
	// is one the client has left.
	rows := []struct {
		name  string
		state admitState
		claim uint64
		want  admitWants
	}{
		{
			name:  "ready-live",
			state: admitState{lifecycle: lifecycle.Ready, connected: true},
			want:  admitWants{},
		},
		{
			// The caller captured the connection the client is still on, so
			// every kind answers what it answers with no claim at all.
			name:  "ready-live-current-claim",
			state: admitState{lifecycle: lifecycle.Ready, connected: true},
			claim: 1,
			want:  admitWants{},
		},
		{
			// The connection was replaced under the caller. Both publish sites
			// refuse with the reconnecting error, which is the text the
			// same-connection check returned for this case, and the consumer
			// admission releases the consumer it has just opened.
			name:  "ready-live-stale-claim",
			state: admitState{lifecycle: lifecycle.Ready, connected: true},
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
			state: admitState{lifecycle: lifecycle.Ready, connected: true, reconnectErr: admitTestExhaustion},
			claim: 2,
			want: admitWants{
				publishEntry:      exhausted,
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
			state: admitState{lifecycle: lifecycle.Draining, connected: true},
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
			state: admitState{lifecycle: lifecycle.Closed, connected: true},
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
			state: admitState{lifecycle: lifecycle.Ready, connected: true, reconnectErr: admitTestExhaustion},
			want: admitWants{
				publishEntry: exhausted, publish: exhausted, reconnect: exhausted, health: exhausted,
			},
		},
		{
			name:  "ready-live-reconnecting",
			state: admitState{lifecycle: lifecycle.Ready, connected: true, conn: connReconnecting},
			want: admitWants{
				publishEntry:      admitTestPublishReconnecting,
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
			state: admitState{lifecycle: lifecycle.Ready, connected: true, conn: connReconnecting, reconnectErr: admitTestExhaustion},
			want: admitWants{
				publishEntry:      exhausted,
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
			state: admitState{lifecycle: lifecycle.Draining, connected: true},
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
			state: admitState{lifecycle: lifecycle.Draining, connected: true, producerTeardown: true},
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
			state: admitState{lifecycle: lifecycle.Draining, connected: true, reconnectErr: admitTestExhaustion},
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
			state: admitState{lifecycle: lifecycle.Aborted, connected: true},
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
			state: admitState{lifecycle: lifecycle.Aborted, connected: true, producerTeardown: true},
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
			state: admitState{lifecycle: lifecycle.Aborted, connected: true, reconnectErr: admitTestExhaustion},
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
			state: admitState{lifecycle: lifecycle.Closed, connected: true},
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
			state: admitState{lifecycle: lifecycle.Closed, connected: true, producerTeardown: true},
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
			state: admitState{lifecycle: lifecycle.Closed, connected: true, reconnectErr: admitTestExhaustion},
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
			state: admitState{lifecycle: lifecycle.Ready},
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
			state: admitState{lifecycle: lifecycle.Aborted},
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

	for _, row := range rows {
		for _, kind := range admitKinds {
			t.Run(row.name+"/"+admitKindName(kind), func(t *testing.T) {
				t.Parallel()
				client := row.state.client()
				want := row.want.forKind(kind)
				got := admitErrorText(client.admit(kind, row.claim))
				if got != want {
					t.Fatalf("admit(%s) with %s and claim %d = %q, want %q",
						admitKindName(kind), row.state.name(), row.claim, got, want)
				}
			})
		}
	}
}

// TestAdmitAgreesWithTheSitesOnEveryReachableState runs the same claim over
// every combination of the stored state a live client can be in rather than the
// rows a person thought to write down, and over every connection a caller can
// claim to hold in that combination: none, the one the client is on, and one it
// has left. The expected answer is transcribed from the sites as they read the
// state today, in the order they read it.
func TestAdmitAgreesWithTheSitesOnEveryReachableState(t *testing.T) {
	t.Parallel()

	for _, state := range reachableAdmitStates() {
		for _, kind := range admitKinds {
			for _, claim := range state.claims() {
				t.Run(state.name()+"/"+admitKindName(kind)+"/"+claimName(state, claim), func(t *testing.T) {
					t.Parallel()
					client := state.client()
					want := admitSiteAnswer(kind, state, claim)
					got := admitErrorText(client.admit(kind, claim))
					if got != want {
						t.Fatalf("admit(%s) with %s and claim %d = %q, want %q, the answer the sites give for this state",
							admitKindName(kind), state.name(), claim, got, want)
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
func (s admitState) claims() []uint64 {
	if !s.connected {
		return []uint64{0}
	}
	return []uint64{0, s.epochValue(), s.epochValue() + 1}
}

// claimName renders a claim against the client's incarnation, so a failing
// subtest says which of the three a caller held.
func claimName(s admitState, claim uint64) string {
	switch {
	case claim == 0:
		return "no-claim"
	case claim == s.epochValue():
		return "current-claim"
	default:
		return "stale-claim"
	}
}

// admitSiteAnswer transcribes what the sites admit replaced answered for one
// combination of the stored state and one connection the caller captured. Each
// branch is the predicate one of them ran, in the order it ran it, with the
// stored lifecycle read where the flags behind it were read:
//
//   - the application publish's entry gate: shutdown begun, no connection,
//     then the retained reconnect error, then a running attempt;
//   - the shared publish admission: Closed, no connection, producer torn down,
//     then the retained reconnect error, then a running attempt, then the
//     caller's connection no longer being the client's. It never read a drain
//     in flight, which is what admits a successor publish while Close is
//     draining;
//   - Subscribe's gates: Closed or no connection, then shutdown begun;
//   - Runner.Run's gate: shutdown begun, no connection;
//   - the consumer admission: a running attempt, then the caller's connection
//     no longer being the client's. It does not read the retained reconnect
//     error, which reported a connection that is still the one the client
//     holds;
//   - requestReconnect's gate: shutdown begun, then the retained reconnect
//     error;
//   - Health: Closed, then the retained reconnect error, then no connection,
//     then a running attempt, then a shutdown that is not a drain.
func admitSiteAnswer(kind workKind, s admitState, claim uint64) string {
	shutdownBegan := s.lifecycle != lifecycle.Ready
	switch kind {
	case workPublishEntry:
		if shutdownBegan || !s.connected {
			return "f1: client is closed"
		}
		if s.reconnectErr != nil {
			return admitTestExhaustion.Error()
		}
		if s.conn == connReconnecting {
			return admitTestPublishReconnecting
		}
	case workPublish:
		if s.lifecycle == lifecycle.Closed || !s.connected || s.producerTeardown {
			return "f1: client is closed"
		}
		if s.reconnectErr != nil {
			return admitTestExhaustion.Error()
		}
		if s.conn == connReconnecting {
			return admitTestPublishReconnecting
		}
		if claim != 0 && claim != s.epochValue() {
			return admitTestPublishReconnecting
		}
	case workSubscribe:
		if s.lifecycle == lifecycle.Closed || !s.connected {
			return "f1: client is closed"
		}
		if shutdownBegan {
			return "f1: client is closing"
		}
	case workRun:
		if shutdownBegan || !s.connected {
			return "f1: client is closing"
		}
	case workConsumerAdmission:
		if s.conn == connReconnecting {
			return admitTestConsumeReconnecting
		}
		if claim != 0 && claim != s.epochValue() {
			return admitTestConsumeReconnecting
		}
	case workReconnect:
		if shutdownBegan {
			return "f1: client is closing"
		}
		if s.reconnectErr != nil {
			return admitTestExhaustion.Error()
		}
	case workHealth:
		if s.lifecycle == lifecycle.Closed {
			return "f1: client is closed"
		}
		if s.reconnectErr != nil {
			return admitTestExhaustion.Error()
		}
		if !s.connected {
			return "f1: client is not connected"
		}
		if s.conn == connReconnecting {
			return "f1: client is reconnecting"
		}
		if shutdownBegan {
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
	client.conn = connReconnecting
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

// epochLocked returns the connection incarnation the client is on and the wake
// that is released when that incarnation changes or when a reconnect attempt
// ends. The caller holds c.mu, so the pair is one view: the epoch names the
// connection the wake belongs to. Read c.attemptErr under the same lock after
// the wake fires, and it carries the outcome of the attempt that released the
// waiter.
func (c *Client) epochLocked() (uint64, <-chan struct{}) {
	return c.current.epoch, c.wakeLocked()
}
