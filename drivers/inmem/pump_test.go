package inmem

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bktrung/edm_sdk/driver"
	"github.com/bktrung/edm_sdk/internal/clock"
)

// armingClock reports every timer the pump arms on an unbuffered channel, after
// registering it, so the test reads each arm in lockstep with the pump. Closing
// stop releases a pump blocked on a report nobody will read.
type armingClock struct {
	*clock.Fake
	arms chan time.Duration
	stop chan struct{}
}

func (c *armingClock) Timer(d time.Duration) clock.Timer {
	timer := c.Fake.Timer(d)
	select {
	case c.arms <- d:
	case <-c.stop:
	}
	return timer
}

// pumpWatchdog bounds every wait in this file, so a pump that waits with no
// timer fails the test instead of hanging it.
const pumpWatchdog = 2 * time.Second

// awaitArm requires the pump's next arm to be want, allowing one stale arm
// first. The pump reads its deadline under c.mu but subtracts Now() after
// unlocking, so a pass that read a's 1s deadline before Advance(1s) arms 0s
// after it, and a pass that also registered its timer before Advance arms 1s.
// The pump runs one pass at a time, so at most one pass straddles Advance: the
// test wakes the pump after that arm, and the arm after the wake comes from a
// fresh pass. A fresh 0s arm is the spin this file guards against.
func awaitArm(t *testing.T, clk *armingClock, c *conn, want time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), pumpWatchdog)
	defer cancel()
	stale := true
	for {
		select {
		case d := <-clk.arms:
			if d == want {
				return
			}
			if !stale {
				require.NotZero(t, d, "pump re-armed a zero-length timer while waiting to arm %s", want)
				t.Fatalf("pump armed a %s timer, want %s", d, want)
			}
			stale = false
			c.signalWake()
		case <-ctx.Done():
			t.Fatalf("timed out waiting for the pump to arm a %s timer", want)
		}
	}
}

// receivePumped waits for a delivery on consumer while answering the pump's
// arms. Any of them may be stale for the reason given on awaitArm, and a pass
// that read Now() before Advance(9s) would otherwise sleep past b's due time,
// so each arm wakes the pump to recompute.
func receivePumped(t *testing.T, clk *armingClock, c *conn, consumer driver.Consumer, what string) driver.InboundMessage {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), pumpWatchdog)
	defer cancel()
	for {
		select {
		case message := <-consumer.Messages():
			return message
		case <-clk.arms:
			c.signalWake()
		case <-ctx.Done():
			t.Fatalf("timed out waiting for %s", what)
			return driver.InboundMessage{}
		}
	}
}

func TestPump_DoesNotRearmZeroTimerForIneligibleDueMessage(t *testing.T) {
	type setup struct {
		// attach creates the consumers on a and returns the ones to stop at the end.
		attach func(t *testing.T, ctx context.Context, raw driver.Conn) []driver.Consumer
		// release makes a's message takeable and returns the consumer that receives it.
		release func(t *testing.T, ctx context.Context, clk *armingClock, c *conn, consumers []driver.Consumer) driver.Consumer
	}
	cases := map[string]setup{
		"paused-consumer": {
			attach: func(t *testing.T, ctx context.Context, raw driver.Conn) []driver.Consumer {
				consumer, err := raw.Consumer(ctx, driver.ConsumerConfig{Destinations: []string{"a"}, Prefetch: 1, Effective: testCaps()})
				require.NoError(t, err)
				require.NoError(t, consumer.Pause())
				return []driver.Consumer{consumer}
			},
			release: func(t *testing.T, _ context.Context, _ *armingClock, _ *conn, consumers []driver.Consumer) driver.Consumer {
				require.NoError(t, consumers[0].Resume())
				return consumers[0]
			},
		},
		"departed-group": {
			attach: func(t *testing.T, ctx context.Context, raw driver.Conn) []driver.Consumer {
				g1, err := raw.Consumer(ctx, driver.ConsumerConfig{
					Group: "g1", Destinations: []string{"a"}, Prefetch: 1, StartAt: driver.StartEarliest, Effective: testCaps(),
				})
				require.NoError(t, err)
				g2, err := raw.Consumer(ctx, driver.ConsumerConfig{
					Group: "g2", Destinations: []string{"a"}, Prefetch: 1, StartAt: driver.StartEarliest, Effective: testCaps(),
				})
				require.NoError(t, err)
				require.NoError(t, g2.Stop(ctx))
				return []driver.Consumer{g1}
			},
			release: func(t *testing.T, ctx context.Context, clk *armingClock, c *conn, consumers []driver.Consumer) driver.Consumer {
				// g1's settle wakes the pump, which must still not spin on the
				// copy only the departed g2 is owed.
				message := receivePumped(t, clk, c, consumers[0], "g1 to receive a's message")
				require.Equal(t, []byte("a"), message.Body)
				require.NoError(t, message.Settle.Ack(ctx))
				// Ack dispatches under the connection lock, so a second copy for
				// g1 would already be on its channel here.
				require.Empty(t, consumers[0].Messages(), "g1 was given a's message again after acking it")
				awaitArm(t, clk, c, 9*time.Second)
				g2, err := c.Consumer(ctx, driver.ConsumerConfig{
					Group: "g2", Destinations: []string{"a"}, Prefetch: 1, StartAt: driver.StartEarliest, Effective: testCaps(),
				})
				require.NoError(t, err)
				return g2
			},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			clk := &armingClock{Fake: clock.NewFake(time.Unix(0, 0)), arms: make(chan time.Duration), stop: make(chan struct{})}
			ctx, raw, producer := openTest(t, clk,
				driver.DestinationSpec{Name: "a", Delay: time.Second},
				driver.DestinationSpec{Name: "b", Delay: 10 * time.Second})
			c, ok := raw.(*conn)
			require.True(t, ok)
			consumerB, err := raw.Consumer(ctx, driver.ConsumerConfig{Destinations: []string{"b"}, Prefetch: 1, Effective: testCaps()})
			require.NoError(t, err)
			consumers := tc.attach(t, ctx, raw)

			require.NoError(t, producer.Publish(ctx,
				driver.OutboundMessage{Destination: "a", Body: []byte("a")},
				driver.OutboundMessage{Destination: "b", Body: []byte("b")}))
			awaitArm(t, clk, c, time.Second)
			clk.Advance(time.Second)
			awaitArm(t, clk, c, 9*time.Second)

			receiver := tc.release(t, ctx, clk, c, consumers)
			message := receivePumped(t, clk, c, receiver, "a's message after it became takeable")
			require.Equal(t, []byte("a"), message.Body)
			require.NoError(t, message.Settle.Ack(ctx))
			if receiver != consumers[0] {
				consumers = append(consumers, receiver)
			}

			clk.Advance(9 * time.Second)
			message = receivePumped(t, clk, c, consumerB, "b's delayed message")
			require.Equal(t, []byte("b"), message.Body)
			require.NoError(t, message.Settle.Ack(ctx))

			close(clk.stop)
			closeTest(t, ctx, raw, producer, append(consumers, consumerB)...)
		})
	}
}
