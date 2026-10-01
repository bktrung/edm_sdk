package kafka

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"sync"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

// gatedSender is a commitSender whose first request blocks until the test
// opens the gate, which holds a request in flight while other commits arrive.
type gatedSender struct {
	gate    chan struct{}
	started chan struct{}
	once    sync.Once

	mu     sync.Mutex
	rounds []map[partitionKey]int64
	fail   map[partitionKey]error
}

func newGatedSender() *gatedSender {
	return &gatedSender{gate: make(chan struct{}), started: make(chan struct{})}
}

func (g *gatedSender) send(ctx context.Context, points map[partitionKey]int64) map[partitionKey]error {
	g.mu.Lock()
	g.rounds = append(g.rounds, maps.Clone(points))
	first := len(g.rounds) == 1
	g.mu.Unlock()
	results := make(map[partitionKey]error, len(points))
	if first {
		g.once.Do(func() { close(g.started) })
		select {
		case <-g.gate:
		case <-ctx.Done():
			for key := range points {
				results[key] = ctx.Err()
			}
			return results
		}
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	for key := range points {
		results[key] = g.fail[key]
	}
	return results
}

func (g *gatedSender) sent() []map[partitionKey]int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]map[partitionKey]int64(nil), g.rounds...)
}

// waitRound waits until the committer's collecting round satisfies ready. The
// committer runs in other goroutines, so the state is polled, bounded by a real
// deadline.
func waitRound(t *testing.T, o *offsetCommitter, what string, ready func(points map[partitionKey]int64) bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for {
		o.mu.Lock()
		var points map[partitionKey]int64
		if o.round != nil {
			points = maps.Clone(o.round.points)
		}
		o.mu.Unlock()
		if ready(points) {
			return
		}
		if err := clock.NewReal().Sleep(ctx, time.Millisecond); err != nil {
			t.Fatalf("collecting round = %v, want %s", points, what)
		}
	}
}

// waitQueued waits until the committer's collecting round holds want
// partitions.
func waitQueued(t *testing.T, o *offsetCommitter, want int) {
	t.Helper()
	waitRound(t, o, fmt.Sprintf("%d partitions", want), func(points map[partitionKey]int64) bool {
		return len(points) == want
	})
}

type commitResult struct {
	key partitionKey
	err error
}

func commitAsync(o *offsetCommitter, ctx context.Context, key partitionKey, point int64, send commitSender, results chan<- commitResult) {
	go func() {
		results <- commitResult{key: key, err: o.commit(ctx, key, point, send)}
	}()
}

// TestCommitterBatchesCommitsArrivingDuringAFlight proves the commits that
// arrive while a request is in flight go out together in the next request, one
// point per partition and the highest one, and that each caller returns only
// after the request carrying its point.
func TestCommitterBatchesCommitsArrivingDuringAFlight(t *testing.T) {
	var o offsetCommitter
	sender := newGatedSender()
	first := partitionKey{destination: "topic", partition: 0}
	second := partitionKey{destination: "topic", partition: 1}
	third := partitionKey{destination: "other", partition: 0}
	results := make(chan commitResult, 4)
	// A round nobody sends leaves its callers waiting, so the bound turns that
	// defect into a failure rather than a hung test.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	commitAsync(&o, ctx, first, 10, sender.send, results)
	<-sender.started
	commitAsync(&o, ctx, second, 5, sender.send, results)
	commitAsync(&o, ctx, third, 7, sender.send, results)
	waitQueued(t, &o, 2)
	commitAsync(&o, ctx, second, 6, sender.send, results)
	waitRound(t, &o, "the higher point of the second partition", func(points map[partitionKey]int64) bool {
		return points[second] == 6
	})
	close(sender.gate)

	for range 4 {
		if result := <-results; result.err != nil {
			t.Fatalf("commit of %v = %v, want nil", result.key, result.err)
		}
	}
	rounds := sender.sent()
	if len(rounds) != 2 {
		t.Fatalf("sent %d requests for four commits, want 2: %v", len(rounds), rounds)
	}
	want := map[partitionKey]int64{second: 6, third: 7}
	if !maps.Equal(rounds[1], want) {
		t.Fatalf("second request = %v, want %v", rounds[1], want)
	}
}

// TestCommitterReportsEachPartitionItsOwnResult proves a partition the broker
// refused fails only its own commit in a shared request.
func TestCommitterReportsEachPartitionItsOwnResult(t *testing.T) {
	var o offsetCommitter
	sender := newGatedSender()
	refused := partitionKey{destination: "topic", partition: 1}
	accepted := partitionKey{destination: "topic", partition: 2}
	errRefused := errors.New("refused")
	sender.fail = map[partitionKey]error{refused: errRefused}
	results := make(chan commitResult, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	commitAsync(&o, ctx, partitionKey{destination: "topic", partition: 0}, 1, sender.send, results)
	<-sender.started
	commitAsync(&o, ctx, refused, 1, sender.send, results)
	commitAsync(&o, ctx, accepted, 1, sender.send, results)
	waitQueued(t, &o, 2)
	close(sender.gate)

	for range 3 {
		result := <-results
		switch result.key {
		case refused:
			if !errors.Is(result.err, errRefused) {
				t.Fatalf("refused partition's commit = %v, want %v", result.err, errRefused)
			}
		default:
			if result.err != nil {
				t.Fatalf("commit of %v = %v, want nil", result.key, result.err)
			}
		}
	}
}

// TestCommitterSenderCancellationDoesNotFailOthers proves a sender whose own
// context ends mid-request does not hand its cancellation to the other callers
// of the round: they read the round as abandoned, join the next one, and their
// commit succeeds.
func TestCommitterSenderCancellationDoesNotFailOthers(t *testing.T) {
	var o offsetCommitter
	sender := newGatedSender()
	follower := partitionKey{destination: "topic", partition: 1}

	// A request is in flight, so the follower joins a round and waits for it.
	o.mu.Lock()
	o.sending = true
	o.mu.Unlock()
	followerDone := make(chan error, 1)
	go func() {
		followerDone <- o.commit(context.Background(), follower, 4, sender.send)
	}()
	waitQueued(t, &o, 1)

	// The flight ends and the round is claimed by a caller whose context is
	// already over, so its request fails with that caller's cancellation.
	o.mu.Lock()
	round := o.round
	o.sending = false
	o.mu.Unlock()
	if !o.claim(round) {
		t.Fatal("claim of the waiting round failed")
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	o.send(cancelled, round, sender.send)

	if err := <-followerDone; err != nil {
		t.Fatalf("follower's commit = %v, want nil after joining the next round", err)
	}
	rounds := sender.sent()
	if len(rounds) != 2 || rounds[1][follower] != 4 {
		t.Fatalf("requests = %v, want the abandoned round and a retry carrying the follower's point", rounds)
	}
}
