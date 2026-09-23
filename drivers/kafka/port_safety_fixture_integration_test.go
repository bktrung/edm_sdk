//go:build integration

package kafka_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/drivers/kafka"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

var portKeyProbe struct {
	mu     sync.Mutex
	probed bool
	keys   []string
	err    error
}

// portDiscoverPartitionKeys publishes keyed candidates until every partition has
// answered with at least one of them.
func portDiscoverPartitionKeys(t *testing.T) ([]string, error) {
	fixture := newPortFixture(t, "key-probe", portPartitions)
	clk := fixture.clock
	consumer := fixture.subscribe(t, fixture.consumerConfig(fixture.group, portKeyProbeBatch))

	byPartition := make(map[int32]string, fixture.partitions)
	for round := range portKeyProbeRounds {
		sequences := make([]string, 0, portKeyProbeBatch)
		keys := make([]string, 0, portKeyProbeBatch)
		for index := range portKeyProbeBatch {
			key := fmt.Sprintf("port-probe-key-%d-%d", round, index)
			keys = append(keys, key)
			sequences = append(sequences, key)
		}
		fixture.publish(t, sequences, keys)

		timer := clk.Timer(portDeliveryTimeout)
		roundOver := false
		for !roundOver && len(byPartition) < fixture.partitions {
			select {
			case message, ok := <-consumer.Messages():
				if !ok {
					timer.Stop()
					return nil, errors.New("the probe consumer closed before every partition answered")
				}
				if _, seen := byPartition[message.Ref.Partition]; !seen {
					byPartition[message.Ref.Partition] = string(message.Key)
				}
				if err := message.Settle.Ack(fixture.ctx); err != nil {
					timer.Stop()
					return nil, fmt.Errorf("settle probe message on partition %d: %w", message.Ref.Partition, err)
				}
			case <-timer.C:
				// The round is over; another round publishes more candidates.
				roundOver = true
			}
		}
		timer.Stop()
		if len(byPartition) == fixture.partitions {
			break
		}
	}

	keys := make([]string, 0, fixture.partitions)
	for partition := range fixture.partitions {
		key, answered := byPartition[int32(partition)]
		if !answered {
			return nil, fmt.Errorf("no candidate key reached partition %d of %d", partition, fixture.partitions)
		}
		keys = append(keys, key)
	}
	return keys, nil
}

// portKeysByPartition returns one key per partition of a destination created
// with portPartitions partitions.
//
// Kafka hashes a key with murmur2 and takes it modulo the partition count, so
// the mapping from a key to a partition is a property of the key and the
// partition count rather than of any one destination, and it is discovered once
// for the package. The probe publishes candidate keys, reads the partition each
// delivery carries in its broker reference, and reports a key for every
// partition it reached. Nothing internal is read: the partition of a delivery is
// part of the port's message.
func portKeysByPartition(t *testing.T) []string {
	t.Helper()
	portKeyProbe.mu.Lock()
	defer portKeyProbe.mu.Unlock()
	if !portKeyProbe.probed {
		// The flag is set before the probe runs, not after: the probe fails
		// through t.Fatalf, which ends its goroutine, so a flag set afterwards
		// would leave the next caller probing again and a sync.Once would leave
		// it holding no keys at all. Setting it first turns that into the
		// length check below, which fails the next caller with a reason.
		portKeyProbe.probed = true
		portKeyProbe.keys, portKeyProbe.err = portDiscoverPartitionKeys(t)
	}
	if portKeyProbe.err != nil {
		t.Fatalf("discover one key per partition: %v", portKeyProbe.err)
	}
	if len(portKeyProbe.keys) < portPartitions {
		t.Fatalf("the partition key probe reported %d keys, want %d: a key is needed for every partition this file publishes to",
			len(portKeyProbe.keys), portPartitions)
	}
	return portKeyProbe.keys
}

// portUniqueName returns a destination or group name no other run shares, so a
// repeated run starts from an empty destination and an unknown group.
func portUniqueName(t *testing.T, label string) string {
	t.Helper()
	trailing := make([]byte, 6)
	if _, err := rand.Read(trailing); err != nil {
		t.Fatalf("generate a unique name: %v", err)
	}
	return fmt.Sprintf("f1-%s-%s-%s", label, strings.ReplaceAll(t.Name(), "/", "-"), hex.EncodeToString(trailing))
}

// portEndpoint reports the Kafka fixture endpoint the environment selects.
func portEndpoint() string {
	if endpoint := os.Getenv(portEndpointEnv); endpoint != "" {
		return endpoint
	}
	return portDefaultEndpoint
}

var portProbe struct {
	once   sync.Once
	reason string
}

// requirePortBroker probes the Kafka fixture once per package and fails the
// calling test when it is unreachable. There is no skip branch: these tests live
// in an integration file, so the honest outcomes for them are a pass and a
// failure that names the missing fixture.
func requirePortBroker(t *testing.T) {
	t.Helper()
	portProbe.once.Do(func() {
		// This runs before any fixture exists, so there is no clock to take and
		// nothing measured here: the dial only answers whether the fixture is up.
		conn, err := net.DialTimeout("tcp", portEndpoint(), 2*time.Second)
		if err != nil {
			portProbe.reason = err.Error()
			return
		}
		_ = conn.Close()
	})
	if portProbe.reason != "" {
		t.Fatalf("Kafka fixture unreachable at %s (%s); start it with `make kafka-up`", portEndpoint(), portProbe.reason)
	}
}

// portSequenceOf reads the sequence number a delivery carries. The corpus body
// is the sequence number, so a settlement is attributed to the message the
// broker redelivered rather than to a delivery the test remembered.
func portSequenceOf(message driver.InboundMessage) string {
	return string(message.Body)
}

// portSequences returns the numbered corpus a test publishes.
func portSequences(count int) []string {
	sequences := make([]string, 0, count)
	for index := range count {
		sequences = append(sequences, fmt.Sprintf("seq-%d", index))
	}
	return sequences
}

// portKeyedSequences assigns one key per sequence, taking the keys in turn so
// the corpus is spread over every partition. An unkeyed burst lands on one
// partition, which would leave a rebalance with partitions that hold nothing.
func portKeyedSequences(sequences, keys []string) []string {
	keyed := make([]string, 0, len(sequences))
	for index := range sequences {
		keyed = append(keyed, keys[index%len(keys)])
	}
	return keyed
}

// portDistinctPartitions counts the partitions a set of deliveries covers.
func portDistinctPartitions(messages []driver.InboundMessage) int {
	seen := make(map[int32]struct{}, len(messages))
	for _, message := range messages {
		seen[message.Ref.Partition] = struct{}{}
	}
	return len(seen)
}

// sortedPartitions renders a partition set in a fixed order, so a failure that
// quotes one reads the same on every run.
// portSettlementRevoked reports whether a settlement failed because the
// delivery's ownership moved in a rebalance.
//
// The port has no sentinel for that case: an ownership move is a driver fact
// and driver/errors.go carries no portable error for it, so the driver's own
// exported sentinel is the only level signal. A settlement refused for any
// other reason is a failure and is reported as one, which is the direction a
// rewrite should fail in: an unrecognised refusal is a finding.
func portSettlementRevoked(err error) bool {
	return errors.Is(err, kafka.ErrRevoked)
}

// portSettleRemaining settles the deliveries a test has already taken and every
// delivery the consumer had already admitted, so the consumer can be stopped. It
// is called after Drain, which is what guarantees that the channel holds the
// whole outstanding set and that nothing arrives while this runs.
func portSettleRemaining(ctx context.Context, ledger *portLedger, subscriber *portSubscriber, messages []driver.InboundMessage) {
	for _, message := range messages {
		ledger.acknowledge(ctx, message)
	}
	for {
		select {
		case message, ok := <-subscriber.Messages():
			if !ok {
				return
			}
			ledger.received(portSequenceOf(message))
			ledger.acknowledge(ctx, message)
		default:
			return
		}
	}
}

// portAwaitSettlements blocks until at least count sequences have settled.
func portAwaitSettlements(t *testing.T, ledger *portLedger, count int, sequences []string, what string) {
	t.Helper()
	timer := ledger.clock.Timer(portSettleTimeout)
	defer timer.Stop()
	for {
		if ledger.settledCount() >= count {
			return
		}
		select {
		case <-ledger.progress:
		case <-timer.C:
			t.Fatalf("%s: only %d of %d settled within %s: %s",
				what, ledger.settledCount(), count, portSettleTimeout, ledger.settleSummary(sequences))
		}
	}
}

// portAwaitCorpus waits until every sequence has settled, or until no further
// settlement has arrived for window. It returns rather than failing in the
// second case, so the caller's loss assertion reports the sequences that never
// settled; it fails only when the whole bound passes without enough
// settlements to reach either condition.
func portAwaitCorpus(t *testing.T, ledger *portLedger, count int, sequences []string, window, bound time.Duration, what string) {
	t.Helper()
	deadline := ledger.clock.Timer(bound)
	defer deadline.Stop()
	quiet := ledger.clock.Timer(window)
	for {
		if ledger.settledCount() >= count {
			quiet.Stop()
			return
		}
		select {
		case <-ledger.progress:
			// Every settlement restarts the window.
			quiet.Stop()
			quiet = ledger.clock.Timer(window)
		case <-quiet.C:
			return
		case <-deadline.C:
			t.Fatalf("%s: only %d of %d settled within %s: %s",
				what, ledger.settledCount(), count, bound, ledger.settleSummary(sequences))
		}
	}
}

// portAssertNoLoss fails when any sequence the test published never settled.
// This is the assertion that separates at-least-once from a delivery count: it
// reads settlements only.
func portAssertNoLoss(t *testing.T, ledger *portLedger, sequences []string) {
	t.Helper()
	lost := ledger.unsettled(sequences)
	if len(lost) > 0 {
		t.Fatalf("lost %d of %d messages: %v (the revoke refused a settlement for %d of them because the delivery's ownership moved)",
			len(lost), len(sequences), lost, len(ledger.revokedSequences()))
	}
}

// portAwaitDelivery waits for one delivery, or fails naming what it waited for
// and what the consumer reported instead.
func portAwaitDelivery(t *testing.T, clk clock.Clock, subscriber *portSubscriber, timeout time.Duration, what string) driver.InboundMessage {
	t.Helper()
	timer := clk.Timer(timeout)
	defer timer.Stop()
	select {
	case message, ok := <-subscriber.Messages():
		if !ok {
			t.Fatalf("%s: Messages channel closed%s", what, subscriber.notifications.failureSummary())
		}
		return message
	case <-timer.C:
		t.Fatalf("%s: no delivery within %s%s", what, timeout, subscriber.notifications.failureSummary())
		return driver.InboundMessage{}
	}
}

// portCollect waits up to window for one delivery and reports whether one
// arrived, so a test that is counting deliveries can report a short count
// rather than a timeout. A consumer that closed its channel while a count was
// being taken is a different failure and fails here. The first delivery of a
// test is awaited with portAwaitDelivery instead, so a destination that never
// delivers at all is told apart from one that delivers fewer than expected.
func portCollect(t *testing.T, clk clock.Clock, subscriber *portSubscriber, window time.Duration) (driver.InboundMessage, bool) {
	t.Helper()
	timer := clk.Timer(window)
	defer timer.Stop()
	select {
	case message, ok := <-subscriber.Messages():
		if !ok {
			t.Fatalf("Messages channel closed while counting deliveries%s", subscriber.notifications.failureSummary())
		}
		return message, true
	case <-timer.C:
		return driver.InboundMessage{}, false
	}
}

// portAwaitQuiet waits out the quiet window and reports a delivery that arrived
// during it, or nil when none did. It is the one wait in this file in which
// absence is the observation.
func portAwaitQuiet(t *testing.T, clk clock.Clock, subscriber *portSubscriber, window time.Duration) *driver.InboundMessage {
	t.Helper()
	timer := clk.Timer(window)
	defer timer.Stop()
	select {
	case message, ok := <-subscriber.Messages():
		if !ok {
			t.Fatalf("Messages channel closed during the quiet window%s", subscriber.notifications.failureSummary())
		}
		return &message
	case <-timer.C:
		return nil
	}
}

// portAwait waits for one value on ch until timeout, or fails naming what it
// was waiting for. Every wait in this file goes through it or through a
// consumer channel, so nothing here polls.
func portAwait[T any](t *testing.T, clk clock.Clock, ch <-chan T, timeout time.Duration, what string) T {
	t.Helper()
	timer := clk.Timer(timeout)
	defer timer.Stop()
	select {
	case value := <-ch:
		return value
	case <-timer.C:
		var zero T
		t.Fatalf("%s: nothing arrived within %s", what, timeout)
		return zero
	}
}

// unsettled lists the sequences that never settled.
func (l *portLedger) unsettled(sequences []string) []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	var missing []string
	for _, sequence := range sequences {
		if _, settled := l.settledAt[sequence]; !settled {
			missing = append(missing, sequence)
		}
	}
	return missing
}

// revokedSequences lists, in order, the sequences whose settlement the driver
// refused because the delivery's ownership had moved. Each of them is a
// delivery a rebalance has to hand back: it was delivered, this consumer could
// not settle it, and only its new owner can.
func (l *portLedger) revokedSequences() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	sequences := make([]string, 0, len(l.revoked))
	for sequence := range l.revoked {
		sequences = append(sequences, sequence)
	}
	sort.Strings(sequences)
	return sequences
}

func (l *portLedger) assertNoFailures(t *testing.T) {
	t.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.failed) > 0 {
		t.Fatalf("settlement failures: %v", l.failed)
	}
}

// metrics prints the fixed, greppable line later runs of this file compare
// against. The
// field order and spelling are part of this test's contract.
//
// lost counts sequence numbers the broker never accepted a settlement for, so
// it is computed from settlements alone. dups counts deliveries beyond the first
// for a sequence number, which is the redundancy a consumer observes rather
// than a second settlement the ledger happened to record.
func (l *portLedger) metrics(t *testing.T, name string, sequences []string, firstDelivery time.Duration) {
	t.Helper()
	duplicates := 0
	l.mu.Lock()
	for _, count := range l.receivedAt {
		if count > 1 {
			duplicates += count - 1
		}
	}
	l.mu.Unlock()
	t.Logf("port-metrics test=%s lost=%d dups=%d first-delivery-ms=%d",
		name, len(l.unsettled(sequences)), duplicates, firstDelivery.Milliseconds())
}

// settleSummary describes what a settlement wait is still missing, per
// sequence: how often the driver delivered it, and what became of the
// settlements it did have. One count for the whole set cannot tell the causes
// apart, and they mean different things. A sequence delivered once whose
// settlement was refused is a delivery a rebalance never handed back, which is
// the loss these tests exist to detect. A sequence delivered again whose
// settlement failed for another reason came back and this consumer could not
// settle it. A sequence never delivered at all never arrived.
func (l *portLedger) settleSummary(sequences []string) string {
	l.mu.Lock()
	var items []string
	for _, sequence := range sequences {
		if _, settled := l.settledAt[sequence]; settled {
			continue
		}
		item := fmt.Sprintf("%s delivered %dx", sequence, l.receivedAt[sequence])
		if l.revoked[sequence] {
			item += ", its settlement was refused as revoked"
		}
		var failures []portSettlementFailure
		for _, failure := range l.failed {
			if failure.sequence == sequence {
				failures = append(failures, failure)
			}
		}
		if len(failures) > 0 {
			shown, more := failures, ""
			if len(shown) > 3 {
				more = fmt.Sprintf(" (+%d more)", len(shown)-3)
				shown = shown[:3]
			}
			texts := make([]string, 0, len(shown))
			for _, failure := range shown {
				texts = append(texts, failure.String())
			}
			item += fmt.Sprintf(", settlement failures: %s%s", strings.Join(texts, "; "), more)
		}
		items = append(items, item)
	}
	unsettled := len(items)
	refused, failures := len(l.revoked), len(l.failed)
	l.mu.Unlock()
	if unsettled == 0 {
		return "nothing unsettled"
	}
	note := ""
	if len(items) > 6 {
		note = fmt.Sprintf(" and %d more", len(items)-6)
		items = items[:6]
	}
	return fmt.Sprintf("%d of %d unsettled [%s%s]; sequences refused as revoked: %d, settlements failed: %d",
		unsettled, len(sequences), strings.Join(items, "; "), note, refused, failures)
}

// portSettlementFailure is one settlement the driver rejected for a reason this
// file does not expect, kept beside the sequence it belongs to so a wait that
// timed out can say which delivery failed and how, rather than only how many
// failures there were.
type portSettlementFailure struct {
	sequence string
	err      error
}

// String renders the failure the way a test message quotes it.
func (f portSettlementFailure) String() string {
	return fmt.Sprintf("settle %s: %v", f.sequence, f.err)
}

// portLedger counts deliveries and settlements per sequence number.
//
// Deliveries and settlements are counted apart and are never conflated: a
// delivery is what the consumer received and a settlement is what the broker
// accepted, and only the second one means the message will not come back. A
// loss count derived from deliveries reports a message as handled while the
// broker still owes it, which is the whole class of defect these tests exist to
// catch.
type portLedger struct {
	clock      clock.Clock
	mu         sync.Mutex
	receivedAt map[string]int
	settledAt  map[string]time.Time
	// revoked holds one entry per sequence whose settlement the driver refused
	// because the delivery's ownership moved, so its length counts sequences
	// rather than refusals, and a sequence refused twice still counts once.
	revoked  map[string]bool
	failed   []portSettlementFailure
	progress chan struct{}
}

func newPortLedger(clk clock.Clock) *portLedger {
	return &portLedger{
		clock:      clk,
		receivedAt: make(map[string]int),
		settledAt:  make(map[string]time.Time),
		revoked:    make(map[string]bool),
		progress:   make(chan struct{}, 1),
	}
}

// received records one delivery.
func (l *portLedger) received(sequence string) {
	l.mu.Lock()
	l.receivedAt[sequence]++
	l.mu.Unlock()
}

// acknowledge settles one delivery and records the outcome.
//
// The settlement is bounded, so a broker that stops answering becomes a
// recorded failure rather than a handler blocked until the package deadline.
// A settlement that fails because a rebalance moved the delivery's ownership is
// expected here and is counted as a revocation, not a failure: the message is
// delivered again to its new owner and only the settlement there counts. Any
// other failure is kept and reported, because a settlement error this file does
// not understand is a finding either way.
func (l *portLedger) acknowledge(ctx context.Context, message driver.InboundMessage) {
	sequence := portSequenceOf(message)
	settleCtx, cancel := context.WithTimeout(ctx, portSettleTimeout)
	defer cancel()
	err := message.Settle.Ack(settleCtx)
	switch {
	case err == nil:
		l.mu.Lock()
		if _, before := l.settledAt[sequence]; !before {
			l.settledAt[sequence] = l.clock.Now()
		}
		l.mu.Unlock()
		select {
		case l.progress <- struct{}{}:
		default:
		}
	case portSettlementRevoked(err):
		l.mu.Lock()
		l.revoked[sequence] = true
		l.mu.Unlock()
	default:
		l.mu.Lock()
		l.failed = append(l.failed, portSettlementFailure{sequence: sequence, err: err})
		l.mu.Unlock()
	}
}

func (l *portLedger) countReceived(sequence string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.receivedAt[sequence]
}

// firstSettlement reports when a sequence first settled, and whether it ever
// did.
func (l *portLedger) firstSettlement(sequence string) (time.Time, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	at, settled := l.settledAt[sequence]
	return at, settled
}

// settledCount reports how many sequences have settled at least once.
func (l *portLedger) settledCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.settledAt)
}

// assertNoFatal fails when the consumer published an error that is neither a
// rebalance lifecycle event nor a transient failure. Such an error on the way
// through a corpus is a finding even when the corpus settled: the port reports
// it, and a test that read only the deliveries would never say so.
func (n *portNotifications) assertNoFatal(t *testing.T) {
	t.Helper()
	n.mu.Lock()
	defer n.mu.Unlock()
	var fatal []error
	for _, err := range n.failed {
		if kind, classified := driver.Classify(err); classified &&
			kind != driver.KindTransient && kind != driver.KindNotification {
			fatal = append(fatal, err)
		}
	}
	if len(fatal) > 0 {
		t.Fatalf("the consumer published %d failures that are neither rebalance events nor transient: %v", len(fatal), fatal)
	}
}

// failureSummary names the errors the consumer published that were not
// rebalance notifications, so a deadline failure says what the driver did
// report instead.
func (n *portNotifications) failureSummary() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	if len(n.failed) == 0 {
		return ""
	}
	return fmt.Sprintf(" (consumer errors: %v)", n.failed)
}

// rebalanceEvent names the lifecycle event one driver error reports, or the
// empty string for anything else.
func rebalanceEvent(err error) string {
	kind, classified := driver.Classify(err)
	if !classified || kind != driver.KindNotification {
		return ""
	}
	switch text := err.Error(); {
	case strings.Contains(text, "partitions assigned"):
		return "assigned"
	case strings.Contains(text, "partitions revoked"):
		return "revoked"
	case strings.Contains(text, "partitions lost"):
		return "lost"
	default:
		return ""
	}
}

// portNotifications records the rebalance notifications a consumer publishes on
// Errors().
//
// The port carries failures and routine lifecycle events on one channel and has
// no typed rebalance event, so an event is recognised by the classification the
// port gives it and by the event name its text carries. That text is the only
// port-level signal a rebalance produces; the tests below assert on deliveries
// and settlements, never on the text, with one exception: the partitions a
// revoke names are the only observable of which partitions a rebalance took
// away from a member, and revokedPartitions reads them out of it.
type portNotifications struct {
	clock     clock.Clock
	stopC     chan struct{}
	done      chan struct{}
	once      sync.Once
	mu        sync.Mutex
	failed    []error
	givenUp   []error
	revokedAt chan time.Time
	assigns   chan time.Time
}

// The port carries no timestamp on a notification, so the instant the recorder
// reads it off the error channel is the closest thing to one it offers, and the
// first-delivery-ms metric is measured from there.

// watchRebalanceNotifications starts recording the notifications one consumer
// publishes. The recorder stops when the consumer's error channel closes or when
// the test calls stop.
func watchRebalanceNotifications(clk clock.Clock, consumer driver.Consumer) *portNotifications {
	notifications := &portNotifications{
		clock:     clk,
		stopC:     make(chan struct{}),
		done:      make(chan struct{}),
		revokedAt: make(chan time.Time, 1),
		assigns:   make(chan time.Time, 1),
	}
	go func() {
		defer close(notifications.done)
		for {
			select {
			case <-notifications.stopC:
				return
			case err, ok := <-consumer.Errors():
				if !ok {
					return
				}
				notifications.record(err)
			}
		}
	}()
	return notifications
}

func (n *portNotifications) record(err error) {
	switch rebalanceEvent(err) {
	case "assigned":
		select {
		case n.assigns <- n.clock.Now():
		default:
		}
	case "revoked", "lost":
		// Rebalance lifecycle the tests do not wait on. It is still a routine
		// event rather than a failure: recording it as one would put it in
		// every failureSummary this file prints. It is kept, separately, because
		// the partitions it names are what a revoke took away from this member.
		at := n.clock.Now()
		n.mu.Lock()
		n.givenUp = append(n.givenUp, err)
		n.mu.Unlock()
		select {
		case n.revokedAt <- at:
		default:
		}
	default:
		n.mu.Lock()
		n.failed = append(n.failed, err)
		n.mu.Unlock()
	}
}

func (n *portNotifications) stop() {
	n.once.Do(func() { close(n.stopC) })
	<-n.done
}

// awaitAssignment waits for a partition assignment notification and returns the
// instant it arrived.
func (n *portNotifications) awaitAssignment(t *testing.T, timeout time.Duration, what string) time.Time {
	t.Helper()
	timer := n.clock.Timer(timeout)
	defer timer.Stop()
	select {
	case assigned := <-n.assigns:
		return assigned
	case <-timer.C:
		t.Fatalf("%s: no partition assignment within %s%s", what, timeout, n.failureSummary())
		return time.Time{}
	}
}

// awaitRevocation waits for the first revoke or lost notification and returns
// the instant the recorder observed it.
func (n *portNotifications) awaitRevocation(t *testing.T, timeout time.Duration, what string) time.Time {
	t.Helper()
	timer := n.clock.Timer(timeout)
	defer timer.Stop()
	select {
	case revoked := <-n.revokedAt:
		return revoked
	case <-timer.C:
		t.Fatalf("%s: no revoke notification within %s%s", what, timeout, n.failureSummary())
		return time.Time{}
	}
}

// parseRevokedPartitions reads the partition lists in one lifecycle error.
// The error formats a map whose outer brackets contain a topic and whose inner
// brackets contain its partition slice, so searching for ":[", rather than the
// first "[", is what keeps partition zero in the result.
func parseRevokedPartitions(text string) map[int32]bool {
	given := make(map[int32]bool)
	for {
		start := strings.Index(text, ":[")
		if start < 0 {
			break
		}
		rest := text[start+2:]
		end := strings.Index(rest, "]")
		if end < 0 {
			break
		}
		for _, field := range strings.Fields(rest[:end]) {
			partition, convErr := strconv.ParseInt(field, 10, 32)
			if convErr != nil {
				continue
			}
			given[int32(partition)] = true
		}
		text = rest[end+1:]
	}
	return given
}

// revokedPartitions lists the partitions the driver has told this consumer to
// give up, read from the lifecycle notifications it published. Which partitions
// a revoke names is the port-level fact a cooperative protocol turns on: it
// names the partitions that moved, and a partition this member keeps is not
// among them, because the group never took it away.
func (n *portNotifications) revokedPartitions() map[int32]bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	given := make(map[int32]bool)
	for _, err := range n.givenUp {
		for partition := range parseRevokedPartitions(err.Error()) {
			given[partition] = true
		}
	}
	return given
}

// portPace is the pause a consumer takes before it settles a delivery, which a
// test shortens or closes once the window the pause exists for has passed.
//
// Pacing a consumer is how a test keeps it mid-stream while a peer joins and
// leaves. Closing the pace afterwards is what lets the corpus finish promptly:
// a fixed pause long enough to cover a join would make the rest of the corpus
// take minutes.
type portPace struct {
	mu   sync.Mutex
	hold time.Duration
}

func newPortPace(hold time.Duration) *portPace {
	return &portPace{hold: hold}
}

// wait pauses for the current hold, or returns the context's error when the
// test is over.
func (p *portPace) wait(ctx context.Context, clk clock.Clock) error {
	p.mu.Lock()
	hold := p.hold
	p.mu.Unlock()
	if hold <= 0 {
		return ctx.Err()
	}
	return clk.Sleep(ctx, hold)
}

// close ends the pause, so the consumer settles the rest of the corpus at the
// speed the broker allows.
func (p *portPace) close() {
	p.mu.Lock()
	p.hold = 0
	p.mu.Unlock()
}

// portHoldings is the queue of deliveries one consumer took and did not settle,
// each timestamped when it arrived.
//
// A test that measures the gap from an assignment to the first delivery needs
// the arrival instant rather than the instant the test got round to reading the
// channel, and a test that abandons deliveries at a leave needs the deliveries
// themselves. Both come from the same queue, filled by the consumer's pump.
type portHoldings struct {
	mu      sync.Mutex
	entries []portHolding
	notify  chan struct{}
}

// portHolding is one delivery a consumer took and did not settle.
type portHolding struct {
	sequence string
	at       time.Time
	message  driver.InboundMessage
}

func newPortHoldings() *portHoldings {
	return &portHoldings{notify: make(chan struct{}, 1)}
}

func (h *portHoldings) take(sequence string, at time.Time, message driver.InboundMessage) {
	h.mu.Lock()
	h.entries = append(h.entries, portHolding{sequence: sequence, at: at, message: message})
	h.mu.Unlock()
	select {
	case h.notify <- struct{}{}:
	default:
	}
}

// all returns the deliveries taken so far, in arrival order.
func (h *portHoldings) all() []portHolding {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]portHolding(nil), h.entries...)
}

func (h *portHoldings) size() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.entries)
}

// portAwaitHoldings blocks until at least count deliveries have been taken.
func portAwaitHoldings(t *testing.T, clk clock.Clock, holdings *portHoldings, count int, what string) {
	t.Helper()
	timer := clk.Timer(portAssignmentTimeout)
	defer timer.Stop()
	for {
		if holdings.size() >= count {
			return
		}
		select {
		case <-holdings.notify:
		case <-timer.C:
			t.Fatalf("%s: only %d of %d deliveries were taken within %s",
				what, holdings.size(), count, portAssignmentTimeout)
		}
	}
}

// portSubscriber is one consumer a test drives, with the rebalance
// notifications it publishes recorded as they arrive.
type portSubscriber struct {
	driver.Consumer
	notifications *portNotifications
}

// pump drains the consumer in the background and hands each delivery to handle.
// stop waits for the goroutine, so no delivery is handled after the test has
// finished with the consumer. handle runs on the pump goroutine and must not
// fail the test; it records what it saw and the test asserts afterwards.
func (s *portSubscriber) pump(handle func(driver.InboundMessage)) *portPump {
	pump := &portPump{stopC: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(pump.done)
		for {
			select {
			case <-pump.stopC:
				return
			case message, ok := <-s.Messages():
				if !ok {
					return
				}
				handle(message)
			}
		}
	}()
	return pump
}

// portPump is a background delivery loop that can be stopped and waited for.
type portPump struct {
	stopC chan struct{}
	done  chan struct{}
	once  sync.Once
}

func (p *portPump) stop() {
	p.once.Do(func() { close(p.stopC) })
	<-p.done
}

// portFixture is the broker furniture one test needs: one connection, one
// producer, one destination created with a known partition count, and a broker
// client for the fixture facts the port does not express. Those are how many
// partitions the destination got and whether every one of them has a live
// leader, which TopologyState does not carry (it reports message depth only),
// and how the test disposes of the destination and the group afterwards: the
// port's only deletion verb is Maintenance.Prune, which refuses a destination
// that still holds messages or has a consumer attached, and it has no group
// verb at all. None of this is an assertion: every behaviour these tests judge
// is read back through the port.
type portFixture struct {
	ctx        context.Context
	clock      clock.Clock
	conn       driver.Conn
	producer   driver.Producer
	admin      *kadm.Client
	topic      string
	partitions int
	group      string
}

// newPortFixture creates the destination, the producer and the groups one test
// owns, and registers their teardown in the order they have to come down in.
// partitions is the destination's partition count.
func newPortFixture(t *testing.T, label string, partitions int) *portFixture {
	t.Helper()
	return newPortFixtureWithLogger(t, label, partitions, nil)
}

// newPortFixtureWithLogger is newPortFixture with a logger for the connection,
// which a test that reads a driver diagnostic needs: the port carries failures
// and routine lifecycle events on Errors(), and a condition the driver survives
// is written to the logger the connection was built with instead.
func newPortFixtureWithLogger(t *testing.T, label string, partitions int, logger *slog.Logger) *portFixture {
	t.Helper()
	requirePortBroker(t)

	ctx := t.Context()
	client, err := kgo.NewClient(kgo.SeedBrokers(portEndpoint()))
	if err != nil {
		t.Fatalf("kgo.NewClient(%s): %v", portEndpoint(), err)
	}
	admin := kadm.NewClient(client)
	t.Cleanup(client.Close)

	topic := portUniqueName(t, "port-"+label)
	t.Cleanup(func() {
		deleteCtx, cancel := context.WithTimeout(context.Background(), portCloseTimeout)
		defer cancel()
		responses, err := admin.DeleteTopics(deleteCtx, topic)
		if err != nil {
			t.Errorf("DeleteTopics %q: %v", topic, err)
			return
		}
		// A destination this fixture never got as far as creating is not a
		// teardown failure: the broker is already in the state the fixture
		// wanted. That is the common case on a failure path, where the real
		// cause is a line above and a second, invented one would sit beside it.
		if response, ok := responses[topic]; ok && response.Err != nil &&
			!errors.Is(response.Err, kerr.UnknownTopicOrPartition) {
			t.Errorf("DeleteTopics %q: %v", topic, response.Err)
		}
	})

	group := portUniqueName(t, "port-"+label)
	t.Cleanup(func() {
		deleteCtx, cancel := context.WithTimeout(context.Background(), portCloseTimeout)
		defer cancel()
		responses, err := admin.DeleteGroups(deleteCtx, group)
		if err != nil {
			t.Errorf("DeleteGroups %q: %v", group, err)
			return
		}
		// Same as the destination above: a group that never existed, because a
		// consumer never joined, is a teardown that has nothing left to do.
		if response, ok := responses[group]; ok && response.Err != nil &&
			!errors.Is(response.Err, kerr.GroupIDNotFound) {
			t.Errorf("DeleteGroups %q: %v", group, response.Err)
		}
	})

	connection, err := (kafka.Driver{}).Open(ctx, driver.Config{
		Endpoints:             []string{portEndpoint()},
		ClientID:              "f1-kafka-port-safety",
		RebalanceDrainTimeout: portDrainTimeout,
		Logger:                logger,
	})
	if err != nil {
		t.Fatalf("Open(%s): %v", portEndpoint(), err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), portCloseTimeout)
		defer cancel()
		if err := connection.Close(closeCtx); err != nil {
			t.Errorf("Close connection: %v", err)
		}
	})

	producer, err := connection.Producer(ctx, driver.ProducerConfig{
		Effective: connection.Capabilities(),
	})
	if err != nil {
		t.Fatalf("Producer(): %v", err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), portCloseTimeout)
		defer cancel()
		if err := producer.Close(closeCtx); err != nil {
			t.Errorf("Close producer: %v", err)
		}
	})

	if _, err := connection.Admin().EnsureTopology(ctx, driver.TopologySpec{
		Destinations: []driver.DestinationSpec{{Name: topic, Partitions: partitions, Durable: true}},
		Policy:       driver.TopologyDeclare,
		Effective:    connection.Capabilities(),
	}); err != nil {
		t.Fatalf("EnsureTopology(%q, %d partitions): %v", topic, partitions, err)
	}

	fixture := &portFixture{
		ctx:        ctx,
		clock:      clock.NewReal(),
		conn:       connection,
		producer:   producer,
		admin:      admin,
		topic:      topic,
		group:      group,
		partitions: partitions,
	}
	if got := fixture.awaitDestination(t); got != partitions {
		t.Fatalf("destination %s has %d partitions, want %d: the test reads the destination it asked for",
			topic, got, partitions)
	}
	return fixture
}

// awaitDestination waits until the broker reports the destination with a live
// leader for every partition, and returns its partition count.
//
// Creation returns before the metadata a consumer client fetches on assignment
// carries usable leaders. A client that fetches against that stale view waits
// for its next metadata refresh, which is seconds, and the delay lands inside
// whatever the test measures next. Waiting here keeps it out.
func (f *portFixture) awaitDestination(t *testing.T) int {
	t.Helper()
	ticker := f.clock.Ticker(portDestinationReadyPoll)
	defer ticker.Stop()
	timer := f.clock.Timer(portDestinationReadyTimeout)
	defer timer.Stop()
	var lastErr error
	for {
		partitions, ready, err := f.destinationState()
		if err != nil {
			lastErr = err
		}
		if ready {
			return partitions
		}
		select {
		case <-ticker.C:
		case <-timer.C:
			t.Fatalf("destination %s did not report a live leader for %d partitions within %s (last metadata error: %v)",
				f.topic, f.partitions, portDestinationReadyTimeout, lastErr)
		}
	}
}

// destinationState reports the destination's partition count and whether every
// one of those partitions has a live leader.
//
// The metadata call is bounded on its own: the poll loop's deadline only bounds
// the loop, and a broker that stops answering inside a call would hold the loop
// open past it. An error is returned rather than raised, so a slow answer reads
// as not ready yet and the loop's deadline reports it with the last error.
func (f *portFixture) destinationState() (int, bool, error) {
	callCtx, cancel := context.WithTimeout(f.ctx, portDestinationReadyTimeout)
	defer cancel()
	details, err := f.admin.ListTopics(callCtx, f.topic)
	if err != nil {
		return 0, false, err
	}
	detail, ok := details[f.topic]
	if !ok || detail.Err != nil {
		return 0, false, nil
	}
	for _, partition := range detail.Partitions {
		if partition.Err != nil || partition.Leader < 0 {
			return len(detail.Partitions), false, nil
		}
	}
	return len(detail.Partitions), len(detail.Partitions) > 0, nil
}

// committedOffsets reads this fixture group's offsets for its topic through
// Kafka. FetchOffsetsForTopics fills partitions without a commit with -1; a
// missing group is the other no-commit result and is treated the same way.
func (f *portFixture) committedOffsets() (kadm.OffsetResponses, error) {
	callCtx, cancel := context.WithTimeout(f.ctx, portDestinationReadyTimeout)
	defer cancel()
	offsets, err := f.admin.FetchOffsetsForTopics(callCtx, f.group, f.topic)
	if errors.Is(err, kerr.GroupIDNotFound) {
		return nil, nil
	}
	return offsets, err
}

// consumerConfig fills in the consumer configuration every test in this file
// shares. A test that needs a per-destination share overrides PerDestination.
//
// Every consumer starts at the earliest retained message. Each test owns a
// freshly created destination and a group nobody has used, so the earliest
// message is the head of that test's corpus, and a consumer that starts at the
// latest message instead would race the group's first fetch against the publish
// and could see an empty destination.
func (f *portFixture) consumerConfig(group string, prefetch int) driver.ConsumerConfig {
	return driver.ConsumerConfig{
		Group:        group,
		Destinations: []string{f.topic},
		Prefetch:     prefetch,
		StartAt:      driver.StartEarliest,
		Effective:    f.conn.Capabilities(),
	}
}

// subscribe opens one consumer on the fixture connection and registers its
// release. Release is idempotent, so a test that stopped the consumer itself
// still tears down cleanly, and one that failed early still hands its uncommitted
// deliveries back instead of leaving them committed by a Stop.
func (f *portFixture) subscribe(t *testing.T, config driver.ConsumerConfig) *portSubscriber {
	t.Helper()
	return f.subscribeWith(t, f.conn, config)
}

// subscribeWith is subscribe on a connection of the caller's choosing. A second
// member of the fixture's group has to come from a second connection when the
// test reads driver diagnostics: the logger belongs to the connection, so
// members sharing one would share one log.
func (f *portFixture) subscribeWith(t *testing.T, connection driver.Conn, config driver.ConsumerConfig) *portSubscriber {
	t.Helper()
	consumer, err := connection.Consumer(f.ctx, config)
	if err != nil {
		t.Fatalf("Consumer(%q): %v", config.Group, err)
	}
	subscriber := &portSubscriber{
		Consumer:      consumer,
		notifications: watchRebalanceNotifications(f.clock, consumer),
	}
	t.Cleanup(func() {
		subscriber.notifications.stop()
		releaseCtx, cancel := context.WithTimeout(context.Background(), portCloseTimeout)
		defer cancel()
		if err := consumer.Release(releaseCtx); err != nil {
			t.Errorf("Release consumer %q: %v", config.Group, err)
		}
	})
	return subscriber
}

// secondConnection opens another connection to the fixture's broker for a second
// member of the same group, with a logger of its own. It carries no instance
// identity, as the fixture's own connection does not, so the group sees two
// dynamic members rather than one static member joining twice.
func (f *portFixture) secondConnection(t *testing.T, logger *slog.Logger) driver.Conn {
	t.Helper()
	connection, err := (kafka.Driver{}).Open(f.ctx, driver.Config{
		Endpoints:             []string{portEndpoint()},
		ClientID:              "f1-kafka-port-safety",
		RebalanceDrainTimeout: portDrainTimeout,
		Logger:                logger,
	})
	if err != nil {
		t.Fatalf("Open(%s): %v", portEndpoint(), err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), portCloseTimeout)
		defer cancel()
		if err := connection.Close(closeCtx); err != nil {
			t.Errorf("Close connection: %v", err)
		}
	})
	return connection
}

// publish sends one numbered message per sequence. When keys is non-nil it
// carries one key per sequence, so a test can place a message on a known
// partition; a nil keys list publishes unkeyed.
func (f *portFixture) publish(t *testing.T, sequences, keys []string) {
	t.Helper()
	messages := make([]driver.OutboundMessage, 0, len(sequences))
	for index, sequence := range sequences {
		message := driver.OutboundMessage{Destination: f.topic, Body: []byte(sequence)}
		if keys != nil {
			message.Key = []byte(keys[index])
		}
		messages = append(messages, message)
	}
	if err := f.producer.Publish(f.ctx, messages...); err != nil {
		t.Fatalf("Publish(%d messages to %q): %v", len(messages), f.topic, err)
	}
}

// portLogCapture is a slog handler that records what a driver logger receives.
//
// The underprovisioning warning is a diagnostic rather than a port error, so the
// logger the connection was built with is the only place it can be observed.
// The structured attributes are kept apart from the message: what the warning
// has to name is read from them, and the message is only checked for the one
// consequence an operator cannot see from the numbers.
type portLogCapture struct {
	mu      sync.Mutex
	records []portLogRecord
	notify  chan struct{}
}

// portLogRecord is one record the driver wrote.
type portLogRecord struct {
	level   slog.Level
	message string
	attrs   map[string]string
}

func newPortLogCapture() *portLogCapture {
	return &portLogCapture{notify: make(chan struct{}, 1)}
}

// logger returns the logger a driver configuration takes.
func (c *portLogCapture) logger() *slog.Logger { return slog.New(c) }

// Enabled accepts every level. A capture that filtered by level would make the
// test's subject disappear whenever a diagnostic moved between levels.
func (c *portLogCapture) Enabled(context.Context, slog.Level) bool { return true }

func (c *portLogCapture) Handle(_ context.Context, record slog.Record) error {
	attrs := make(map[string]string, record.NumAttrs())
	record.Attrs(func(attr slog.Attr) bool {
		attrs[attr.Key] = attr.Value.String()
		return true
	})
	c.mu.Lock()
	c.records = append(c.records, portLogRecord{level: record.Level, message: record.Message, attrs: attrs})
	c.mu.Unlock()
	select {
	case c.notify <- struct{}{}:
	default:
	}
	return nil
}

func (c *portLogCapture) WithAttrs([]slog.Attr) slog.Handler { return c }
func (c *portLogCapture) WithGroup(string) slog.Handler      { return c }

// warnings returns the records written at warn level.
func (c *portLogCapture) warnings() []portLogRecord {
	c.mu.Lock()
	defer c.mu.Unlock()
	records := make([]portLogRecord, 0, len(c.records))
	for _, record := range c.records {
		if record.level >= slog.LevelWarn {
			records = append(records, record)
		}
	}
	return records
}

// messages renders records as their messages, so a count assertion says what it
// counted.
func (c *portLogCapture) messages(records []portLogRecord) []string {
	messages := make([]string, 0, len(records))
	for _, record := range records {
		messages = append(messages, record.message)
	}
	return messages
}

// portAwaitWarnings blocks until the capture holds count warnings.
func portAwaitWarnings(t *testing.T, clk clock.Clock, logs *portLogCapture, count int, what string) {
	t.Helper()
	timer := clk.Timer(portAssignmentTimeout)
	defer timer.Stop()
	for {
		if len(logs.warnings()) >= count {
			return
		}
		select {
		case <-logs.notify:
		case <-timer.C:
			t.Fatalf("%s: %d of %d warnings within %s", what, len(logs.warnings()), count, portAssignmentTimeout)
		}
	}
}

// portAssertWarning checks one underprovisioning warning names the destination,
// the partitions this member holds, the budget those partitions fall short of,
// the lever that would change it, and the fact that raising the partition count
// re-maps keys. The last one is the only part an operator cannot read off the
// numbers, so it is the only part checked in the text.
func portAssertWarning(t *testing.T, warning portLogRecord, destination string, assigned, budget int) {
	t.Helper()
	if got := warning.attrs["destination"]; got != destination {
		t.Fatalf("warning names destination %q, want %q (attributes: %v)", got, destination, warning.attrs)
	}
	if got := warning.attrs["assigned_partitions"]; got != fmt.Sprint(assigned) {
		t.Fatalf("warning names assigned_partitions %q, want %d (attributes: %v)", got, assigned, warning.attrs)
	}
	if got := warning.attrs["budget"]; got != fmt.Sprint(budget) {
		t.Fatalf("warning names budget %q, want %d (attributes: %v)", got, budget, warning.attrs)
	}
	if warning.attrs["lever"] == "" {
		t.Fatalf("warning names no lever to raise (attributes: %v)", warning.attrs)
	}
	text := strings.ToLower(warning.message)
	if !strings.Contains(text, "partition count") || !strings.Contains(text, "re-map") {
		t.Fatalf("warning message %q does not say that raising the partition count re-maps keys", warning.message)
	}
}

// portTurnaroundPercentile returns the nearest-rank percentile of sorted
// samples, in the percent the caller names.
func portTurnaroundPercentile(sorted []time.Duration, percent int) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	index := (len(sorted)*percent+99)/100 - 1
	if index < 0 {
		index = 0
	}
	if index >= len(sorted) {
		index = len(sorted) - 1
	}
	return sorted[index]
}

// portPartitionTrace records the instants deliveries arrived at one consumer,
// by the partition they came from. A rebalance's cost to a partition is the
// interval between the last delivery before the membership change and the first
// one after it, and this is where both come from.
type portPartitionTrace struct {
	mu     sync.Mutex
	byPart map[int32][]time.Time
	notify chan struct{}
}

func newPortPartitionTrace() *portPartitionTrace {
	return &portPartitionTrace{byPart: make(map[int32][]time.Time), notify: make(chan struct{}, 1)}
}

func (p *portPartitionTrace) record(partition int32, at time.Time) {
	p.mu.Lock()
	p.byPart[partition] = append(p.byPart[partition], at)
	p.mu.Unlock()
	select {
	case p.notify <- struct{}{}:
	default:
	}
}

// reaching counts the partitions that have delivered at least each times.
func (p *portPartitionTrace) reaching(each int) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	count := 0
	for _, arrivals := range p.byPart {
		if len(arrivals) >= each {
			count++
		}
	}
	return count
}

// after counts the partitions that have delivered at or after boundary.
func (p *portPartitionTrace) after(boundary time.Time) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	count := 0
	for _, arrivals := range p.byPart {
		if len(arrivals) > 0 && !arrivals[len(arrivals)-1].Before(boundary) {
			count++
		}
	}
	return count
}

// awaitPartitions blocks until count partitions have each delivered at least
// each times.
func (p *portPartitionTrace) awaitPartitions(t *testing.T, clk clock.Clock, count, each int, what string) {
	t.Helper()
	p.await(t, clk, count, what, func() int { return p.reaching(each) })
}

// awaitPartitionsAfter blocks until count partitions have delivered at or after
// boundary.
func (p *portPartitionTrace) awaitPartitionsAfter(t *testing.T, clk clock.Clock, boundary time.Time, count int, what string) {
	t.Helper()
	p.await(t, clk, count, what, func() int { return p.after(boundary) })
}

func (p *portPartitionTrace) await(t *testing.T, clk clock.Clock, count int, what string, progress func() int) {
	t.Helper()
	timer := clk.Timer(portAssignmentTimeout)
	defer timer.Stop()
	for {
		if progress() >= count {
			return
		}
		select {
		case <-p.notify:
		case <-timer.C:
			t.Fatalf("%s: only %d of %d partitions did within %s",
				what, progress(), count, portAssignmentTimeout)
		}
	}
}

// gapsAcross reports, for every partition delivered to on both sides of
// boundary, the interval from its last delivery before the boundary to its first
// one at or after it. A partition missing from either side is one the consumer
// did not hold across the boundary, and its absence is not a gap.
func (p *portPartitionTrace) gapsAcross(boundary time.Time) map[int32]time.Duration {
	p.mu.Lock()
	defer p.mu.Unlock()
	gaps := make(map[int32]time.Duration, len(p.byPart))
	for partition, arrivals := range p.byPart {
		var before time.Time
		var after time.Time
		for _, at := range arrivals {
			if at.Before(boundary) {
				before = at
				continue
			}
			if after.IsZero() {
				after = at
			}
		}
		if before.IsZero() || after.IsZero() {
			continue
		}
		gaps[partition] = after.Sub(before)
	}
	return gaps
}
