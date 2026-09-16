//go:build integration

// Package kafka_test holds the port-level safety tests for the Kafka consumer.
//
// Every test here drives the exported driver port over a real broker and reads
// no package state. The package clause is the point: kafka_test cannot reach an
// unexported symbol, so the compiler rather than a reviewer's discipline proves
// that these tests measure the contract and not one implementation of it. They
// keep running, unchanged, across a rewrite of the consumer's internals, which
// is what a rewrite needs to be judged by.
package kafka_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	//nolint:depguard // this driver's own external test package exercises the driver through its exported port.
	kafka "fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/drivers/kafka"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

const (
	// portEndpointEnv names the fixture endpoint this file connects to.
	portEndpointEnv = "F1_KAFKA_ENDPOINT"
	// portDefaultEndpoint is the fixture's historical host and port.
	portDefaultEndpoint = "localhost:19092"

	// portPartitions is the partition count every destination in this file is
	// created with. A one-partition destination would make the rebalance tests
	// pass vacuously: nothing could move between members, and a prefetch share
	// would have a single partition to spread over however large it was.
	portPartitions = 6

	// portDrainTimeout is the in-flight settlement bound the driver under test
	// is built with.
	portDrainTimeout = time.Second
	// portAssignmentMargin is what the revoke test allows the group rebalance
	// protocol on top of portDrainTimeout.
	portAssignmentMargin = 5 * time.Second

	// portHold is how long the first consumer pauses before it settles a
	// delivery while a peer is joining or leaving, so the rebalance lands while
	// it is still working through the corpus and deliveries really are in
	// flight across it. It is not synchronization: every wait in this file
	// blocks on a notification or a delivery under a deadline. The consumer
	// closes the pace once the leave is over, so it is not a limit on the whole
	// corpus either.
	portHold = 150 * time.Millisecond

	// portQuietWindow is how long a held prefetch share must go without a
	// further delivery before the absence counts as evidence.
	portQuietWindow = 2 * time.Second

	// portSettleQuietWindow is how long a corpus must go without a further
	// settlement before the wait treats it as finished. It is what lets the
	// loss assertion name the sequences that never settled: a wait that only
	// accepted the full count would fail on the wait first and report how many
	// were missing rather than which.
	portSettleQuietWindow = 10 * time.Second

	// portCorpus is the numbered corpus the at-least-once test publishes. The
	// consumer settles it partition by partition, in the order a fetch returned
	// the partitions, so the corpus has to be long enough that a peer joining
	// after portJoinSettlements settlements still finds records on whichever
	// partitions it is assigned: at portHold per delivery and a join that takes
	// a couple of seconds, the holder has settled a few dozen records of the
	// corpus when the join completes, and the join's own pace line reports how
	// many.
	portCorpus = 360
	// portJoinSettlements is how many deliveries the first consumer must have
	// settled before the second one joins, and how many the second one settles
	// before it leaves.
	portJoinSettlements = 5
	// portMinAbandoned is the smallest number of unsettled deliveries the test
	// will accept as a leave to measure. Fewer than this and the leave handed
	// nothing back, which is not the state the test is about.
	portMinAbandoned = 3

	// portReleaseCorpus is the numbered corpus the release test hands back. It
	// is published one key per partition and is the destination's partition
	// count: the departing consumer takes the whole corpus without settling any
	// of it, and one unsettled delivery per partition is all a partition carries.
	portReleaseCorpus = 6
	// portNackCorpus is the numbered corpus the requeue test publishes. Every
	// message carries the same key, so all of them share one partition and the
	// order in which a requeue rewinds is defined.
	portNackCorpus = 6
	// portNackKey is the key every message of the requeue corpus carries.
	portNackKey = "port-requeue-key"

	// portShare is the prefetch share the share test requests. It must not
	// exceed the destination's partition count: a consumer can only hold one
	// delivery per partition, so the share test's destination carries exactly
	// one record per partition and a larger share shows up as a short count
	// rather than as the distribution a share is about.
	portShare = 3

	// portTurnaroundCorpus is how many records the turnaround measurement takes,
	// settles and times. It is published under one key, so the whole corpus
	// shares one partition and the gap between two consecutive deliveries is
	// that partition's turnaround rather than a delivery another partition had
	// already handed over.
	portTurnaroundCorpus = 500
	// portTurnaroundKey is the key every record of that corpus carries.
	portTurnaroundKey = "port-turnaround-key"
	// portTurnaroundShare is the prefetch share the turnaround consumer runs
	// with, and so the read-ahead limit of the partition it measures. It is far
	// larger than the corpus's arrival burst needs to be: a partition whose
	// fetches are held too early starves its handler, and a settle that finds
	// nothing waiting measures the broker's next response instead of the wake.
	portTurnaroundShare = 64
	// portTurnaroundBound is the p99 the turnaround measurement requires. A wake
	// the settle failed to take shows up at the fetch wait, FetchMaxWait, which
	// the driver sets to 50 ms (drivers/kafka/consumer.go), because the
	// successor then waits for the next broker response instead.
	portTurnaroundBound = 10 * time.Millisecond

	// portKeyProbeRounds bounds how many candidate keys the partition probe
	// publishes before it gives up.
	portKeyProbeRounds = 4
	// portKeyProbeBatch is how many candidate keys one probe round publishes.
	// Each key hashes to one of six partitions, so one round is expected to
	// cover all six with room to spare.
	portKeyProbeBatch = 40

	// portAssignmentTimeout bounds a wait for a partition assignment.
	portAssignmentTimeout = 45 * time.Second
	// portDeliveryTimeout bounds a wait for one delivery.
	portDeliveryTimeout = 15 * time.Second
	// portSettleTimeout bounds a wait for a settlement.
	portSettleTimeout = 45 * time.Second
	// portCloseTimeout bounds the teardown of a producer or consumer.
	portCloseTimeout = 15 * time.Second
	// portDestinationReadyTimeout bounds the wait for a new destination to
	// report a live leader for every partition.
	portDestinationReadyTimeout = 10 * time.Second
	// portDestinationReadyPoll is how often the fixture asks the broker whether
	// the destination is ready. It is a bounded retry against a broker-side
	// fact with no notification to wait on.
	portDestinationReadyPoll = 25 * time.Millisecond
)

// TestPortAtLeastOnceAcrossJoinAndLeave publishes a numbered corpus and settles
// every message while a second consumer joins the group mid-stream and then
// leaves holding deliveries it never settled. It asserts that every number is
// settled at least once, and that each message the departing consumer held
// unsettled settles only after it left, which is what proves the broker
// redelivered it rather than the test having counted a delivery as its
// settlement.
func TestPortAtLeastOnceAcrossJoinAndLeave(t *testing.T) {
	fixture := newPortFixture(t, "at-least-once")
	clk, ctx := fixture.clock, fixture.ctx
	keys := portKeysByPartition(t)
	sequences := portSequences(portCorpus)
	ledger := newPortLedger(clk)

	holder := fixture.subscribe(t, fixture.consumerConfig(fixture.group, portPartitions))
	holder.notifications.awaitAssignment(t, portAssignmentTimeout, "the first consumer")

	// The holder settles every delivery it takes, slowly enough that the join
	// and the leave both land while it is still working through the corpus. The
	// pace is closed once the leave is over, so the rest of the corpus finishes
	// promptly.
	pace := newPortPace(portHold)
	fixture.publish(t, sequences, portKeyedSequences(sequences, keys))
	settler := holder.pump(func(message driver.InboundMessage) {
		ledger.received(portSequenceOf(message))
		if err := pace.wait(ctx, clk); err != nil {
			return
		}
		ledger.acknowledge(ctx, message)
	})
	defer settler.stop()

	portAwaitSettlements(t, ledger, portJoinSettlements, sequences, "the first consumer to settle before the join")

	// The joining consumer settles a few deliveries and leaves holding the
	// rest: deliveries it took and never settled are exactly what a leave has
	// to hand back. Its prefetch is the whole corpus, so its partitions cannot
	// stall it while the test holds them.
	joiningAt := clk.Now()
	holdings := newPortHoldings()
	joiner := fixture.subscribe(t, fixture.consumerConfig(fixture.group, portCorpus))
	taker := joiner.pump(func(message driver.InboundMessage) {
		ledger.received(portSequenceOf(message))
		holdings.take(portSequenceOf(message), clk.Now(), message)
	})
	defer taker.stop()

	assigned := joiner.notifications.awaitAssignment(t, portAssignmentTimeout, "the joining consumer")
	settledAtAssignment := ledger.settledCount()
	// The joining consumer holds no more unsettled deliveries at once than the
	// partitions it was assigned, and a two-member split of portPartitions
	// hands it fewer than portJoinSettlements+portMinAbandoned of them, so the
	// share it settles before it leaves is taken and settled a delivery at a
	// time rather than taken whole: a settlement frees its partition for that
	// partition's next record, which is how a consumer works through any
	// corpus. The arithmetic asserted below is unchanged: portJoinSettlements
	// deliveries settle before the leave, and portMinAbandoned are still in
	// hand when it happens.
	settledHere := make(map[string]bool, portJoinSettlements)
	settled := 0
	for settled < portJoinSettlements {
		portAwaitHoldings(t, clk, holdings, settled+1, "the joining consumer to take a share of the corpus")
		holding := holdings.all()[settled]
		settled++
		ledger.acknowledge(ctx, holding.message)
		settledHere[holding.sequence] = true
	}
	portAwaitHoldings(t, clk, holdings, settled+portMinAbandoned,
		"the joining consumer to hold its share of the corpus")

	// Taking stops before the held deliveries are read, so the set the leaver
	// holds is fixed. A snapshot taken while it was still taking would leave
	// whatever arrived after it unmeasured and the settlement those deliveries
	// owe unchecked. Deliveries the consumer has admitted but the pump has not
	// read stay in its channel and the same leave hands them back: they are not
	// in this set, and the corpus-wide loss assertion below covers them.
	taker.stop()
	held := holdings.all()
	// How much of the corpus was still the holder's when the join completed,
	// and how much the joining consumer took from it, is what a later run of
	// this same file compares against: it is the evidence that the join landed
	// mid-stream rather than after the work was over.
	t.Logf("port-join test=at-least-once ms=%d settled=%d held=%d",
		clk.Since(joiningAt).Milliseconds(), settledAtAssignment, len(held))
	// A consumer can take one sequence twice, because a handoff and its own
	// fetch can both carry the same record, and this file counts that as a
	// duplicate rather than a defect. A sequence settled above is therefore
	// dropped from the abandoned set however many copies of it were taken, so
	// the check below never asks for a settlement after the leave from a
	// sequence this test settled before it.
	abandoned := make([]portHolding, 0, len(held))
	seen := make(map[string]bool, len(held))
	for _, holding := range held {
		if settledHere[holding.sequence] || seen[holding.sequence] {
			continue
		}
		seen[holding.sequence] = true
		abandoned = append(abandoned, holding)
	}
	if len(abandoned) < portMinAbandoned {
		t.Fatalf("the leaving consumer held %d unsettled deliveries, want at least %d: a leave that hands back fewer than that is not the state this test measures",
			len(abandoned), portMinAbandoned)
	}
	leftAt := clk.Now()
	if err := joiner.Release(ctx); err != nil {
		t.Fatalf("Release the joining consumer while it holds %d unsettled deliveries: %v", len(abandoned), err)
	}
	pace.close()

	// The corpus is finished when every sequence has settled, or when nothing
	// new has settled for the quiet window. The second case is a loss, and the
	// assertion below is what names it.
	portAwaitCorpus(t, ledger, len(sequences), sequences, portSettleQuietWindow, portSettleTimeout,
		"the corpus to finish settling after the leave")

	settler.stop()
	ledger.assertNoFailures(t)
	portAssertNoLoss(t, ledger, sequences)
	for _, holding := range abandoned {
		settledAt, everSettled := ledger.firstSettlement(holding.sequence)
		if !everSettled {
			t.Fatalf("message %s was held unsettled by the consumer that left and never settled", holding.sequence)
		}
		if !settledAt.After(leftAt) {
			t.Fatalf("message %s was held unsettled by the consumer that left and settled at %s, at or before the leave at %s: a settlement that predates the leave is not the redelivery the leave owes",
				holding.sequence, settledAt.Format(time.RFC3339Nano), leftAt.Format(time.RFC3339Nano))
		}
	}
	ledger.metrics(t, "at-least-once", sequences, held[0].at.Sub(assigned))
	holder.notifications.assertNoFatal(t)
	joiner.notifications.assertNoFatal(t)
}

// TestPortHeldDeliveryDoesNotHoldRevoke holds one delivery per partition without
// settling any of them, then joins a second consumer to the same group. It
// asserts that the joining consumer is assigned within portDrainTimeout plus
// portAssignmentMargin, and that every held message is redelivered and settled
// by whoever owns its partition afterwards.
//
// One delivery per partition is the point of the corpus: whichever partitions
// the rebalance moves, at least one delivery the holder is still holding sits on
// one of them, so the settle window really is exercised.
func TestPortHeldDeliveryDoesNotHoldRevoke(t *testing.T) {
	fixture := newPortFixture(t, "held-revoke")
	clk, ctx := fixture.clock, fixture.ctx
	keys := portKeysByPartition(t)
	sequences := portSequences(fixture.partitions)
	ledger := newPortLedger(clk)

	holder := fixture.subscribe(t, fixture.consumerConfig(fixture.group, fixture.partitions))
	holder.notifications.awaitAssignment(t, portAssignmentTimeout, "the holding consumer")

	fixture.publish(t, sequences, keys)

	held := make([]driver.InboundMessage, 0, len(sequences))
	for range sequences {
		message := portAwaitDelivery(t, clk, holder, portDeliveryTimeout, "the holding consumer to take the whole corpus")
		ledger.received(portSequenceOf(message))
		held = append(held, message)
	}
	if covered := portDistinctPartitions(held); covered != fixture.partitions {
		t.Fatalf("the holding consumer has deliveries from %d partitions, want %d: a rebalance that moved any partition would show whether a held delivery can hold it",
			covered, fixture.partitions)
	}

	joiningAt := clk.Now()
	joiner := fixture.subscribe(t, fixture.consumerConfig(fixture.group, fixture.partitions))
	assigned := joiner.notifications.awaitAssignment(t, portDrainTimeout+portAssignmentMargin,
		"the joining consumer to be assigned within the settlement bound plus its margin")
	revoke := clk.Since(joiningAt)
	t.Logf("port-revoke test=held-revoke ms=%d", revoke.Milliseconds())

	firstDeliveries := make(chan time.Time, 1)
	pump := joiner.pump(func(message driver.InboundMessage) {
		select {
		case firstDeliveries <- clk.Now():
		default:
		}
		ledger.received(portSequenceOf(message))
		ledger.acknowledge(ctx, message)
	})
	defer pump.stop()

	firstDeliveryAt := portAwait(t, clk, firstDeliveries, portDeliveryTimeout, "the joining consumer to receive a transferred delivery")

	// The holder settles what it is still holding. A settlement whose ownership
	// moved during the rebalance fails and is recorded as such; the message
	// itself has to arrive at its new owner, which is what the loss assertion
	// below measures.
	for _, message := range held {
		ledger.acknowledge(ctx, message)
	}

	portAwaitSettlements(t, ledger, len(sequences), sequences, "every held message to settle somewhere")
	// How many sequences had a settlement refused is the evidence that the held
	// deliveries really were admitted across it: a consumer that never held a
	// delivery at a revoke would refuse none, and the count is zero only then.
	refused := ledger.revokedSequences()
	t.Logf("port-revoke-settlements test=held-revoke refused=%d", len(refused))
	pump.stop()
	ledger.assertNoFailures(t)
	portAssertNoLoss(t, ledger, sequences)

	// A refused settlement is what a rebalance does to a held delivery: it was
	// delivered to the holder, the holder cannot settle it, and only the
	// partition's new owner can. Each of those has to come back, otherwise the
	// handoff dropped it and the corpus only looks settled because the holder's
	// own acks were recorded. The count is also the guard that this test
	// measured anything: a rebalance that moved no partition refuses none.
	if len(refused) == 0 {
		t.Fatalf("the revoke refused none of the %d settlements the holder attempted: no held delivery was in flight over the rebalance, so this test measured nothing",
			len(sequences))
	}
	for _, sequence := range refused {
		if received := ledger.countReceived(sequence); received < 2 {
			t.Fatalf("%s was delivered %d times, want at least 2: its settlement was refused when the rebalance moved its partition, so its new owner had to deliver it again",
				sequence, received)
		}
		settledAt, settled := ledger.firstSettlement(sequence)
		if !settled || !settledAt.After(assigned) {
			t.Fatalf("%s settled at %s, want a settlement after the assignment at %s: the delivery this consumer was refused is the new owner's to settle, and a settlement that predates the assignment is not the redelivery the rebalance owes",
				sequence, settledAt.Format(time.RFC3339Nano), assigned.Format(time.RFC3339Nano))
		}
	}

	ledger.metrics(t, "held-revoke", sequences, firstDeliveryAt.Sub(assigned))
	holder.notifications.assertNoFatal(t)
	joiner.notifications.assertNoFatal(t)
}

// TestPortReleaseWithoutSettleRedelivers has one consumer take a whole corpus
// and leave without settling any of it, then joins a second consumer to the same
// group. The second consumer must receive every message, because Release hands
// back what the first consumer was given.
func TestPortReleaseWithoutSettleRedelivers(t *testing.T) {
	fixture := newPortFixture(t, "release")
	clk, ctx := fixture.clock, fixture.ctx
	keys := portKeysByPartition(t)
	sequences := portSequences(portReleaseCorpus)
	ledger := newPortLedger(clk)

	departing := fixture.subscribe(t, fixture.consumerConfig(fixture.group, len(sequences)))
	departing.notifications.awaitAssignment(t, portAssignmentTimeout, "the departing consumer")

	fixture.publish(t, sequences, portKeyedSequences(sequences, keys))

	for range sequences {
		message := portAwaitDelivery(t, clk, departing, portDeliveryTimeout, "the departing consumer to take the whole corpus")
		ledger.received(portSequenceOf(message))
	}
	if err := departing.Release(ctx); err != nil {
		t.Fatalf("Release the departing consumer: %v", err)
	}

	joiner := fixture.subscribe(t, fixture.consumerConfig(fixture.group, len(sequences)))
	assigned := joiner.notifications.awaitAssignment(t, portAssignmentTimeout, "the second consumer")

	first := portAwaitDelivery(t, clk, joiner, portDeliveryTimeout, "the second consumer to receive a released message")
	ledger.received(portSequenceOf(first))
	ledger.acknowledge(ctx, first)
	firstDelivery := clk.Since(assigned)

	for range len(sequences) - 1 {
		message := portAwaitDelivery(t, clk, joiner, portDeliveryTimeout, "the second consumer to receive every released message")
		ledger.received(portSequenceOf(message))
		ledger.acknowledge(ctx, message)
	}

	ledger.assertNoFailures(t)
	if unsettled := ledger.unsettled(sequences); len(unsettled) > 0 {
		t.Fatalf("the departing consumer released %d messages and %d of them were never settled by the second consumer: %v",
			len(sequences), len(unsettled), unsettled)
	}
	ledger.metrics(t, "release-redelivers", sequences, firstDelivery)
	departing.notifications.assertNoFatal(t)
	joiner.notifications.assertNoFatal(t)
}

// TestPortNackRequeueRedelivers requeues the first delivery of a partition and
// settles the rest. It asserts that the requeued message arrives again and that
// the later messages of the same partition are still delivered.
func TestPortNackRequeueRedelivers(t *testing.T) {
	fixture := newPortFixture(t, "nack-requeue")
	clk, ctx := fixture.clock, fixture.ctx
	sequences := portSequences(portNackCorpus)
	ledger := newPortLedger(clk)

	keys := make([]string, len(sequences))
	for index := range keys {
		keys[index] = portNackKey
	}
	fixture.publish(t, sequences, keys)

	// A share of one is what places the rest of the partition after the
	// requeue. With a larger share the later messages of the partition would
	// already be in hand when the requeue happens, so their arrival would say
	// nothing about it, and a consumer that never resumed the partition would
	// still settle them from the buffer.
	consumer := fixture.subscribe(t, fixture.consumerConfig(fixture.group, 1))
	assigned := consumer.notifications.awaitAssignment(t, portAssignmentTimeout, "the consumer")

	first := portAwaitDelivery(t, clk, consumer, portDeliveryTimeout, "the first delivery of the partition")
	firstDelivery := clk.Since(assigned)
	requeued := portSequenceOf(first)
	if requeued != sequences[0] {
		t.Fatalf("first delivery = %s, want %s: the corpus is published to one partition and this test requeues its head",
			requeued, sequences[0])
	}
	ledger.received(requeued)
	if err := first.Settle.Nack(ctx, driver.NackOptions{Requeue: true}); err != nil {
		t.Fatalf("Nack(requeue) %s: %v", requeued, err)
	}

	// Every delivery is settled as it arrives, so the partition drains one
	// delivery at a time and the corpus can only settle if the requeue both
	// brings the head back and resumes the messages behind it. The head counts
	// as delivered again only when it comes back a second time.
	for ledger.settledCount() < len(sequences) {
		message := portAwaitDelivery(t, clk, consumer, portDeliveryTimeout, "the next delivery of the partition after the requeue")
		ledger.received(portSequenceOf(message))
		ledger.acknowledge(ctx, message)
	}

	ledger.assertNoFailures(t)
	if received := ledger.countReceived(requeued); received != 2 {
		t.Fatalf("%s was delivered %d times, want 2: a requeue delivers the message again and nothing else", requeued, received)
	}
	ledger.metrics(t, "nack-requeue", sequences, firstDelivery)
	consumer.notifications.assertNoFatal(t)
}

// TestPortPrefetchShareHolds gives one consumer a prefetch share of portShare on
// a destination whose every partition carries a record, and takes deliveries
// without settling any of them. It asserts that exactly portShare arrive, that
// nothing further arrives while they are held, and that settling one admits
// exactly one more.
//
// The destination carries one record per partition, so a record is available on
// every partition for the whole of the test: a share that was ignored could take
// more, and a share that could not be reached would show up as a short count.
func TestPortPrefetchShareHolds(t *testing.T) {
	fixture := newPortFixture(t, "prefetch-share")
	clk, ctx := fixture.clock, fixture.ctx
	keys := portKeysByPartition(t)

	sequences := portSequences(len(keys))
	fixture.publish(t, sequences, keys)

	config := fixture.consumerConfig(fixture.group, portShare)
	config.PerDestination = map[string]int{fixture.topic: portShare}
	consumer := fixture.subscribe(t, config)
	assigned := consumer.notifications.awaitAssignment(t, portAssignmentTimeout, "the consumer")

	ledger := newPortLedger(clk)
	held := make([]driver.InboundMessage, 0, portShare+1)
	taken := make([]string, 0, portShare+1)
	firstDelivery := time.Duration(0)
	// Every delivery of the count is awaited under the same deadline as the
	// first: the count is the assertion, and a consumer that took fewer than
	// its share is a different finding from one that took them slowly.
	for len(held) < portShare {
		message, ok := portCollect(t, clk, consumer, portDeliveryTimeout)
		if !ok {
			break
		}
		if len(held) == 0 {
			firstDelivery = clk.Since(assigned)
		}
		ledger.received(portSequenceOf(message))
		taken = append(taken, portSequenceOf(message))
		held = append(held, message)
	}
	if len(held) != portShare {
		t.Fatalf("the consumer took %d deliveries under a share of %d, want %d: a share is honoured when the consumer takes exactly that many and a destination record is left for it on every partition",
			len(held), portShare, portShare)
	}

	if extra := portAwaitQuiet(t, clk, consumer, portQuietWindow); extra != nil {
		t.Fatalf("a delivery of %s arrived within %s while the consumer held %d unsettled deliveries against a share of %d",
			portSequenceOf(*extra), portQuietWindow, len(held), portShare)
	}

	ledger.acknowledge(ctx, held[0])
	message := portAwaitDelivery(t, clk, consumer, portDeliveryTimeout, "one more delivery after one settlement freed a share slot")
	// What a freed slot admits has to be a record the consumer had not taken
	// yet. A share of N that admitted a second copy of one of the N would be
	// holding more of the destination than it was given, and without this check
	// the count below could be reached by redelivering what is already held.
	if seen := ledger.countReceived(portSequenceOf(message)); seen > 0 {
		t.Fatalf("the delivery admitted by one settlement was %s, which the consumer had already taken %d times: a settled slot admits a record the share had not admitted",
			portSequenceOf(message), seen)
	}
	ledger.received(portSequenceOf(message))
	taken = append(taken, portSequenceOf(message))

	// One settlement admits exactly one more: the consumer holds a full share
	// again, so the quiet window has to pass a second time.
	if second := portAwaitQuiet(t, clk, consumer, portQuietWindow); second != nil {
		t.Fatalf("a second delivery of %s arrived within %s after one settlement admitted one more: one settlement frees one slot against a share of %d",
			portSequenceOf(*second), portQuietWindow, portShare)
	}

	// Drain stops fetching, so after it returns the deliveries already taken are
	// the whole outstanding set. Settling them before Stop is what keeps the
	// lost count at zero for the messages this test took, which is what the
	// metric line reports.
	if err := consumer.Drain(ctx); err != nil {
		t.Fatalf("Drain the consumer: %v", err)
	}
	portSettleRemaining(ctx, ledger, consumer, append([]driver.InboundMessage{message}, held[1:]...))
	if err := consumer.Stop(ctx); err != nil {
		t.Fatalf("Stop the consumer: %v", err)
	}

	ledger.assertNoFailures(t)
	// The loss assertion covers the sequences the share admitted. The records
	// past it are the guard that the share was not ignored: they are unsettled
	// because the share never let the consumer take them, which is the state
	// this test is about rather than a loss.
	portAssertNoLoss(t, ledger, taken)
	ledger.metrics(t, "prefetch-share", taken, firstDelivery)
	consumer.notifications.assertNoFatal(t)
}

// TestPortTurnaroundAfterSettle measures the time from a settlement returning to
// the arrival of that partition's next delivery, over a corpus that shares one
// key so every delivery of it comes from one partition.
//
// This is the number a next-record wait rule lives or dies by. The successor is
// already in the consumer's pending list when the settle returns, and the only
// thing that admits it is the wake the settle takes, so a consumer that waits
// for its next broker response instead measures the fetch wait here rather than
// a wake. It reports port-metrics test=turnaround p50-ms=<n> p99-ms=<n> and
// requires the p99 to stay under portTurnaroundBound.
func TestPortTurnaroundAfterSettle(t *testing.T) {
	fixture := newPortFixture(t, "turnaround")
	clk, ctx := fixture.clock, fixture.ctx

	keys := make([]string, portTurnaroundCorpus)
	for index := range keys {
		keys[index] = portTurnaroundKey
	}
	sequences := portSequences(portTurnaroundCorpus)
	fixture.publish(t, sequences, keys)

	config := fixture.consumerConfig(fixture.group, portTurnaroundShare)
	consumer := fixture.subscribe(t, config)
	assigned := consumer.notifications.awaitAssignment(t, portAssignmentTimeout, "the consumer")

	ledger := newPortLedger(clk)
	samples := make([]time.Duration, 0, portTurnaroundCorpus-1)
	firstDelivery := time.Duration(0)
	partition := int32(-1)
	var settled time.Time
	for index := range portTurnaroundCorpus {
		message, ok := portCollect(t, clk, consumer, portDeliveryTimeout)
		if !ok {
			t.Fatalf("the consumer took %d of %d deliveries, then none within %s", index, portTurnaroundCorpus, portDeliveryTimeout)
		}
		arrived := clk.Now()
		if index == 0 {
			firstDelivery = clk.Since(assigned)
			partition = message.Ref.Partition
		}
		if message.Ref.Partition != partition {
			t.Fatalf("delivery %d arrived from partition %d after partition %d: the corpus shares one key, so every delivery of it comes from one partition and a gap between two of them is that partition's turnaround",
				index, message.Ref.Partition, partition)
		}
		if !settled.IsZero() {
			samples = append(samples, arrived.Sub(settled))
		}
		ledger.received(portSequenceOf(message))
		ledger.acknowledge(ctx, message)
		settled = clk.Now()
	}

	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	p50 := portTurnaroundPercentile(samples, 50)
	p99 := portTurnaroundPercentile(samples, 99)
	t.Logf("port-metrics test=turnaround p50-ms=%.3f p99-ms=%.3f",
		float64(p50)/float64(time.Millisecond), float64(p99)/float64(time.Millisecond))
	if p99 > portTurnaroundBound {
		t.Fatalf("turnaround p99 = %s over %d deliveries, want at most %s: a settled delivery's successor is already in the consumer's pending list, and a consumer that measures the fetch wait here did not wake for it",
			p99, len(samples), portTurnaroundBound)
	}

	if err := consumer.Drain(ctx); err != nil {
		t.Fatalf("Drain the consumer: %v", err)
	}
	portSettleRemaining(ctx, ledger, consumer, nil)
	if err := consumer.Stop(ctx); err != nil {
		t.Fatalf("Stop the consumer: %v", err)
	}
	ledger.assertNoFailures(t)
	ledger.metrics(t, "turnaround", sequences, firstDelivery)
	consumer.notifications.assertNoFatal(t)
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
func newPortFixture(t *testing.T, label string) *portFixture {
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
		RequireDurableAck: true,
		Effective:         connection.Capabilities(),
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
		Destinations: []driver.DestinationSpec{{Name: topic, Partitions: portPartitions, Durable: true}},
		Policy:       driver.TopologyDeclare,
		Effective:    connection.Capabilities(),
	}); err != nil {
		t.Fatalf("EnsureTopology(%q, %d partitions): %v", topic, portPartitions, err)
	}

	fixture := &portFixture{
		ctx:      ctx,
		clock:    clock.NewReal(),
		conn:     connection,
		producer: producer,
		admin:    admin,
		topic:    topic,
		group:    group,
	}
	fixture.partitions = fixture.awaitDestination(t)
	if fixture.partitions != portPartitions {
		t.Fatalf("destination %s has %d partitions, want %d: a rebalance and a prefetch share both need the partitions they are measured over",
			topic, fixture.partitions, portPartitions)
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
				f.topic, portPartitions, portDestinationReadyTimeout, lastErr)
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
	consumer, err := f.conn.Consumer(f.ctx, config)
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

// portNotifications records the rebalance notifications a consumer publishes on
// Errors().
//
// The port carries failures and routine lifecycle events on one channel and has
// no typed rebalance event, so an event is recognised by the classification the
// port gives it and by the event name its text carries. That text is the only
// port-level signal a rebalance produces; the tests below assert on deliveries
// and settlements, never on the text.
type portNotifications struct {
	clock   clock.Clock
	stopC   chan struct{}
	done    chan struct{}
	once    sync.Once
	mu      sync.Mutex
	failed  []error
	assigns chan time.Time
}

// The port carries no timestamp on a notification, so the instant the recorder
// reads it off the error channel is the closest thing to one it offers, and the
// first-delivery-ms metric is measured from there.

// watchRebalanceNotifications starts recording the notifications one consumer
// publishes. The recorder stops when the consumer's error channel closes or when
// the test calls stop.
func watchRebalanceNotifications(clk clock.Clock, consumer driver.Consumer) *portNotifications {
	notifications := &portNotifications{
		clock:   clk,
		stopC:   make(chan struct{}),
		done:    make(chan struct{}),
		assigns: make(chan time.Time, 1),
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
		// every failureSummary this file prints.
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

var portKeyProbe struct {
	mu     sync.Mutex
	probed bool
	keys   []string
	err    error
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

// portDiscoverPartitionKeys publishes keyed candidates until every partition has
// answered with at least one of them.
func portDiscoverPartitionKeys(t *testing.T) ([]string, error) {
	fixture := newPortFixture(t, "key-probe")
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
