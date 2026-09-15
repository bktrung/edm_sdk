//go:build integration

// Tests in this file raise a real alarm inside the fixture's own container, so
// they need the container id the RabbitMQ make targets pass, and they assert
// against a live broker's publishing block rather than a simulation of one.
// Neither test is parallel: a RabbitMQ alarm is node-wide, so the test that
// raised one has to be the only test publishing while it is up.
package rabbitmq

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

const (
	// alarmContainerEnv names the fixture container the alarm commands run in.
	alarmContainerEnv = "F1_RABBITMQ_CONTAINER"
	// alarmWatermark is a memory high watermark no broker can stay under: it is
	// small enough a fraction of total memory that the running node is above
	// it, which raises the memory alarm, the alarm that blocks publishing. The
	// watermark the broker was running with is read first and put back in its
	// place.
	alarmWatermark = "0.0000001"
	// publishDeadline is the deadline the blocked publishes run under and
	// publishBound is the wall-clock bound they have to return within. The gap
	// between them is what a caller keeps after a driver stops writing.
	publishDeadline = 3 * time.Second
	publishBound    = 5 * time.Second
	// blockedBatchMessages and blockedMessageSize size a batch at 16 MiB, more
	// than the socket buffers on either side of the connection hold.
	blockedBatchMessages = 256
	blockedMessageSize   = 64 << 10
	// blockNoticeTimeout bounds the wait for the broker's own notification, and
	// blockStatePoll is the interval the state it produces is confirmed at. The
	// notification is the wait; the re-check that follows it exists because
	// amqp091 hands the notification to the driver's listener first and to this
	// test's second, and the goroutine draining the driver's listener is
	// scheduled on its own, so being told is not the same as having recorded
	// it. It exits on its first pass whenever the driver was already awake.
	blockNoticeTimeout = 15 * time.Second
	blockStatePoll     = 5 * time.Millisecond
	// alarmGuardDelay is how long a test lets its own alarm stand behind
	// itself. A run in which a close is not bounded by the caller ends in a
	// channel close the broker has stopped answering, and that is what a run
	// where the bound is missing or broken does: without the guard it would
	// outlast the package timeout instead of reporting the time it took. A
	// cleanup close, which runs on a context with no deadline by design, is
	// released by the guard the same way. No passing run comes close, because
	// every publish it makes is bounded by its own deadline.
	alarmGuardDelay = 30 * time.Second
	// unreportedDeadline is the deadline the calls that meet an alarm the
	// connection has not been told about run under, and unreportedBound is the
	// wall-clock bound they have to return within.
	unreportedDeadline = 2 * time.Second
	unreportedBound    = 4 * time.Second
	// closeMeasureDeadline bounds the connection close this file measures under
	// a block. That close is measured and not asserted: a close that outlives
	// its deadline belongs to the row that owns the close path.
	closeMeasureDeadline = 3 * time.Second
)

// requireAlarmContainer returns the container id the publishing-block tests
// raise their alarm in. There is no skip branch: a suite that cannot raise the
// alarm cannot make the assertion it exists for, and reporting success without
// it is the outcome this file is here to remove.
func requireAlarmContainer(t *testing.T) string {
	t.Helper()
	container := os.Getenv(alarmContainerEnv)
	if container == "" {
		t.Fatalf("%s is empty: these tests raise an alarm inside the fixture container, and make test-rabbitmq passes its id", alarmContainerEnv)
	}
	return container
}

// rabbitmqctl runs one rabbitmqctl command inside the fixture container. Every
// argument is passed to docker exec as a list and never through a shell, so a
// value read back from the broker cannot become a command.
func rabbitmqctl(container string, args ...string) (string, error) {
	command := exec.Command("docker", append([]string{"exec", container, "rabbitmqctl"}, args...)...) //nolint:gosec // the container id comes from the fixture's own make target and every argument is a literal or a value read back from this broker.
	output, err := command.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("docker exec rabbitmqctl %s: %w: %s", strings.Join(args, " "), err, bytes.TrimSpace(output))
	}
	return strings.TrimSpace(string(output)), nil
}

// memoryWatermark reads the relative memory high watermark the fixture is
// running with. It is read rather than assumed so the alarm can be lifted by
// the value it replaced: the next test in this package publishes against this
// broker, and putting back a plausible default would leave it with a watermark
// nobody chose. Failing on a watermark configured as an absolute value is
// deliberate, because that is one this test cannot name and restore.
func memoryWatermark(t *testing.T, container string) string {
	t.Helper()
	status, err := rabbitmqctl(container, "status", "--formatter", "json")
	if err != nil {
		t.Fatalf("reading the broker status: %v", err)
	}
	var parsed struct {
		Setting struct {
			Relative *float64 `json:"relative"`
		} `json:"vm_memory_high_watermark_setting"`
	}
	if err := json.Unmarshal([]byte(status), &parsed); err != nil {
		t.Fatalf("parsing the broker status: %v", err)
	}
	if parsed.Setting.Relative == nil {
		t.Fatalf("vm_memory_high_watermark_setting is not a relative watermark, so this test cannot restore what it replaces: %s", status)
	}
	return strconv.FormatFloat(*parsed.Setting.Relative, 'f', -1, 64)
}

// raiseMemoryAlarm puts the fixture under a memory alarm and returns a function
// that lifts it again by restoring the watermark it replaced. The restore is
// idempotent because the test's cleanup and its stuck-publish guard both call
// it, and only one of them may act.
func raiseMemoryAlarm(t *testing.T, container string) func() {
	t.Helper()
	original := memoryWatermark(t, container)
	var once sync.Once
	restore := func() {
		once.Do(func() {
			if _, err := rabbitmqctl(container, "set_vm_memory_high_watermark", original); err != nil {
				t.Errorf("restoring vm_memory_high_watermark %s: %v", original, err)
			}
		})
	}
	// The restore is registered before the watermark is set, so an alarm raised
	// by a command that then reported a failure is still lifted.
	t.Cleanup(restore)
	if _, err := rabbitmqctl(container, "set_vm_memory_high_watermark", alarmWatermark); err != nil {
		t.Fatalf("raising the memory alarm: %v", err)
	}
	return restore
}

// openBlockedDestination declares the queue the publishing-block tests publish
// to, through a raw connection that never publishes under the alarm and so is
// never blocked itself, and leaves the queue's removal to cleanup. Nothing this
// file created is left behind for the next test in the package.
func openBlockedDestination(t *testing.T, destination string) {
	t.Helper()
	raw, err := amqp.Dial(defaultEndpoint)
	if err != nil {
		t.Fatalf("raw broker connection: %v", err)
	}
	t.Cleanup(func() { _ = raw.Close() })
	channel, err := raw.Channel()
	if err != nil {
		t.Fatalf("raw channel: %v", err)
	}
	_, _ = channel.QueueDelete(destination, false, false, false)
	if _, err := channel.QueueDeclare(destination, true, false, false, false, amqp.Table{"x-queue-type": "quorum"}); err != nil {
		t.Fatalf("QueueDeclare(%q): %v", destination, err)
	}
	t.Cleanup(func() {
		_, _ = channel.QueueDelete(destination, false, false, false)
		_ = channel.Close()
	})
}

// raiseBlockedConnection makes the broker tell conn it is blocking publishers,
// and reports how long the broker took to say so. A broker that is blocking
// only tells a connection when that connection publishes under the alarm: the
// alarm on its own is silent, which is why the probe is the publish.
//
// The publish that carries the connection over that line is a raw one on the
// connection itself and not a driver publish: it exists to put the connection
// into the blocked state before the code under test runs, and it does that with
// nothing under test in the way. A driver publish would either have to be
// waited out, since its confirmation is held for as long as the block lasts, or
// abandoned at its deadline and leave the producer it was made on closed - and
// that abandonment is a measurement of the publish path, not a way to set up
// the state for one.
func raiseBlockedConnection(t *testing.T, ctx context.Context, publicConn driver.Conn, destination string) time.Duration {
	t.Helper()
	rabbitConn := publicConn.(*conn)
	// amqp091 broadcasts these to every listener it holds for the connection,
	// and the driver registered its own when the connection opened, so a
	// notification received here is one the driver is being handed too.
	events := rabbitConn.amqp.NotifyBlocked(make(chan amqp.Blocking, 1))
	channel, err := rabbitConn.amqp.Channel()
	if err != nil {
		t.Fatalf("raw channel on the connection under test: %v", err)
	}
	start := time.Now() //nolint:forbidigo // the broker's own notification latency is one of the measurements this file reports.
	if err := channel.PublishWithContext(ctx, "", destination, false, false, amqp.Publishing{Body: []byte("make the broker report the block")}); err != nil {
		t.Fatalf("the publish that makes the broker report the block: %v", err)
	}
	waitCtx, cancel := context.WithTimeout(ctx, blockNoticeTimeout)
	defer cancel()
	select {
	case event := <-events:
		if !event.Active {
			t.Fatalf("the broker reported an unblock under an alarm: %+v", event)
		}
		t.Logf("the broker reported the publishing block %s after the publish, reason %q", time.Since(start), event.Reason) //nolint:forbidigo // the broker's own notification latency is one of the measurements this file reports.
	case <-waitCtx.Done():
		t.Fatalf("the broker did not report the publishing block within %s of a publish under the alarm", blockNoticeTimeout)
	}
	for recorded := time.Now().Add(blockNoticeTimeout); rabbitConn.blocked.Load() == nil && time.Now().Before(recorded); { //nolint:forbidigo // bounded re-check of the state the notification sets, see blockStatePoll.
		time.Sleep(blockStatePoll) //nolint:forbidigo // bounded re-check of the state the notification sets, see blockStatePoll.
	}
	if rabbitConn.blocked.Load() == nil {
		t.Fatalf("the broker reported the publishing block but the driver has not recorded it within %s", blockNoticeTimeout)
	}
	return time.Since(start) //nolint:forbidigo // the broker's own notification latency is one of the measurements this file reports.
}

// blockedMessage builds one message for destination. The id matters: the
// producer's window stops before a repeated MessageId, so a batch whose
// messages carry none is published one message at a time and never puts enough
// on the wire to fill a socket buffer. The core stamps a unique envelope id on
// every message it publishes, so this is the shape a caller's batch arrives in.
func blockedMessage(destination, id string) driver.OutboundMessage {
	return driver.OutboundMessage{
		Destination: destination,
		Headers:     []driver.Header{{Key: "id", Value: []byte(id)}},
		Body:        []byte("blocked"),
	}
}

// blockedBatch returns a batch whose total size is larger than the socket
// buffers on either side of the connection hold.
func blockedBatch(destination string) []driver.OutboundMessage {
	body := bytes.Repeat([]byte("b"), blockedMessageSize)
	messages := make([]driver.OutboundMessage, 0, blockedBatchMessages)
	for i := range blockedBatchMessages {
		message := blockedMessage(destination, fmt.Sprintf("blocked-%d", i))
		message.Body = body
		messages = append(messages, message)
	}
	return messages
}

// publishUnderDeadline publishes messages under a context deadline of its own
// and reports how long the call took. The clock is the measurement because the
// question is whether the caller gets its deadline back, and only a clock
// answers that.
func publishUnderDeadline(ctx context.Context, producer driver.Producer, deadline time.Duration, msgs ...driver.OutboundMessage) (time.Duration, error) {
	publishCtx, cancel := context.WithTimeout(ctx, deadline)
	defer cancel()
	start := time.Now() //nolint:forbidigo // the assertion is about wall-clock time around a live broker.
	err := producer.Publish(publishCtx, msgs...)
	return time.Since(start), err //nolint:forbidigo // the assertion is about wall-clock time around a live broker.
}

// openBlockedProducer creates a producer on conn and registers a cleanup that
// closes it, so a test can take its producers before it raises the alarm and
// have each of them torn down after the alarm is lifted: cleanup runs in
// reverse, and closing an AMQP channel is a round trip the broker cannot answer
// while it is blocking that connection.
func openBlockedProducer(t *testing.T, conn driver.Conn, ctx context.Context) driver.Producer {
	t.Helper()
	producer, err := conn.Producer(ctx, driver.ProducerConfig{})
	if err != nil {
		t.Fatalf("Producer: %v", err)
	}
	t.Cleanup(func() { _ = producer.Close(context.WithoutCancel(ctx)) })
	return producer
}

// assertBlockedByDeadline fails unless err is the driver's report of the
// publishing block and the caller's context is what ended the call: the four
// things every blocked path in this file has to carry back, checked in one
// place because being wrong about any of them is the same defect at each call
// site. The deadline is asserted as the cause, not only as the bound, because a
// transient failure that returns early would otherwise pass for the right
// answer.
func assertBlockedByDeadline(t *testing.T, operation string, err error) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s error = nil, want the publishing block reported", operation)
	}
	if kind, classified := driver.Classify(err); !classified || kind != driver.KindTransient {
		t.Fatalf("%s error = %v, want a transient error", operation, err)
	}
	if !strings.Contains(err.Error(), "blocked") {
		t.Fatalf("%s error = %v, want it to name the block", operation, err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("%s error = %v, want the caller's deadline to be what ended it", operation, err)
	}
}

// TestPingReportsBrokerPublishingBlock pins what an operator's health probe
// sees while the broker is blocking publishers, and what it sees once the alarm
// is cleared. Client.Health forwards Ping, so the probe carries the driver's own
// report of the block, the broker's reason included.
func TestPingReportsBrokerPublishingBlock(t *testing.T) {
	requireBroker(t)
	container := requireAlarmContainer(t)

	const destination = "rabbitmq-driver-blocked-health"
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	openBlockedDestination(t, destination)

	publicConn, err := (Driver{}).Open(ctx, driver.Config{Endpoints: []string{defaultEndpoint}})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = publicConn.Close(context.Background()) })

	restore := raiseMemoryAlarm(t, container)
	guard := time.AfterFunc(alarmGuardDelay, restore) //nolint:forbidigo // releases a publish stuck in a channel close the blocked broker will not answer, so a run that is about to fail still reports what it saw.
	defer guard.Stop()

	// The probe is taken before the connection has published under the alarm,
	// because that is the reading the broker's silence produces, and again
	// after it, which is the reading an operator's probe gets in the field.
	pingBefore := publicConn.Ping(ctx)
	t.Logf("Ping under the alarm, before the connection has published under it: %v", pingBefore)

	raiseBlockedConnection(t, ctx, publicConn, destination)

	pingAfter := publicConn.Ping(ctx)
	if pingAfter == nil {
		t.Fatalf("Ping under the alarm = nil, want the publishing block reported (Ping before the publish was %v)", pingBefore)
	}
	if !strings.Contains(pingAfter.Error(), "blocked") {
		t.Fatalf("Ping under the alarm = %v, want it to name the block", pingAfter)
	}
	if kind, classified := driver.Classify(pingAfter); !classified || kind != driver.KindTransient {
		t.Fatalf("Ping under the alarm = %v, want a transient error", pingAfter)
	}

	restore()
	waitCtx, waitCancel := context.WithTimeout(ctx, blockNoticeTimeout)
	defer waitCancel()
	// The wait parks on the connection's own state and ends when the broker's
	// unblock arrives, so this is a wait on the broker, not a poll of it.
	if err := publicConn.(*conn).awaitUnblocked(waitCtx); err != nil {
		t.Fatalf("waiting for the broker's unblock: %v", err)
	}
	if err := publicConn.Ping(ctx); err != nil {
		t.Fatalf("Ping after the alarm cleared = %v, want nil", err)
	}
}

// TestPublishDeadlineBoundsBlockedPublish pins that a caller's deadline is what
// ends a publish made while the broker is blocking publishers. amqp091's
// PublishWithContext checks the context only on entry, so what follows it is
// what the deadline has to reach.
func TestPublishDeadlineBoundsBlockedPublish(t *testing.T) {
	requireBroker(t)
	container := requireAlarmContainer(t)

	const destination = "rabbitmq-driver-blocked-deadline"
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	openBlockedDestination(t, destination)

	publicConn, err := (Driver{}).Open(ctx, driver.Config{Endpoints: []string{defaultEndpoint}})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = publicConn.Close(context.Background()) })

	cases := []struct {
		name     string
		messages []driver.OutboundMessage
	}{
		{name: "batch larger than the socket buffers", messages: blockedBatch(destination)},
		{name: "single small message", messages: []driver.OutboundMessage{blockedMessage(destination, "blocked-small")}},
	}
	// A producer of its own per case, taken before the alarm goes up so that
	// each is closed after it is lifted. The separation is not tidiness: a
	// publish that ends on the caller's deadline leaves the channel it was made
	// on unusable, and each case has to be measured on a channel the one before
	// it did not spoil.
	producers := make([]driver.Producer, len(cases))
	for i := range cases {
		producers[i] = openBlockedProducer(t, publicConn, ctx)
	}

	restore := raiseMemoryAlarm(t, container)
	guard := time.AfterFunc(alarmGuardDelay, restore) //nolint:forbidigo // releases a publish stuck in a channel close the blocked broker will not answer, so a run that is about to fail still reports what it saw.
	defer guard.Stop()

	raiseBlockedConnection(t, ctx, publicConn, destination)

	for index, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			elapsed, publishErr := publishUnderDeadline(ctx, producers[index], publishDeadline, testCase.messages...)
			t.Logf("Publish of %s returned after %s with %v", testCase.name, elapsed, publishErr)
			if elapsed > publishBound {
				t.Fatalf("Publish returned after %s, want it bounded by the %s deadline: %v", elapsed, publishDeadline, publishErr)
			}
			if publishErr == nil {
				t.Fatal("Publish error = nil, want the caller's deadline reported")
			}
			if kind, classified := driver.Classify(publishErr); !classified || kind != driver.KindTransient {
				t.Fatalf("Publish error = %v, want a transient error", publishErr)
			}
			if !strings.Contains(publishErr.Error(), "blocked") {
				t.Fatalf("Publish error = %v, want it to name the block", publishErr)
			}
		})
	}
}

// TestPublishDeadlineBoundsPublishIntoUnreportedBlock pins the calls that meet
// a publishing alarm, in the order the test runs them, and the state each one
// meets. All four come back on the caller's deadline.
//
// Cases 1, the first publish under the alarm, and 4, the batch, begin on a
// connection the broker has told nothing: no block is recorded when they start,
// and the confirmation they wait for is one the blocked broker is holding, so
// only the caller's context can end them. Case 1's own publish is what carries
// that connection over the line.
//
// Cases 2, a producer taken after case 1, and 3, a publish on a producer taken
// before the alarm, run on that connection afterwards, so they meet the block
// case 1 recorded, and their errors name it. The close subtest that follows
// measures conn.Close during that block: logged, not asserted.
func TestPublishDeadlineBoundsPublishIntoUnreportedBlock(t *testing.T) {
	requireBroker(t)
	container := requireAlarmContainer(t)

	const (
		firstDestination = "rabbitmq-driver-blocked-unreported"
		batchDestination = "rabbitmq-driver-blocked-midwindow"
	)
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	openBlockedDestination(t, firstDestination)
	openBlockedDestination(t, batchDestination)

	// Two connections, because two of the cases need one that has published
	// nothing under this alarm. The first carries the single publish and what
	// follows it; the second carries the batch on its own, since on the first
	// the block is recorded by then and the batch's gate would fire before its
	// first write, which is the case the test above already covers.
	firstConn, err := (Driver{}).Open(ctx, driver.Config{Endpoints: []string{defaultEndpoint}})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = firstConn.Close(context.Background()) })
	// Two producers on the first connection, both taken before the alarm: the
	// first publish closes the producer it was made on, so the case that
	// publishes on a producer taken before the alarm needs one of its own.
	firstProducer := openBlockedProducer(t, firstConn, ctx)
	spareProducer := openBlockedProducer(t, firstConn, ctx)

	batchConn, err := (Driver{}).Open(ctx, driver.Config{Endpoints: []string{defaultEndpoint}})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = batchConn.Close(context.Background()) })
	batchProducer := openBlockedProducer(t, batchConn, ctx)

	restore := raiseMemoryAlarm(t, container)
	guard := time.AfterFunc(alarmGuardDelay, restore) //nolint:forbidigo // releases a call stuck in a channel close the blocked broker will not answer, so a run that is about to fail still reports what it saw.
	defer guard.Stop()

	t.Run("the first publish after the alarm ends on its deadline", func(t *testing.T) {
		elapsed, publishErr := publishUnderDeadline(ctx, firstProducer, unreportedDeadline, blockedMessage(firstDestination, "unreported-first"))
		t.Logf("the first publish under the alarm returned after %s with %v", elapsed, publishErr)
		if elapsed > unreportedBound {
			t.Fatalf("Publish returned after %s, want it bounded by the %s deadline: %v", elapsed, unreportedDeadline, publishErr)
		}
		if publishErr == nil {
			t.Fatal("Publish error = nil, want the caller's deadline reported")
		}
		if kind, classified := driver.Classify(publishErr); !classified || kind != driver.KindTransient {
			t.Fatalf("Publish error = %v, want a transient error", publishErr)
		}
		// The block is not named here, and must not be: the broker had not told
		// this connection anything when the publish began, so what has to carry
		// the outcome back is the deadline itself.
		if !errors.Is(publishErr, context.DeadlineExceeded) {
			t.Fatalf("Publish error = %v, want the caller's deadline to be what ended it", publishErr)
		}
	})

	t.Run("a producer taken after the alarm reports the block", func(t *testing.T) {
		// The broker has told this connection it is blocking by now, so this is
		// the state the driver stays in for the rest of the alarm and the call
		// has to come back on its own deadline rather than on the unblock.
		producerCtx, producerCancel := context.WithTimeout(ctx, unreportedDeadline)
		defer producerCancel()
		start := time.Now() //nolint:forbidigo // the assertion is about wall-clock time around a live broker.
		_, producerErr := firstConn.Producer(producerCtx, driver.ProducerConfig{})
		elapsed := time.Since(start) //nolint:forbidigo // the assertion is about wall-clock time around a live broker.
		t.Logf("Producer under the alarm returned after %s with %v", elapsed, producerErr)
		if elapsed > unreportedBound {
			t.Fatalf("Producer returned after %s, want it bounded by the %s deadline: %v", elapsed, unreportedDeadline, producerErr)
		}
		assertBlockedByDeadline(t, "Producer", producerErr)
	})

	t.Run("a publish on a producer taken before the alarm reports the block", func(t *testing.T) {
		elapsed, publishErr := publishUnderDeadline(ctx, spareProducer, unreportedDeadline, blockedMessage(firstDestination, "unreported-spare"))
		t.Logf("Publish on the producer taken before the alarm returned after %s with %v", elapsed, publishErr)
		if elapsed > unreportedBound {
			t.Fatalf("Publish returned after %s, want it bounded by the %s deadline: %v", elapsed, unreportedDeadline, publishErr)
		}
		assertBlockedByDeadline(t, "Publish", publishErr)
	})

	t.Run("a batch that starts before the block is recorded ends on its deadline", func(t *testing.T) {
		elapsed, publishErr := publishUnderDeadline(ctx, batchProducer, publishDeadline, blockedBatch(batchDestination)...)
		t.Logf("the batch that met the unreported block returned after %s with %v", elapsed, publishErr)
		if elapsed > publishBound {
			t.Fatalf("Publish returned after %s, want it bounded by the %s deadline: %v", elapsed, publishDeadline, publishErr)
		}
		assertBlockedByDeadline(t, "Publish of the batch", publishErr)
	})

	t.Run("the connection close during the block", func(t *testing.T) {
		// A measurement and not an assertion: the close path is where it was
		// left, and a close that outlives its deadline belongs to the row that
		// owns that path. The connection is the batch's, which has a block
		// recorded and no producer left registered, so what this times is the
		// close rather than an admission check in front of it.
		closeCtx, closeCancel := context.WithTimeout(ctx, closeMeasureDeadline)
		defer closeCancel()
		start := time.Now() //nolint:forbidigo // the measurement is wall-clock time around a live broker.
		closeErr := batchConn.Close(closeCtx)
		elapsed := time.Since(start) //nolint:forbidigo // the measurement is wall-clock time around a live broker.
		t.Logf("conn.Close with a block recorded returned after %s with %v", elapsed, closeErr)
	})
}
