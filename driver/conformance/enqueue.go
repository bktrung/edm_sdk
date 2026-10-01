package conformance

import (
	"context"
	"fmt"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

const (
	enqueueEarlyMargin = 2 * time.Second
	enqueueLateMargin  = time.Second
)

func init() { registerGroup("enqueue", runEnqueue) }

func runEnqueue(group *groupContext) {
	group.Check("known enqueue time is in the publish and receive window", func(t *testing.T) {
		name := "enqueue.known"
		producer := newProducer(t, group, profileDestination(group, name), driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumer(t, group, profileDestination(group, name), 1)
		publishedAt := deferredNow(group)
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: name}); err != nil {
			t.Fatal(err)
		}
		message := receiveMessage(t, group, consumer)
		ackMessage(t, group, message)
		if message.EnqueuedAtSource == driver.EnqueueSourceUnknown {
			return
		}
		if message.EnqueuedAt.IsZero() {
			t.Fatalf("EnqueuedAt is zero for source %s", message.EnqueuedAtSource)
		}
		if message.EnqueuedAt.Before(publishedAt.Add(-enqueueEarlyMargin)) {
			t.Fatalf("EnqueuedAt=%s, before publish window starting %s", message.EnqueuedAt, publishedAt.Add(-enqueueEarlyMargin))
		}
		if message.EnqueuedAt.After(message.ReceivedAt.Add(enqueueLateMargin)) {
			t.Fatalf("EnqueuedAt=%s, after receive window ending %s", message.EnqueuedAt, message.ReceivedAt.Add(enqueueLateMargin))
		}
	})

	group.Check("unknown enqueue time is paired with an unknown source", func(t *testing.T) {
		name := "enqueue.unknown"
		producer := newProducer(t, group, profileDestination(group, name), driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumer(t, group, profileDestination(group, name), 1)
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: name}); err != nil {
			t.Fatal(err)
		}
		message := receiveMessage(t, group, consumer)
		ackMessage(t, group, message)
		if message.EnqueuedAt.IsZero() && message.EnqueuedAtSource != driver.EnqueueSourceUnknown {
			t.Fatalf("zero EnqueuedAt has source %s, want unknown", message.EnqueuedAtSource)
		}
		if message.EnqueuedAtSource == driver.EnqueueSourceUnknown && !message.EnqueuedAt.IsZero() {
			t.Fatalf("unknown source has EnqueuedAt=%s, want zero", message.EnqueuedAt)
		}
	})

	group.Check("backlog counts match Lag", func(t *testing.T) {
		name := "enqueue.backlog-lag"
		producer := newProducer(t, group, profileDestination(group, name), driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumer(t, group, profileDestination(group, name), 1)
		reader, available := backlogReader(t, group, consumer)
		if !available {
			return
		}
		if err := consumer.Drain(group.ctx); err != nil {
			t.Fatal(err)
		}
		publishCount(t, group, producer, name, 3)
		waitForLag(t, group, consumer, name, 3)
		lag, err := consumer.Lag(group.ctx)
		if err != nil {
			t.Fatal(err)
		}
		backlog, err := reader.Backlog(group.ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(lag) != len(backlog) {
			t.Fatalf("Lag keys=%v, Backlog keys=%v", lag, backlog)
		}
		for destination, count := range lag {
			sample, ok := backlog[destination]
			if !ok {
				t.Fatalf("Backlog keys=%v, missing %q", backlog, destination)
			}
			if sample.Lag != count {
				t.Fatalf("Backlog[%q].Lag=%d, Lag=%d", destination, sample.Lag, count)
			}
		}
	})

	group.Check("backlog head is in the publish window or unknown", func(t *testing.T) {
		name := "enqueue.head-window"
		producer := newProducer(t, group, profileDestination(group, name), driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumer(t, group, profileDestination(group, name), 1)
		reader, available := backlogReader(t, group, consumer)
		if !available {
			return
		}
		if err := consumer.Drain(group.ctx); err != nil {
			t.Fatal(err)
		}
		publishedAt := deferredNow(group)
		publishCount(t, group, producer, name, 3)
		publishedUntil := deferredNow(group)
		waitForLag(t, group, consumer, name, 3)
		backlog, err := reader.Backlog(group.ctx)
		if err != nil {
			t.Fatal(err)
		}
		sample, ok := backlog[name]
		if !ok {
			t.Fatalf("Backlog keys=%v, missing %q", backlog, name)
		}
		assertBacklogHead(t, sample, publishedAt, publishedUntil)
	})

	group.Check("empty destination has an unknown backlog head", func(t *testing.T) {
		name := "enqueue.empty-head"
		newProducer(t, group, profileDestination(group, name), driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumer(t, group, profileDestination(group, name), 1)
		reader, available := backlogReader(t, group, consumer)
		if !available {
			return
		}
		if err := consumer.Drain(group.ctx); err != nil {
			t.Fatal(err)
		}
		backlog, err := reader.Backlog(group.ctx)
		if err != nil {
			t.Fatal(err)
		}
		sample, ok := backlog[name]
		if !ok {
			t.Fatalf("Backlog keys=%v, missing %q", backlog, name)
		}
		if sample.Lag != 0 || !sample.HeadEnqueuedAt.IsZero() || sample.HeadSource != driver.EnqueueSourceUnknown {
			t.Fatalf("Backlog[%q]=%+v, want zero lag and unknown head", name, sample)
		}
	})

	group.Check("backlog head does not get younger", func(t *testing.T) {
		name := "enqueue.head-order"
		producer := newProducer(t, group, profileDestination(group, name), driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumer(t, group, profileDestination(group, name), 1)
		reader, available := backlogReader(t, group, consumer)
		if !available {
			return
		}
		if err := consumer.Pause(name); err != nil {
			t.Fatal(err)
		}
		waitForStable(t, group, "paused destination before backlog publish", func() (bool, string) {
			select {
			case message, ok := <-consumer.Messages():
				if !ok {
					return false, "Messages channel closed"
				}
				return false, fmt.Sprintf("received destination=%q before publish", message.Destination)
			default:
				return true, "no message before publish"
			}
		})
		publishDistinctCount(t, group, producer, name, 3)
		waitForLagAtLeast(t, group, consumer, name, 2)
		first, err := reader.Backlog(group.ctx)
		if err != nil {
			t.Fatal(err)
		}
		firstSample, ok := first[name]
		if !ok {
			t.Fatalf("first Backlog keys=%v, missing %q", first, name)
		}
		if err := consumer.Resume(name); err != nil {
			t.Fatal(err)
		}
		message := receiveMessage(t, group, consumer)
		if err := consumer.Pause(name); err != nil {
			t.Fatal(err)
		}
		if !firstSample.HeadEnqueuedAt.IsZero() && !message.EnqueuedAt.IsZero() &&
			firstSample.HeadEnqueuedAt.After(message.EnqueuedAt.Add(enqueueLateMargin)) {
			t.Fatalf("first backlog head=%s is newer than first delivery=%s", firstSample.HeadEnqueuedAt, message.EnqueuedAt)
		}
		ackMessage(t, group, message)
		var secondSample driver.BacklogSample
		waitFor(t, group, "backlog after settling the first message", func() (bool, string) {
			backlog, backlogErr := reader.Backlog(group.ctx)
			if backlogErr != nil {
				return false, backlogErr.Error()
			}
			var found bool
			secondSample, found = backlog[name]
			if !found {
				return false, fmt.Sprintf("Backlog keys=%v, missing %q", backlog, name)
			}
			return secondSample.Lag < firstSample.Lag, fmt.Sprintf("Backlog[%q]=%+v", name, secondSample)
		})
		if firstSample.HeadEnqueuedAt.IsZero() || secondSample.HeadEnqueuedAt.IsZero() {
			return
		}
		if secondSample.HeadEnqueuedAt.Before(firstSample.HeadEnqueuedAt) {
			t.Fatalf("backlog head moved from %s to younger %s", firstSample.HeadEnqueuedAt, secondSample.HeadEnqueuedAt)
		}
	})
}

func backlogReader(t *testing.T, group *groupContext, consumer driver.Consumer) (driver.BacklogReader, bool) {
	t.Helper()
	wrapped, ok := consumer.(*profileConsumer)
	if !ok {
		t.Fatalf("consumer has type %T, want profileConsumer", consumer)
	}
	tracked, ok := wrapped.consumer.(*trackedConsumer)
	if !ok {
		t.Fatalf("profile consumer has type %T, want trackedConsumer", wrapped.consumer)
	}
	reader, ok := tracked.Consumer.(driver.BacklogReader)
	if !ok || !group.effective.LagQueryable {
		group.Skip(t, group.currentCheck, "driver.BacklogReader is not available in this profile")
		return nil, false
	}
	return &profileBacklogReader{reader: reader, logical: wrapped.logical}, true
}

type profileBacklogReader struct {
	reader  driver.BacklogReader
	logical map[string]string
}

func (r *profileBacklogReader) Backlog(ctx context.Context) (map[string]driver.BacklogSample, error) {
	physical, err := r.reader.Backlog(ctx)
	if err != nil {
		return nil, err
	}
	logical := make(map[string]driver.BacklogSample, len(physical))
	for destination, sample := range physical {
		if name, ok := r.logical[destination]; ok {
			destination = name
		}
		logical[destination] = sample
	}
	return logical, nil
}

func waitForLag(t *testing.T, group *groupContext, consumer driver.Consumer, destination string, want int64) {
	t.Helper()
	waitFor(t, group, fmt.Sprintf("Lag(%q) to reach %d", destination, want), func() (bool, string) {
		lag, err := consumer.Lag(group.ctx)
		if err != nil {
			return false, err.Error()
		}
		got, ok := lag[destination]
		if !ok {
			return false, fmt.Sprintf("Lag keys=%v, missing %q", lag, destination)
		}
		return got == want, fmt.Sprintf("Lag[%q]=%d", destination, got)
	})
}

func waitForLagAtLeast(t *testing.T, group *groupContext, consumer driver.Consumer, destination string, want int64) {
	t.Helper()
	waitFor(t, group, fmt.Sprintf("Lag(%q) to reach at least %d", destination, want), func() (bool, string) {
		lag, err := consumer.Lag(group.ctx)
		if err != nil {
			return false, err.Error()
		}
		got, ok := lag[destination]
		if !ok {
			return false, fmt.Sprintf("Lag keys=%v, missing %q", lag, destination)
		}
		return got >= want, fmt.Sprintf("Lag[%q]=%d", destination, got)
	})
}

func publishDistinctCount(t *testing.T, group *groupContext, producer driver.Producer, destination string, count int) {
	t.Helper()
	for i := range count {
		if err := producer.Publish(group.ctx, driver.OutboundMessage{
			Destination: destination,
			Body:        fmt.Appendf(nil, "%s-%d", destination, i),
		}); err != nil {
			t.Fatal(err)
		}
		if i+1 < count && group.deadline != nil {
			group.deadline.Advance(time.Second)
		}
	}
}

func assertBacklogHead(t *testing.T, sample driver.BacklogSample, publishedAt, publishedUntil time.Time) {
	t.Helper()
	if sample.HeadEnqueuedAt.IsZero() {
		if sample.HeadSource != driver.EnqueueSourceUnknown {
			t.Fatalf("zero HeadEnqueuedAt has source %s, want unknown", sample.HeadSource)
		}
		return
	}
	if sample.HeadSource == driver.EnqueueSourceUnknown {
		t.Fatalf("known HeadEnqueuedAt=%s has unknown source", sample.HeadEnqueuedAt)
	}
	if sample.HeadEnqueuedAt.Before(publishedAt.Add(-enqueueEarlyMargin)) {
		t.Fatalf("HeadEnqueuedAt=%s, before publish window starting %s", sample.HeadEnqueuedAt, publishedAt.Add(-enqueueEarlyMargin))
	}
	if sample.HeadEnqueuedAt.After(publishedUntil.Add(enqueueLateMargin)) {
		t.Fatalf("HeadEnqueuedAt=%s, after publish window ending %s", sample.HeadEnqueuedAt, publishedUntil.Add(enqueueLateMargin))
	}
}
