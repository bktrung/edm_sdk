package f1

import (
	"context"
	"maps"
	"slices"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

// backlogTarget is one consume destination the poll samples: the logical
// topic and lane behind one physical destination string.
type backlogTarget struct {
	topic    string
	priority Priority
}

// pollBacklog samples one runner generation's consumer backlog on the client
// clock's ticker and records one backlog_sampled per consume destination
// present in the result. It returns when ctx ends.
//
// The destination table is built once from the subscription's topics and
// priorities through consumeDestination, the same derivation
// consumingTopicFamily uses, so destination strings are never parsed. A
// retry-tier destination in the result is skipped, a driver error skips the
// tick silently, and an optional head timestamp is converted to client-clock
// age without reporting a negative duration.
//
// The start site in Run checks the observer and the interval once, so this
// function takes them as given: r, ctx, the consumer, the client clock and
// a positive interval are all set by the caller.
func pollBacklog(r *Runner, ctx context.Context, consumer driver.Consumer) {
	client := r.client
	interval := client.backlogPollInterval
	clk := client.options.clock
	client.mu.Lock()
	effective := client.effective
	source := client.source
	client.mu.Unlock()
	subscription := r.subscription
	targets := make(map[string]backlogTarget, len(subscription.Topics)*len(subscription.Priorities))
	for _, topic := range subscription.Topics {
		logical := topicFor(topic)
		for _, priority := range subscription.Priorities {
			destination := consumeDestination(effective, source, logical, priority, subscription.Name)
			targets[destination] = backlogTarget{topic: logical, priority: priority}
		}
	}
	backlogReader, hasBacklog := consumer.(driver.BacklogReader)
	ticker := clk.Ticker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		// Bound a broker call to this tick so a stuck call cannot outlive the
		// generation. The parent is the poll's own context, so teardown
		// cancellation stops both the tick wait and the call.
		lagCtx, cancel := context.WithTimeout(ctx, interval)
		var (
			lag     map[string]int64
			backlog map[string]driver.BacklogSample
			err     error
		)
		if hasBacklog {
			backlog, err = backlogReader.Backlog(lagCtx)
		} else {
			lag, err = consumer.Lag(lagCtx)
		}
		cancel()
		if err != nil {
			continue
		}
		now := clk.Now()
		if hasBacklog {
			if len(backlog) == 0 {
				continue
			}
			for _, destination := range slices.Sorted(maps.Keys(backlog)) {
				target, ok := targets[destination]
				if !ok {
					continue
				}
				client.observeRecord(backlogPoint(now, subscription.Name, target, backlog[destination]))
			}
			continue
		}
		if len(lag) == 0 {
			continue
		}
		for _, destination := range slices.Sorted(maps.Keys(lag)) {
			target, ok := targets[destination]
			if !ok {
				continue
			}
			client.observeRecord(backlogPoint(now, subscription.Name, target, driver.BacklogSample{Lag: lag[destination]}))
		}
	}
}

func backlogPoint(now time.Time, subscription string, target backlogTarget, sample driver.BacklogSample) PointEvent {
	event := PointEvent{
		Kind:         ObserverBacklogSampled,
		At:           now,
		Topic:        target.topic,
		Subscription: subscription,
		Priority:     target.priority,
		Backlog:      sample.Lag,
	}
	if sample.HeadEnqueuedAt.IsZero() {
		return event
	}
	event.HeadAgeKnown = true
	event.HeadAge = max(now.Sub(sample.HeadEnqueuedAt), 0)
	event.EnqueuedAtSource = observerEnqueueSource(sample.HeadSource)
	return event
}
