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
// The destination table is the consume destinations this generation's
// consumer was opened on, read once from the runner's destination metadata,
// so capabilities that moved since the open cannot rename them and destination
// strings are never parsed. A retry-tier destination in the result is skipped, a driver error skips the
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
	r.mu.Lock()
	targets := make(map[string]backlogTarget, len(r.destinationMetadata))
	for destination, metadata := range r.destinationMetadata {
		if metadata.tier == 0 {
			targets[destination] = backlogTarget{topic: metadata.topic, priority: metadata.priority}
		}
	}
	r.mu.Unlock()
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
			backlog map[string]driver.BacklogSample
			err     error
		)
		if hasBacklog {
			backlog, err = backlogReader.Backlog(lagCtx)
		} else {
			// The legacy read reports counts only, so it is normalised into
			// the same sample shape here and both paths share one loop below:
			// a second loop is a second place for the target lookup and the
			// observation to drift.
			var lag map[string]int64
			lag, err = consumer.Lag(lagCtx)
			if len(lag) > 0 {
				backlog = make(map[string]driver.BacklogSample, len(lag))
				for destination, count := range lag {
					backlog[destination] = driver.BacklogSample{Lag: count}
				}
			}
		}
		cancel()
		if err != nil {
			continue
		}
		if len(backlog) == 0 {
			continue
		}
		now := clk.Now()
		for _, destination := range slices.Sorted(maps.Keys(backlog)) {
			target, ok := targets[destination]
			if !ok {
				continue
			}
			client.observeRecord(backlogPoint(now, r.subscription.Name, target, backlog[destination]))
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
