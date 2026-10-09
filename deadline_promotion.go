package f1

import (
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/sched"
)

// deadlinePromotionWindow bounds deadline_promoted to one event per lane per
// fixed window on the client clock. The window starts at the first emitted
// event for the lane, not at the last promotion, so promotions inside a window
// that already emitted only raise the suppressed count carried on the next
// emitted event.
const deadlinePromotionWindow = 15 * time.Second

// laneWindow tracks one lane's rate limit: the start of its current fixed
// window and how many promotions were dropped inside it.
type laneWindow struct {
	start      time.Time
	suppressed int
}

// promotionLimiter rate-limits deadline promotions per lane. The lane table
// maps a scheduler lane ID to the logical topic and priority to report. The
// dispatch loop is one goroutine, so the limiter needs no lock: it is built
// once per pipeline and touched only by that loop.
type promotionLimiter struct {
	subscription string
	lanes        map[string]runnerLane
	windows      map[string]*laneWindow
}

// newPromotionLimiter builds the limiter for one dispatch pipeline. The caller
// checks the observer: runDispatchPipeline calls it only when
// r.client.observer is set, which is what keeps the nil path free of maps.
func newPromotionLimiter(r *Runner) *promotionLimiter {
	plan := runnerLanePlan(r)
	table := make(map[string]runnerLane, len(plan))
	for _, lane := range plan {
		table[lane.id] = lane
	}
	return &promotionLimiter{
		subscription: r.subscription.Name,
		lanes:        table,
		windows:      make(map[string]*laneWindow),
	}
}

// promotionEvent maps one scheduler promotion to the point event to record.
// The caller guarantees a promotion: the worker checks prom.LaneID before
// calling, which is what keeps the hot path free of extra branches. A lane ID
// the table does not know is refused: the scheduler served a lane the plan did
// not derive, so there is no topic or priority to report. EnqueuedAt is the
// promoted item's enqueue time and now is the client clock.
func (l *promotionLimiter) promotionEvent(prom sched.Promotion, enqueuedAt, now time.Time) (PointEvent, bool) {
	lane, ok := l.lanes[prom.LaneID]
	if !ok {
		return PointEvent{}, false
	}
	suppressed := 0
	if window, ok := l.windows[prom.LaneID]; ok {
		if now.Sub(window.start) < deadlinePromotionWindow {
			window.suppressed++
			return PointEvent{}, false
		}
		suppressed = window.suppressed
		window.suppressed = 0
		window.start = now
	} else {
		l.windows[prom.LaneID] = &laneWindow{start: now}
	}
	return PointEvent{
		Kind:         ObserverDeadlinePromoted,
		At:           now,
		Topic:        lane.topic,
		Subscription: l.subscription,
		Priority:     lane.priority,
		LaneWait:     now.Sub(enqueuedAt),
		LaneDepth:    prom.Depth,
		Suppressed:   suppressed,
	}, true
}
