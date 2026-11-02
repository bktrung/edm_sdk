package sched

import (
	"errors"
	"time"
)

// ErrLaneFull indicates that a bounded lane cannot accept another item yet.
var ErrLaneFull = errors.New("sched: lane buffer is full")

// LaneSpec configures one bounded scheduler lane.
type LaneSpec struct {
	ID       string
	Group    string
	Weight   int
	Budget   time.Duration
	Capacity int
}

// Item is one message waiting in a lane. EnqueuedAt drives deadline promotion.
type Item struct {
	Value      any
	EnqueuedAt time.Time
}

// lane is a fixed circular buffer of spec.Capacity items. The buffer is
// allocated once so a bounded lane never reallocates as it fills and drains;
// it does not grow, because Capacity is already the admission bound.
type lane struct {
	spec  LaneSpec
	ring  []Item
	head  int
	count int
}

func newLane(spec LaneSpec) (*lane, error) {
	if spec.ID == "" {
		return nil, errors.New("sched: lane id must not be empty")
	}
	if spec.Weight < 1 {
		return nil, errors.New("sched: lane weight must be positive")
	}
	if spec.Capacity < 1 {
		spec.Capacity = 1
	}
	return &lane{spec: spec, ring: make([]Item, spec.Capacity)}, nil
}

func (l *lane) depth() int {
	return l.count
}

// oldest returns the head item. The lane must not be empty.
func (l *lane) oldest() Item {
	return l.ring[l.head]
}

func (l *lane) enqueue(item Item) error {
	if l.count == len(l.ring) {
		return ErrLaneFull
	}
	tail := l.head + l.count
	if tail >= len(l.ring) {
		tail -= len(l.ring)
	}
	l.ring[tail] = item
	l.count++
	return nil
}

func (l *lane) pop() Item {
	item := l.ring[l.head]
	// Clear the vacated slot so the buffer does not keep the popped Value
	// reachable until the slot is overwritten.
	l.ring[l.head] = Item{}
	l.head++
	if l.head == len(l.ring) {
		l.head = 0
	}
	l.count--
	return item
}
