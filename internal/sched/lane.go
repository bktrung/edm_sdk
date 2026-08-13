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

// Item is one message waiting in a lane. EnqueuedAt drives aging.
type Item struct {
	Value      any
	EnqueuedAt time.Time
}

type lane struct {
	spec    LaneSpec
	items   []Item
	deficit int
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
	return &lane{spec: spec}, nil
}

func (l *lane) enqueue(item Item) error {
	if len(l.items) >= l.spec.Capacity {
		return ErrLaneFull
	}
	l.items = append(l.items, item)
	return nil
}

func (l *lane) pop() Item {
	item := l.items[0]
	l.items[0] = Item{}
	l.items = l.items[1:]
	return item
}
