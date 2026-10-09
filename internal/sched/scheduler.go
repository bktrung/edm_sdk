package sched

import (
	"errors"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

// Scheduler selects queued items using smooth weighted round robin and
// optional deadline promotion. Smoothness costs a full scan of every group on
// each weighted pick, so selection work grows linearly with group count. In the
// disabled-promotion weighted-pick benchmark's six-group, two-lanes-per-group
// shape, this implementation measured 21.53 ns/op and the previous scheme
// measured 17.84 ns/op; their three-run spreads overlapped. At sixty groups,
// the disabled-promotion weighted-pick measurements were 118.8 ns/op and
// 16.59 ns/op, respectively. The zero value has no selectable item, and all
// methods are intended for one goroutine.
type Scheduler struct {
	clock          clock.Clock
	promoteOverdue bool
	slots          []*slot
	byID           map[string]*lane
	byGroup        map[string]*slot
}

type slot struct {
	id      string
	weight  int
	lanes   []*lane
	cursor  int
	deficit int
}

// New creates a scheduler from lane specifications.
func New(specs []LaneSpec, clk clock.Clock, promoteOverdue bool) (*Scheduler, error) {
	if len(specs) == 0 {
		return nil, errors.New("sched: at least one lane is required")
	}
	if clk == nil {
		return nil, errors.New("sched: clock is required")
	}
	s := &Scheduler{clock: clk, promoteOverdue: promoteOverdue, byID: make(map[string]*lane), byGroup: make(map[string]*slot)}
	for _, spec := range specs {
		if _, exists := s.byID[spec.ID]; exists {
			return nil, errors.New("sched: duplicate lane id")
		}
		lane, err := newLane(spec)
		if err != nil {
			return nil, err
		}
		group := spec.Group
		if group == "" {
			group = spec.ID
		}
		groupSlot := s.byGroup[group]
		if groupSlot == nil {
			groupSlot = &slot{id: group, weight: spec.Weight}
			s.byGroup[group] = groupSlot
			s.slots = append(s.slots, groupSlot)
		} else if groupSlot.weight != spec.Weight {
			return nil, errors.New("sched: grouped lanes must share a weight")
		}
		groupSlot.lanes = append(groupSlot.lanes, lane)
		s.byID[spec.ID] = lane
	}
	return s, nil
}

// Enqueue appends an item to a bounded lane.
func (s *Scheduler) Enqueue(id string, item Item) error {
	lane := s.byID[id]
	if lane == nil {
		return errors.New("sched: unknown lane")
	}
	if item.EnqueuedAt.IsZero() {
		item.EnqueuedAt = s.clock.Now()
	}
	return lane.enqueue(item)
}

// Depth returns the number of queued items in one lane.
func (s *Scheduler) Depth(id string) int {
	if lane := s.byID[id]; lane != nil {
		return len(lane.items)
	}
	return 0
}

// Pending returns the total number of queued items across all lanes.
func (s *Scheduler) Pending() int {
	if s == nil {
		return 0
	}
	total := 0
	for _, group := range s.slots {
		for _, lane := range group.lanes {
			total += len(lane.items)
		}
	}
	return total
}

// Promotion reports a deadline promotion decision from Next. LaneID is the
// overdue lane that was served and Depth is that lane's depth before the pop,
// so it counts the promoted head. The zero value means the pick was not a
// deadline promotion.
type Promotion struct {
	LaneID string
	Depth  int
}

// Next returns the next item, the promotion that produced it, and whether an
// item was available. Promotion is zero when the pick was not a deadline
// promotion.
func (s *Scheduler) Next() (Item, Promotion, bool) {
	if len(s.slots) == 0 {
		return Item{}, Promotion{}, false
	}
	if s.promoteOverdue {
		if promoted := s.mostOverdue(); promoted != nil {
			depth := len(promoted.items)
			item := promoted.pop()
			return item, Promotion{LaneID: promoted.spec.ID, Depth: depth}, true
		}
	}
	var selected *slot
	totalWeight := 0
	for _, group := range s.slots {
		if group.empty() {
			// An empty group forfeits its whole deficit, positive credit and negative
			// debt alike. A bursty group that refills starts from zero rather than
			// spending credit or repaying debt from before it drained.
			group.deficit = 0
			continue
		}
		group.deficit += group.weight
		totalWeight += group.weight
		if selected == nil || group.deficit > selected.deficit {
			selected = group
		}
	}
	if selected == nil {
		return Item{}, Promotion{}, false
	}
	selected.deficit -= totalWeight
	return selected.pop(), Promotion{}, true
}

func (s *Scheduler) mostOverdue() *lane {
	now := s.clock.Now()
	var selected *lane
	var selectedOverrun time.Duration
	for _, group := range s.slots {
		groupEmpty := true
		for _, lane := range group.lanes {
			if len(lane.items) == 0 {
				continue
			}
			groupEmpty = false
			if lane.spec.Budget <= 0 {
				continue
			}
			overrun := now.Sub(lane.items[0].EnqueuedAt) - lane.spec.Budget
			if overrun < 0 || selected != nil && overrun <= selectedOverrun {
				continue
			}
			selected = lane
			selectedOverrun = overrun
		}
		if groupEmpty {
			// An empty group forfeits its whole deficit, positive credit and negative
			// debt alike. A bursty group that refills starts from zero rather than
			// spending credit or repaying debt from before it drained.
			group.deficit = 0
		}
	}
	return selected
}

func (s *slot) empty() bool {
	for _, lane := range s.lanes {
		if len(lane.items) > 0 {
			return false
		}
	}
	return true
}

func (s *slot) pop() Item {
	for checked := range len(s.lanes) {
		index := (s.cursor + checked) % len(s.lanes)
		lane := s.lanes[index]
		if len(lane.items) == 0 {
			continue
		}
		s.cursor = (index + 1) % len(s.lanes)
		return lane.pop()
	}
	return Item{}
}
