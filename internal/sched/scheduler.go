package sched

import (
	"errors"
	"sort"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

// Scheduler selects queued items using deficit weighted round robin and
// optional age-based promotion. All methods are intended for one goroutine.
type Scheduler struct {
	clock   clock.Clock
	aging   bool
	slots   []*slot
	byID    map[string]*lane
	byGroup map[string]*slot
	cursor  int
	quantum int
}

type slot struct {
	id      string
	weight  int
	lanes   []*lane
	cursor  int
	deficit int
}

// New creates a scheduler from lane specifications.
func New(specs []LaneSpec, clk clock.Clock, aging bool) (*Scheduler, error) {
	if len(specs) == 0 {
		return nil, errors.New("sched: at least one lane is required")
	}
	if clk == nil {
		return nil, errors.New("sched: clock is required")
	}
	s := &Scheduler{clock: clk, aging: aging, byID: make(map[string]*lane), byGroup: make(map[string]*slot), quantum: 1}
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

// Next returns the next item, or false when every lane is empty.
func (s *Scheduler) Next() (Item, bool) {
	if len(s.slots) == 0 {
		return Item{}, false
	}
	if s.aging {
		if promoted := s.promoted(); len(promoted) > 0 {
			return promoted[0].popPromoted(s.clock.Now()), true
		}
	}
	for checked := 0; checked < len(s.slots); checked++ {
		index := (s.cursor + checked) % len(s.slots)
		group := s.slots[index]
		if group.empty() {
			group.deficit = 0
			continue
		}
		if group.deficit == 0 {
			group.deficit += group.weight * s.quantum
		}
		group.deficit--
		if group.deficit == 0 {
			s.cursor = (index + 1) % len(s.slots)
		} else {
			s.cursor = index
		}
		return group.pop(), true
	}
	return Item{}, false
}

func (s *Scheduler) promoted() []*slot {
	now := s.clock.Now()
	urgent := make([]*slot, 0)
	for _, group := range s.slots {
		if group.empty() {
			group.deficit = 0
			continue
		}
		if group.promotedLane(now) != nil {
			urgent = append(urgent, group)
		}
	}
	sort.SliceStable(urgent, func(i, j int) bool {
		left := urgent[i].overrun(now)
		right := urgent[j].overrun(now)
		return left > right
	})
	return urgent
}

func (s *slot) promotedLane(now time.Time) *lane {
	var selected *lane
	var selectedOverrun time.Duration
	for _, lane := range s.lanes {
		if len(lane.items) == 0 || lane.spec.Budget <= 0 {
			continue
		}
		overrun := now.Sub(lane.items[0].EnqueuedAt) - lane.spec.Budget
		if overrun < 0 || selected != nil && overrun <= selectedOverrun {
			continue
		}
		selected = lane
		selectedOverrun = overrun
	}
	return selected
}

func (s *slot) overrun(now time.Time) time.Duration {
	lane := s.promotedLane(now)
	if lane == nil {
		return 0
	}
	return now.Sub(lane.items[0].EnqueuedAt) - lane.spec.Budget
}

func (s *slot) empty() bool {
	for _, lane := range s.lanes {
		if len(lane.items) > 0 {
			return false
		}
	}
	return true
}

func (s *slot) head() Item {
	var head Item
	set := false
	for _, lane := range s.lanes {
		if len(lane.items) == 0 {
			continue
		}
		if !set || lane.items[0].EnqueuedAt.Before(head.EnqueuedAt) {
			head = lane.items[0]
			set = true
		}
	}
	return head
}

func (s *slot) pop() Item {
	for checked := 0; checked < len(s.lanes); checked++ {
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

func (s *slot) popPromoted(now time.Time) Item {
	if lane := s.promotedLane(now); lane != nil {
		return lane.pop()
	}
	return Item{}
}
