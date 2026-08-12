package obs

import (
	"sort"
	"sync/atomic"
)

// SampledCounter accumulates values from a hot path until a sampler takes the
// value. The zero value is ready for use. Do not copy after first use.
type SampledCounter struct {
	value atomic.Uint64
}

// Add records delta without allocating or taking a mutex.
func (c *SampledCounter) Add(delta uint64) {
	c.value.Add(delta)
}

// Sample returns the accumulated value and starts the next sampling window.
func (c *SampledCounter) Sample() uint64 {
	return c.value.Swap(0)
}

// SampledGauge keeps the largest observed value until a sampler takes it. The
// zero value is ready for use. Do not copy after first use.
type SampledGauge struct {
	value atomic.Uint64
}

// Observe records value as the current high-water mark without allocating or
// taking a mutex.
func (g *SampledGauge) Observe(value uint64) {
	for {
		current := g.value.Load()
		if value <= current {
			return
		}
		if g.value.CompareAndSwap(current, value) {
			return
		}
	}
}

// Sample returns the high-water mark and starts the next sampling window.
func (g *SampledGauge) Sample() uint64 {
	return g.value.Swap(0)
}

// SampledGaugeSet stores one high-water mark per immutable name/lane pair.
// The zero value is ready for use. Do not copy after first use.
type SampledGaugeSet struct {
	names  []string
	lanes  int
	gauges []SampledGauge
}

// NewSampledGaugeSet allocates the gauges and immutable index once. Runtime
// observations use binary search and never take a lock or allocate.
func NewSampledGaugeSet(names []string, lanes int) SampledGaugeSet {
	if lanes <= 0 {
		return SampledGaugeSet{}
	}
	sorted := append([]string(nil), names...)
	sort.Strings(sorted)
	unique := sorted[:0]
	for _, name := range sorted {
		if len(unique) == 0 || unique[len(unique)-1] != name {
			unique = append(unique, name)
		}
	}
	return SampledGaugeSet{
		names:  unique,
		lanes:  lanes,
		gauges: make([]SampledGauge, len(unique)*lanes),
	}
}

func (s *SampledGaugeSet) index(name string, lane int) (int, bool) {
	if s == nil || lane < 0 || lane >= s.lanes {
		return 0, false
	}
	nameIndex := sort.SearchStrings(s.names, name)
	if nameIndex >= len(s.names) || s.names[nameIndex] != name {
		return 0, false
	}
	return nameIndex*s.lanes + lane, true
}

// Observe records a high-water value for one name/lane pair.
func (s *SampledGaugeSet) Observe(name string, lane int, value uint64) {
	index, ok := s.index(name, lane)
	if ok {
		s.gauges[index].Observe(value)
	}
}

// Sample returns and resets one name/lane pair.
func (s *SampledGaugeSet) Sample(name string, lane int) uint64 {
	index, ok := s.index(name, lane)
	if !ok {
		return 0
	}
	return s.gauges[index].Sample()
}
