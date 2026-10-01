package f1

const automaticPrefetchOverflow = maxPrefetch + 1

// laneSizing keeps configuration validation and runner windows on the same
// calculation. Retry tiers share one scheduling group, but each tier has
// its own destination window.
type laneSizing struct {
	weights     map[Priority]int
	concurrency int
	totalWeight int
	divisor     int
	factor      int
}

func boundedAdd(a, b int) int {
	if a >= automaticPrefetchOverflow || b >= automaticPrefetchOverflow-a {
		return automaticPrefetchOverflow
	}
	return a + b
}

func boundedMul(a, b int) int {
	if a <= 0 || b <= 0 {
		return 0
	}
	if a > automaticPrefetchOverflow/b {
		return automaticPrefetchOverflow
	}
	return a * b
}

func subscriptionLaneSizing(sub SubscriptionConfig) laneSizing {
	s := laneSizing{
		weights:     sub.Fairness.Weights,
		concurrency: sub.Concurrency,
		divisor:     sub.Fairness.RetryWeightDivisor,
		factor:      sub.Fairness.PrefetchFactor,
	}
	if s.divisor < 1 {
		s.divisor = defaultRetryWeightDivisor
	}
	if s.factor < 1 {
		s.factor = defaultPrefetchFactor
	}
	// The total is kept exact: a total clipped to the prefetch bound would
	// overstate every lane's share. Validation bounds each weight by
	// maxFairnessWeight, so it cannot overflow for any number of topics.
	for _, priority := range sub.Priorities {
		s.totalWeight += s.mainWeight(priority)
		if sub.Retry.tierCount() > 0 {
			s.totalWeight += s.retryWeight(priority)
		}
	}
	s.totalWeight = max(s.totalWeight*len(sub.Topics), 1)
	return s
}

func (s laneSizing) mainWeight(priority Priority) int {
	return max(s.weights[priority], 1)
}

func (s laneSizing) retryWeight(priority Priority) int {
	return max(s.mainWeight(priority)/s.divisor, 1)
}

// capacity is a lane's share of the handler slots, rounded up and floored at
// minimumLaneCapacity, times the prefetch factor. The share is computed
// exactly: validation bounds concurrency by 1024 and a weight by
// maxFairnessWeight, so concurrency x weight stays below 2^27. Only the final
// multiplication by the factor, which validation does not bound, saturates.
// Clipping the product before the division instead would shrink the share of
// any lane whose product passed the prefetch bound: 256 slots at weight 1024
// of a total of 1024 is a share of 256, and clipped first it was 64.
func (s laneSizing) capacity(weight int) int {
	share := (s.concurrency*weight + s.totalWeight - 1) / s.totalWeight
	return boundedMul(max(share, minimumLaneCapacity), s.factor)
}

func automaticSubscriptionPrefetch(sub SubscriptionConfig) int {
	sizing := subscriptionLaneSizing(sub)
	total := 0
	for _, priority := range sub.Priorities {
		total = boundedAdd(total, sizing.capacity(sizing.mainWeight(priority)))
		total = boundedAdd(total, boundedMul(sub.Retry.tierCount(), sizing.capacity(sizing.retryWeight(priority))))
	}
	return boundedMul(total, len(sub.Topics))
}
