package f1

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode"
)

func applySubscriptionEnvironment(name string, cfg *SubscriptionConfig) error {
	prefix := subscriptionEnvPrefix(name)
	if value, ok := lookupSubscriptionEnv(prefix, "topics"); ok {
		cfg.Topics = splitEnvList(value)
	}
	if value, ok := lookupSubscriptionEnv(prefix, "mode"); ok {
		mode, err := parseMode(value)
		if err != nil {
			return fmt.Errorf("f1: %s mode: %w", envKey(prefix, "mode"), err)
		}
		cfg.Mode = mode
	}
	if value, ok := lookupSubscriptionEnv(prefix, "concurrency"); ok {
		parsed, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("f1: %s concurrency: %w", envKey(prefix, "concurrency"), err)
		}
		cfg.Concurrency = parsed
	}
	if value, ok := lookupSubscriptionEnv(prefix, "prefetch"); ok {
		parsed, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("f1: %s prefetch: %w", envKey(prefix, "prefetch"), err)
		}
		cfg.Prefetch = parsed
	}
	if value, ok := lookupSubscriptionEnv(prefix, "priorities"); ok {
		priorities := make([]Priority, 0)
		for _, name := range splitEnvList(value) {
			priority, err := ParsePriority(name)
			if err != nil {
				return fmt.Errorf("f1: %s priorities: %w", envKey(prefix, "priorities"), err)
			}
			priorities = append(priorities, priority)
		}
		cfg.Priorities = priorities
	}
	if value, ok := lookupSubscriptionEnv(prefix, "handlerTimeout"); ok {
		parsed, err := time.ParseDuration(value)
		if err != nil {
			return fmt.Errorf("f1: %s handlerTimeout: %w", envKey(prefix, "handlerTimeout"), err)
		}
		cfg.HandlerTimeout = parsed
	}
	if value, ok := lookupSubscriptionEnv(prefix, "unmatchedPolicy"); ok {
		policy, err := parseUnmatchedPolicy(value)
		if err != nil {
			return fmt.Errorf("f1: %s unmatchedPolicy: %w", envKey(prefix, "unmatchedPolicy"), err)
		}
		cfg.UnmatchedPolicy = policy
	}
	if value, ok := lookupSubscriptionEnv(prefix, "fairness.retryWeightDivisor"); ok {
		parsed, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("f1: %s: %w", envKey(prefix, "fairness.retryWeightDivisor"), err)
		}
		cfg.Fairness.RetryWeightDivisor = parsed
	}
	if value, ok := lookupSubscriptionEnv(prefix, "fairness.costModel"); ok {
		cfg.Fairness.CostModel = value
	}
	if value, ok := lookupSubscriptionEnv(prefix, "fairness.prefetchFactor"); ok {
		parsed, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("f1: %s: %w", envKey(prefix, "fairness.prefetchFactor"), err)
		}
		cfg.Fairness.PrefetchFactor = parsed
	}
	if value, ok := lookupSubscriptionEnv(prefix, "fairness.agingEnabled"); ok {
		parsed, err := strconv.ParseBool(value)
		if err != nil {
			return fmt.Errorf("f1: %s: %w", envKey(prefix, "fairness.agingEnabled"), err)
		}
		cfg.Fairness.AgingEnabled = parsed
	}
	for _, priority := range []Priority{PriorityHigh, PriorityMedium, PriorityLow} {
		if value, ok := lookupSubscriptionEnv(prefix, "fairness.weights."+priority.String()); ok {
			parsed, err := strconv.Atoi(value)
			if err != nil {
				return fmt.Errorf("f1: %s: %w", envKey(prefix, "fairness.weights."+priority.String()), err)
			}
			if cfg.Fairness.Weights == nil {
				cfg.Fairness.Weights = make(map[Priority]int)
			}
			cfg.Fairness.Weights[priority] = parsed
		}
		if value, ok := lookupSubscriptionEnv(prefix, "fairness.budgets."+priority.String()); ok {
			parsed, err := time.ParseDuration(value)
			if err != nil {
				return fmt.Errorf("f1: %s: %w", envKey(prefix, "fairness.budgets."+priority.String()), err)
			}
			if cfg.Fairness.Budgets == nil {
				cfg.Fairness.Budgets = make(map[Priority]time.Duration)
			}
			cfg.Fairness.Budgets[priority] = parsed
		}
	}
	if value, ok := lookupSubscriptionEnv(prefix, "retry.maxAttempts"); ok {
		parsed, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("f1: %s: %w", envKey(prefix, "retry.maxAttempts"), err)
		}
		cfg.Retry.MaxAttempts = parsed
	}
	if value, ok := lookupSubscriptionEnv(prefix, "retry.initialInterval"); ok {
		parsed, err := time.ParseDuration(value)
		if err != nil {
			return fmt.Errorf("f1: %s: %w", envKey(prefix, "retry.initialInterval"), err)
		}
		cfg.Retry.InitialInterval = parsed
	}
	if value, ok := lookupSubscriptionEnv(prefix, "retry.multiplier"); ok {
		parsed, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return fmt.Errorf("f1: %s: %w", envKey(prefix, "retry.multiplier"), err)
		}
		cfg.Retry.Multiplier = parsed
	}
	if value, ok := lookupSubscriptionEnv(prefix, "retry.maxInterval"); ok {
		parsed, err := time.ParseDuration(value)
		if err != nil {
			return fmt.Errorf("f1: %s: %w", envKey(prefix, "retry.maxInterval"), err)
		}
		cfg.Retry.MaxInterval = parsed
	}
	if value, ok := lookupSubscriptionEnv(prefix, "retry.jitter"); ok {
		parsed, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return fmt.Errorf("f1: %s: %w", envKey(prefix, "retry.jitter"), err)
		}
		cfg.Retry.Jitter = parsed
	}
	if value, ok := lookupSubscriptionEnv(prefix, "retry.tiers"); ok {
		tiers := make([]time.Duration, 0)
		for _, item := range splitEnvList(value) {
			tier, err := time.ParseDuration(item)
			if err != nil {
				return fmt.Errorf("f1: %s retry.tiers: %w", envKey(prefix, "retry.tiers"), err)
			}
			tiers = append(tiers, tier)
		}
		cfg.Retry.Tiers = tiers
	}
	return nil
}

func subscriptionEnvPrefix(name string) string {
	return "F1_SUBSCRIPTIONS_" + envToken(name) + "_"
}

func lookupSubscriptionEnv(prefix, key string) (string, bool) {
	return os.LookupEnv(envKey(prefix, key))
}

func envKey(prefix, key string) string {
	return prefix + envToken(key)
}

func envToken(value string) string {
	var b strings.Builder
	for i, r := range value {
		if unicode.IsUpper(r) && i > 0 {
			b.WriteByte('_')
		}
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(unicode.ToUpper(r))
		} else {
			b.WriteByte('_')
		}
	}
	return strings.Trim(b.String(), "_")
}

func splitEnvList(value string) []string {
	if strings.TrimSpace(value) == "" {
		return []string{}
	}
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		result = append(result, strings.TrimSpace(part))
	}
	return result
}

func parseMode(value string) (Mode, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "unordered":
		return Unordered, nil
	case "orderedbykey", "ordered_by_key":
		return OrderedByKey, nil
	default:
		return Unordered, fmt.Errorf("unsupported mode %q", value)
	}
}

func parseUnmatchedPolicy(value string) (UnmatchedPolicy, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "ignore":
		return Ignore, nil
	case "deadletter", "dead_letter":
		return DeadLetter, nil
	default:
		return Ignore, fmt.Errorf("unsupported unmatched policy %q", value)
	}
}
