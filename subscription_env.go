package f1

import (
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode"
)

// applySubscriptionEnvironment overlays the variables under the subscription's
// prefix onto cfg, then rejects any other variable under that prefix that no
// subscription can own, so a misspelled or removed key is an error rather than a
// silent no-op. The lookups run first, so a bad value is reported before an
// unknown name.
func applySubscriptionEnvironment(name string, cfg *SubscriptionConfig) error {
	reads := &subscriptionEnvReads{prefix: subscriptionEnvPrefix(name)}
	if value, ok := reads.get("topics"); ok {
		cfg.Topics = splitEnvList(value)
	}
	readEnv(reads, "mode", parseMode, &cfg.Mode)
	readEnv(reads, "concurrency", strconv.Atoi, &cfg.Concurrency)
	readEnv(reads, "prefetch", strconv.Atoi, &cfg.Prefetch)
	readEnv(reads, "priorities", parseEnvPriorities, &cfg.Priorities)
	readEnv(reads, "handlerTimeout", time.ParseDuration, &cfg.HandlerTimeout)
	readEnv(reads, "unmatchedPolicy", parseUnmatchedPolicy, &cfg.UnmatchedPolicy)
	readEnv(reads, "fairness.retryWeightDivisor", strconv.Atoi, &cfg.Fairness.RetryWeightDivisor)
	readEnv(reads, "fairness.prefetchFactor", strconv.Atoi, &cfg.Fairness.PrefetchFactor)
	readEnv(reads, "fairness.disableDeadlinePromotion", strconv.ParseBool, &cfg.Fairness.DisableDeadlinePromotion)
	for _, priority := range []Priority{PriorityHigh, PriorityMedium, PriorityLow} {
		var weight int
		if readEnv(reads, "fairness.weights."+priority.String(), strconv.Atoi, &weight) {
			if cfg.Fairness.Weights == nil {
				cfg.Fairness.Weights = make(map[Priority]int)
			}
			cfg.Fairness.Weights[priority] = weight
		}
		var budget time.Duration
		if readEnv(reads, "fairness.budgets."+priority.String(), time.ParseDuration, &budget) {
			if cfg.Fairness.Budgets == nil {
				cfg.Fairness.Budgets = make(map[Priority]time.Duration)
			}
			cfg.Fairness.Budgets[priority] = budget
		}
	}
	readEnv(reads, "retry.maxAttempts", strconv.Atoi, &cfg.Retry.MaxAttempts)
	readEnv(reads, "retry.initialInterval", time.ParseDuration, &cfg.Retry.InitialInterval)
	readEnv(reads, "retry.multiplier", func(value string) (float64, error) {
		return strconv.ParseFloat(value, 64)
	}, &cfg.Retry.Multiplier)
	readEnv(reads, "retry.maxInterval", time.ParseDuration, &cfg.Retry.MaxInterval)
	readEnv(reads, "retry.tiers", parseEnvRetryTiers, &cfg.Retry.Tiers)
	if reads.err != nil {
		return reads.err
	}
	return checkSubscriptionEnvNamespace(reads)
}

// readEnv assigns only present, successfully parsed values. Retaining the first
// error prevents later lookups and mutations before the overlay returns it.
// The result lets map entries remain absent when their variable is absent.
func readEnv[T any](reads *subscriptionEnvReads, key string, parse func(string) (T, error), dst *T) bool {
	if reads.err != nil {
		return false
	}
	value, ok := reads.get(key)
	if !ok {
		return false
	}
	parsed, err := parse(value)
	if err != nil {
		reads.err = fmt.Errorf("f1: %s: %w", envKey(reads.prefix, key), err)
		return false
	}
	*dst = parsed
	return true
}

func parseEnvPriorities(value string) ([]Priority, error) {
	priorities := make([]Priority, 0)
	for _, name := range splitEnvList(value) {
		priority, err := ParsePriority(name)
		if err != nil {
			return nil, err
		}
		priorities = append(priorities, priority)
	}
	return priorities, nil
}

func parseEnvRetryTiers(value string) ([]time.Duration, error) {
	tiers := make([]time.Duration, 0)
	for _, item := range splitEnvList(value) {
		tier, err := time.ParseDuration(item)
		if err != nil {
			return nil, err
		}
		tiers = append(tiers, tier)
	}
	return tiers, nil
}

// subscriptionEnvBase is the namespace every per-subscription variable lives in.
// A subscription's prefix is this, its name token and a trailing underscore.
const subscriptionEnvBase = "F1_SUBSCRIPTIONS_"

func subscriptionEnvPrefix(name string) string {
	return subscriptionEnvBase + envToken(name) + "_"
}

// checkSubscriptionEnvNamespace reports the first variable, in name order, under
// the subscription's prefix that no subscription can own. A key this package
// does not read is otherwise a silent no-op, while the YAML side of the same
// configuration rejects an unknown key by name.
func checkSubscriptionEnvNamespace(reads *subscriptionEnvReads) error {
	unowned := make([]string, 0)
	for _, entry := range os.Environ() {
		name, _, _ := strings.Cut(entry, "=")
		if strings.HasPrefix(name, reads.prefix) && !subscriptionEnvOwned(name, reads.tokens) {
			unowned = append(unowned, name)
		}
	}
	if len(unowned) == 0 {
		return nil
	}
	slices.Sort(unowned)
	return unknownSubscriptionEnvKey(unowned[0])
}

// subscriptionEnvOwned reports whether any subscription can own the variable
// name. A name a subscription spells is a name token, an underscore and a key
// token, and one name can be two subscriptions at once:
// F1_SUBSCRIPTIONS_ORDERS_RETRY_MAX_ATTEMPTS is "orders" with retry.maxAttempts,
// and "ordersRetry" with maxAttempts. So the name is owned when a token some
// subscription reads ends it, behind a single underscore: envToken trims the
// underscores at the ends of the tokens it builds, so no name token ends with
// one and no key token opens with one.
func subscriptionEnvOwned(name string, tokens []string) bool {
	suffix := strings.TrimPrefix(name, subscriptionEnvBase)
	for _, token := range tokens {
		boundary := "_" + token
		if !strings.HasSuffix(suffix, boundary) {
			continue
		}
		if rest := strings.TrimSuffix(suffix, boundary); !strings.HasSuffix(rest, "_") {
			return true
		}
	}
	return false
}

// subscriptionEnvRemovedKeys are the keys this package read once and no longer
// does, with the advice an error about one carries. A renamed key names its
// replacement and a dropped key says so, because the alternative is a service
// that keeps setting it long after it stopped doing anything.
var subscriptionEnvRemovedKeys = []struct{ token, hint string }{
	{"FAIRNESS_AGING_ENABLED", "fairness.agingEnabled is now fairness.disableDeadlinePromotion and the sense is inverted: FAIRNESS_AGING_ENABLED=true is FAIRNESS_DISABLE_DEADLINE_PROMOTION=false"},
	{"FAIRNESS_DISABLE_AGING", "FAIRNESS_DISABLE_AGING is now FAIRNESS_DISABLE_DEADLINE_PROMOTION"},
	{"FAIRNESS_COST_MODEL", "fairness.costModel was removed; no key replaces it"},
	{"RETRY_JITTER", "retry.jitter was removed; no key replaces it"},
}

// unknownSubscriptionEnvKey builds the error for a variable under the
// subscription's prefix that no subscription can own. The error carries the
// variable's full name, so a deployment can find the line it came from, and when
// the name spells a key this package used to read, what to write instead. That
// key is matched as a whole run between underscores, so a name that qualified it
// further, F1_SUBSCRIPTIONS_ORDERS_RETRY_JITTER_MS, still carries the advice the
// removal deserves.
func unknownSubscriptionEnvKey(name string) error {
	padded := "_" + strings.TrimPrefix(name, subscriptionEnvBase) + "_"
	for _, removed := range subscriptionEnvRemovedKeys {
		if strings.Contains(padded, "_"+removed.token+"_") {
			return fmt.Errorf("f1: %s: unknown key; %s", name, removed.hint)
		}
	}
	return fmt.Errorf("f1: %s: unknown key", name)
}

// subscriptionEnvReads performs one subscription's environment lookups and
// records the key token each one reads. The namespace check that ends
// applySubscriptionEnvironment accepts a variable only when its name ends with
// one of those tokens, so the known keys are the keys the overlay reads: a lookup
// added to the overlay cannot be one the check does not know.
type subscriptionEnvReads struct {
	prefix string
	tokens []string
	err    error
}

// get reads the variable for key under the subscription prefix and records the
// token the namespace check compares against.
func (r *subscriptionEnvReads) get(key string) (string, bool) {
	r.tokens = append(r.tokens, envToken(key))
	return lookupSubscriptionEnv(r.prefix, key)
}

// lookupSubscriptionEnv reads one variable without recording it, for a caller
// outside the overlay. Such a caller must read a key the overlay reads too: only
// the overlay's keys reach the namespace check, which would reject a variable
// this package honours but the overlay never reads.
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
