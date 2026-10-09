package f1_test

import (
	"maps"
	"sort"
	"strings"
	"testing"
	"time"

	f1 "fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk"
)

func FuzzDecodeHeaders(f *testing.F) {
	for _, headers := range fuzzEnvelopeHeaderSeeds() {
		f.Add(encodeFuzzHeaders(headers))
	}

	f.Fuzz(func(t *testing.T, encoded string) {
		headers := decodeFuzzHeaders(encoded)
		decoded, err := f1.DecodeHeaders(headers)
		if err != nil {
			return
		}

		reencoded, err := decoded.EncodeHeaders(0)
		if err != nil {
			return
		}
		replayed, err := f1.DecodeHeaders(reencoded)
		if err != nil {
			t.Fatalf("decoding re-encoded headers: %v", err)
		}
		if !fuzzEnvelopesEqual(decoded, replayed) {
			t.Fatalf("decoded envelope changed after round trip: %#v != %#v", decoded, replayed)
		}
	})
}

func fuzzEnvelopeHeaderSeeds() []map[string]string {
	seeds := make([]map[string]string, 0, 16)
	addEnvelope := func(envelope f1.Envelope) {
		headers, err := envelope.EncodeHeaders(0)
		if err == nil {
			seeds = append(seeds, headers)
		}
	}
	addHeaders := func(headers map[string]string) {
		seeds = append(seeds, headers)
	}

	addEnvelope(minimalEnvelope())
	addEnvelope(fullEnvelope())

	e := fullEnvelope()
	e.DataContentType = "application/json"
	e.Subject = "ORD-88213"
	e.DataSchema = "https://schemas.example/order.v1.json"
	e.Extensions = map[string]string{"x-custom-ext": "value"}
	addEnvelope(e)

	e.DeathDetails = map[string]string{"tenant": "acme", "step": "charge"}
	addEnvelope(e)

	e.Forwarded = map[string]string{"f1future": "future-value"}
	addEnvelope(e)

	e.DeathReason = f1.DeathReason("a_reason_this_build_predates")
	addEnvelope(e)

	e.MaxAttempts = -1
	addEnvelope(e)
	e.MaxAttempts = 1_000_000
	addEnvelope(e)

	addHeaders(map[string]string{
		"specversion":     "1.0",
		"id":              "archived-dependency-outage",
		"source":          "/legacy/orders",
		"type":            "orders.created.v1",
		"f1priority":      "medium",
		"f1attempt":       "2",
		"f1deathreason":   "dependency_unavailable",
		"f1correlationid": "archived-dependency-outage",
	})
	addHeaders(map[string]string{
		"specversion":   "1.0",
		"id":            "evt-1",
		"source":        "/prod/ingest-api",
		"type":          "com.za.order.created.v1",
		"time":          "2026-08-05T11:00:00Z",
		"f1priority":    "high",
		"f1futurething": "future-value",
	})
	addHeaders(map[string]string{
		"specversion": "1.0",
		"id":          "evt-1",
		"source":      "/prod/orders",
		"type":        "orders.created.v1",
	})

	malformed := map[string]string{}
	maps.Copy(malformed, seeds[1])
	for key, value := range map[string]string{
		"time":          "not-a-time",
		"f1attempt":     "not-an-int",
		"f1maxattempts": "not-an-int",
		"f1duetime":     "not-a-time",
		"f1expiry":      "not-a-time",
		"f1deathtime":   "not-a-time",
		"f1priority":    "urgent",
	} {
		mutated := make(map[string]string, len(malformed))
		maps.Copy(mutated, malformed)
		mutated[key] = value
		addHeaders(mutated)
	}

	return seeds
}

func encodeFuzzHeaders(headers map[string]string) string {
	keys := make([]string, 0, len(headers))
	for key := range headers {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	var encoded strings.Builder
	for _, key := range keys {
		encoded.WriteString(key)
		encoded.WriteByte('=')
		encoded.WriteString(headers[key])
		encoded.WriteByte('\n')
	}
	return encoded.String()
}

func decodeFuzzHeaders(encoded string) map[string]string {
	headers := make(map[string]string)
	for line := range strings.SplitSeq(encoded, "\n") {
		key, value, ok := strings.Cut(line, "=")
		if ok {
			headers[key] = value
		}
	}
	return headers
}

func fuzzEnvelopesEqual(left, right f1.Envelope) bool {
	if left.CorrelationID == "" {
		left.CorrelationID = left.ID
	}
	if right.CorrelationID == "" {
		right.CorrelationID = right.ID
	}
	if left.SpecVersion != right.SpecVersion ||
		left.ID != right.ID ||
		left.Source != right.Source ||
		left.Type != right.Type ||
		!left.Time.Equal(right.Time) ||
		left.DataContentType != right.DataContentType ||
		left.Subject != right.Subject ||
		left.DataSchema != right.DataSchema ||
		left.IdempotencyKey != right.IdempotencyKey ||
		left.Priority != right.Priority ||
		left.Attempt != right.Attempt ||
		left.MaxAttempts != right.MaxAttempts ||
		left.OriginalDest != right.OriginalDest ||
		left.CorrelationID != right.CorrelationID ||
		left.CausationID != right.CausationID ||
		left.Producer != right.Producer ||
		left.PartitionKey != right.PartitionKey ||
		left.DeathError != right.DeathError ||
		left.DeathReason != right.DeathReason ||
		left.TraceParent != right.TraceParent ||
		left.TraceState != right.TraceState {
		return false
	}
	if !fuzzOptionalTimesEqual(left.DueTime, right.DueTime) ||
		!fuzzOptionalTimesEqual(left.Expiry, right.Expiry) ||
		!fuzzOptionalTimesEqual(left.DeathTime, right.DeathTime) {
		return false
	}
	return fuzzDeathDetailsEqual(left.DeathDetails, right.DeathDetails) &&
		maps.Equal(left.Forwarded, right.Forwarded) &&
		maps.Equal(left.Extensions, right.Extensions)
}

func fuzzDeathDetailsEqual(left, right map[string]string) bool {
	validLeft := 0
	for key, value := range left {
		if !fuzzValidDeathDetailKey(key) {
			continue
		}
		validLeft++
		rightValue, ok := right[key]
		if !ok || rightValue != value {
			return false
		}
	}

	validRight := 0
	for key := range right {
		if fuzzValidDeathDetailKey(key) {
			validRight++
		}
	}
	return validLeft == validRight
}

func fuzzValidDeathDetailKey(key string) bool {
	if key == "" {
		return false
	}
	for i := range len(key) {
		c := key[i]
		if (c < 'a' || c > 'z') && (c < '0' || c > '9') {
			return false
		}
	}
	return true
}

func fuzzOptionalTimesEqual(left, right *time.Time) bool {
	if left == nil || right == nil {
		return left == right
	}
	return left.Equal(*right)
}
