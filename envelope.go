package f1

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Envelope contains the canonical message headers.
type Envelope struct {
	// CloudEvents attributes.
	SpecVersion     string
	ID              string
	Source          string
	Type            string
	Time            time.Time
	DataContentType string
	Subject         string
	DataSchema      string

	// F1 attributes.
	IdempotencyKey string
	Priority       Priority // encoded as a string on the wire
	Attempt        int
	MaxAttempts    int // absent unless producer-capped or stamped on a retry copy
	Deferrals      int
	DueTime        *time.Time
	OriginalDest   string
	CorrelationID  string
	CausationID    string
	Producer       string
	PartitionKey   string
	Expiry         *time.Time

	// Dead-letter metadata.
	DeathError  string
	DeathReason DeathReason // empty when live or no SDK-recorded reason
	DeathTime   *time.Time

	// Trace context is preserved separately from free-form extensions.
	TraceParent string
	TraceState  string

	// Forwarded holds unknown reserved attributes received from the wire.
	Forwarded map[string]string

	// Extensions holds user-defined attributes. Reserved names are rejected
	// when the envelope is encoded.
	Extensions map[string]string
}

// timeLayout preserves sub-second precision while accepting RFC3339 values.
const timeLayout = time.RFC3339Nano

// CoreMaxHeaderBytes is the SDK's maximum encoded header size.
const CoreMaxHeaderBytes = 8 * 1024

// EncodeHeaders serializes e to canonical wire headers. driverMaxBytes is the
// connected driver's limit; zero or a larger value uses CoreMaxHeaderBytes.
// Oversized headers drop Extensions and then truncate DeathError. Invalid
// priorities and reserved extension keys return errors.
func (e Envelope) EncodeHeaders(driverMaxBytes int) (map[string]string, error) {
	if !e.Priority.Valid() {
		return nil, ErrInvalidPriority
	}
	for k := range e.Extensions {
		if isReservedExtensionKey(k) {
			return nil, ErrReservedExtension
		}
	}

	limit := CoreMaxHeaderBytes
	if driverMaxBytes > 0 && driverMaxBytes < limit {
		limit = driverMaxBytes
	}

	h := make(map[string]string, len(e.Forwarded)+32)
	for k, v := range e.Forwarded {
		h[k] = v
	}
	h["specversion"] = e.SpecVersion
	h["id"] = e.ID
	h["source"] = e.Source
	h["type"] = e.Type
	h["time"] = e.Time.Format(timeLayout)
	h["f1idempotencykey"] = e.IdempotencyKey
	h["f1priority"] = e.Priority.String()
	h["f1attempt"] = strconv.Itoa(e.Attempt)
	setOpt(h, "datacontenttype", e.DataContentType)
	setOpt(h, "subject", e.Subject)
	setOpt(h, "dataschema", e.DataSchema)
	if e.MaxAttempts != 0 {
		h["f1maxattempts"] = strconv.Itoa(e.MaxAttempts)
	}
	if e.Deferrals != 0 {
		h["f1deferrals"] = strconv.Itoa(e.Deferrals)
	}
	setOptTime(h, "f1duetime", e.DueTime)
	setOpt(h, "f1originaldest", e.OriginalDest)
	if e.CorrelationID != "" {
		h["f1correlationid"] = e.CorrelationID
	} else {
		h["f1correlationid"] = e.ID // Root events use their ID as the correlation ID.
	}
	setOpt(h, "f1causationid", e.CausationID)
	setOpt(h, "f1producer", e.Producer)
	setOpt(h, "f1partitionkey", e.PartitionKey)
	setOptTime(h, "f1expiry", e.Expiry)
	setOpt(h, "f1deatherror", e.DeathError)
	if e.DeathReason != ReasonUnspecified {
		h["f1deathreason"] = e.DeathReason.String()
	}
	setOptTime(h, "f1deathtime", e.DeathTime)
	setOpt(h, "traceparent", e.TraceParent)
	setOpt(h, "tracestate", e.TraceState)

	for k, v := range e.Extensions {
		h[k] = v
	}

	if headerBytes(h) <= limit {
		return h, nil
	}
	for k := range e.Extensions {
		delete(h, k)
	}
	if headerBytes(h) <= limit {
		return h, nil
	}
	shrinkDeathError(h, limit)
	return h, nil
}

func setOpt(h map[string]string, key, v string) {
	if v != "" {
		h[key] = v
	}
}

func setOptTime(h map[string]string, key string, v *time.Time) {
	if v != nil {
		h[key] = v.Format(timeLayout)
	}
}

func headerBytes(h map[string]string) int {
	n := 0
	for k, v := range h {
		n += len(k) + len(v)
	}
	return n
}

// shrinkDeathError trims f1deatherror until h fits limit.
func shrinkDeathError(h map[string]string, limit int) {
	for headerBytes(h) > limit {
		v, ok := h["f1deatherror"]
		if !ok || v == "" {
			return // There is no remaining field to shrink.
		}
		over := headerBytes(h) - limit
		cut := len(v) - over
		if cut < 0 {
			cut = 0
		}
		h["f1deatherror"] = v[:cut]
	}
}

// DecodeHeaders parses canonical wire headers into an Envelope. Unknown
// broker-internal headers are dropped, reserved-prefix headers are kept in
// Forwarded, and other unknown headers are stored in Extensions. Priority is
// validated; DeathReason is preserved as received.
func DecodeHeaders(h map[string]string) (Envelope, error) {
	var e Envelope
	e.SpecVersion = h["specversion"]
	e.ID = h["id"]
	e.Source = h["source"]
	e.Type = h["type"]
	if v, ok := h["time"]; ok {
		t, err := time.Parse(timeLayout, v)
		if err != nil {
			return Envelope{}, fmt.Errorf("f1: header time: %w", err)
		}
		e.Time = t
	}
	e.DataContentType = h["datacontenttype"]
	e.Subject = h["subject"]
	e.DataSchema = h["dataschema"]
	e.IdempotencyKey = h["f1idempotencykey"]

	if v, ok := h["f1priority"]; ok {
		p, err := ParsePriority(v)
		if err != nil {
			return Envelope{}, err
		}
		e.Priority = p
	}
	if v, ok := h["f1attempt"]; ok {
		n, err := strconv.Atoi(v)
		if err != nil {
			return Envelope{}, fmt.Errorf("f1: header f1attempt: %w", err)
		}
		e.Attempt = n
	}
	if v, ok := h["f1maxattempts"]; ok {
		n, err := strconv.Atoi(v)
		if err != nil {
			return Envelope{}, fmt.Errorf("f1: header f1maxattempts: %w", err)
		}
		e.MaxAttempts = n
	}
	if v, ok := h["f1deferrals"]; ok {
		n, err := strconv.Atoi(v)
		if err != nil {
			return Envelope{}, fmt.Errorf("f1: header f1deferrals: %w", err)
		}
		e.Deferrals = n
	}
	var err error
	if e.DueTime, err = parseOptTime(h, "f1duetime"); err != nil {
		return Envelope{}, err
	}
	e.OriginalDest = h["f1originaldest"]
	e.CorrelationID = h["f1correlationid"]
	e.CausationID = h["f1causationid"]
	e.Producer = h["f1producer"]
	e.PartitionKey = h["f1partitionkey"]
	if e.Expiry, err = parseOptTime(h, "f1expiry"); err != nil {
		return Envelope{}, err
	}
	e.DeathError = h["f1deatherror"]
	e.DeathReason = DeathReason(h["f1deathreason"])
	if e.DeathTime, err = parseOptTime(h, "f1deathtime"); err != nil {
		return Envelope{}, err
	}
	e.TraceParent = h["traceparent"]
	e.TraceState = h["tracestate"]

	for k, v := range h {
		if knownHeaders[k] {
			continue
		}
		if isBrokerReserved(k) {
			continue // Broker-internal headers are not propagated.
		}
		if isReservedExtensionKey(k) {
			if e.Forwarded == nil {
				e.Forwarded = map[string]string{}
			}
			e.Forwarded[k] = v
			continue
		}
		if e.Extensions == nil {
			e.Extensions = map[string]string{}
		}
		e.Extensions[k] = v
	}
	return e, nil
}

// isBrokerReserved reports whether k is a broker death-history header.
func isBrokerReserved(k string) bool {
	return k == "x-death" || strings.HasPrefix(k, "x-first-death-")
}

// isReservedExtensionKey reports whether k is a known header or uses a
// reserved extension prefix.
func isReservedExtensionKey(k string) bool {
	return knownHeaders[k] || strings.HasPrefix(k, "f1") || strings.HasPrefix(k, "ce_") || strings.HasPrefix(k, "ce-")
}

func parseOptTime(h map[string]string, key string) (*time.Time, error) {
	v, ok := h[key]
	if !ok {
		return nil, nil
	}
	t, err := time.Parse(timeLayout, v)
	if err != nil {
		return nil, fmt.Errorf("f1: header %s: %w", key, err)
	}
	return &t, nil
}

var knownHeaders = map[string]bool{
	"specversion": true, "id": true, "source": true, "type": true, "time": true,
	"datacontenttype": true, "subject": true, "dataschema": true,
	"f1idempotencykey": true, "f1priority": true, "f1attempt": true,
	"f1maxattempts": true, "f1deferrals": true, "f1duetime": true,
	"f1originaldest": true, "f1correlationid": true, "f1causationid": true,
	"f1producer": true, "f1partitionkey": true, "f1expiry": true,
	"f1deatherror": true, "f1deathreason": true, "f1deathtime": true,
	"traceparent": true, "tracestate": true,
}
