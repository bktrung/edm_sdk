package f1

import (
	"fmt"
	"maps"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
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
	MaxAttempts    int // positive producer cap; the subscription policy is the ceiling when consumed
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
	// DeathDetails holds handler-supplied diagnostic keys, written as one
	// f1detail<key> header each and read back from them. A map rather than a
	// nested object because the whole point is that each key is addressable on
	// the wire. Populated only on a dead-letter copy whose reason is terminal or
	// max_attempts, and cleared on a retry copy: details that rode the retry
	// ladder would accumulate the same way forwarded death records do.
	DeathDetails map[string]string `json:"-"`

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
// Oversized headers drop Extensions, preserve DeathError down to a floor, shed
// DeathDetails, and then truncate DeathError further. Invalid priorities and
// reserved extension keys return errors.
func (e Envelope) EncodeHeaders(driverMaxBytes int) (map[string]string, error) {
	if !e.Priority.Valid() {
		return nil, ErrInvalidPriority
	}
	for k := range e.Extensions {
		if isReservedExtensionKey(k) || isBrokerReserved(k) {
			return nil, ErrReservedExtension
		}
	}

	limit := CoreMaxHeaderBytes
	if driverMaxBytes > 0 && driverMaxBytes < limit {
		limit = driverMaxBytes
	}

	h := make(map[string]string, len(e.Forwarded)+32)
	maps.Copy(h, e.Forwarded)
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
	for key, value := range e.DeathDetails {
		if !validDeathDetailKey(key) {
			// This final wire guard has no logger; discarded keys are reported when the dead-letter copy is built.
			continue
		}
		h["f1detail"+key] = value
	}
	setOpt(h, "traceparent", e.TraceParent)
	setOpt(h, "tracestate", e.TraceState)

	maps.Copy(h, e.Extensions)

	if headerBytes(h) <= limit {
		return h, nil
	}
	for k := range e.Extensions {
		delete(h, k)
	}
	if headerBytes(h) <= limit {
		return h, nil
	}
	shrinkDeathErrorTo(h, limit, deathErrorFloor)
	if headerBytes(h) <= limit {
		return h, nil
	}
	for k := range h {
		if strings.HasPrefix(k, "f1detail") {
			delete(h, k)
		}
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

const deathErrorFloor = 512

// shrinkDeathError trims f1deatherror until h fits limit.
func shrinkDeathError(h map[string]string, limit int) {
	shrinkDeathErrorTo(h, limit, 0)
}

// shrinkDeathErrorTo trims f1deatherror until h fits limit or its value
// cannot be trimmed below floor; a floor of zero means no lower bound.
func shrinkDeathErrorTo(h map[string]string, limit, floor int) {
	for headerBytes(h) > limit {
		v, ok := h["f1deatherror"]
		if !ok || v == "" {
			return // There is no remaining field to shrink.
		}
		previousLen := len(v)
		over := headerBytes(h) - limit
		cut := max(previousLen-over, floor)
		if cut >= previousLen {
			return // The floor prevents further shrinking.
		}
		for cut > 0 && cut < previousLen && !utf8.RuneStart(v[cut]) {
			cut--
		}
		if cut < floor {
			cut = floor
			for cut < previousLen && !utf8.RuneStart(v[cut]) {
				cut++
			}
		}
		if cut >= previousLen {
			return // The floor falls inside the final rune.
		}
		h["f1deatherror"] = v[:cut]
	}
}

func validDeathDetailKey(key string) bool {
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
	if e.SpecVersion == "" {
		return Envelope{}, fmt.Errorf("f1: header specversion must not be empty")
	}
	if e.SpecVersion != "1.0" {
		return Envelope{}, fmt.Errorf("f1: header specversion must be \"1.0\", got %q", e.SpecVersion)
	}
	if e.ID == "" {
		return Envelope{}, fmt.Errorf("f1: header id must not be empty")
	}
	if e.Source == "" {
		return Envelope{}, fmt.Errorf("f1: header source must not be empty")
	}
	if e.Type == "" {
		return Envelope{}, fmt.Errorf("f1: header type must not be empty")
	}
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
		if n < 0 {
			return Envelope{}, fmt.Errorf("f1: header f1maxattempts: value must not be negative")
		}
		e.MaxAttempts = n
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
		if strings.HasPrefix(k, "f1detail") {
			if e.DeathDetails == nil {
				e.DeathDetails = map[string]string{}
			}
			e.DeathDetails[strings.TrimPrefix(k, "f1detail")] = v
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
	"f1maxattempts": true, "f1duetime": true,
	"f1originaldest": true, "f1correlationid": true, "f1causationid": true,
	"f1producer": true, "f1partitionkey": true, "f1expiry": true,
	"f1deatherror": true, "f1deathreason": true, "f1deathtime": true,
	"traceparent": true, "tracestate": true,
}
