package f1

import (
	"fmt"
	"maps"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/wire"
)

// Envelope carries the CloudEvents and F1 metadata serialized as message
// headers. Use EncodeHeaders and DecodeHeaders to cross the wire boundary. Its
// zero Priority is PriorityMedium.
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
	Producer       string // "service/env/instanceID" of the client that published
	PartitionKey   string // the WithKey value; empty when the broker key fell back to the subject or the event ID
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

// CoreMaxHeaderBytes is the largest encoded header set the SDK permits. A
// configured or broker-specific limit may lower the effective cap.
const CoreMaxHeaderBytes = 8 * 1024

// EncodeHeaders serializes e as canonical wire headers. A positive
// maxHeaderBytes below CoreMaxHeaderBytes lowers the cap; non-positive or larger
// values use CoreMaxHeaderBytes.
//
// When headers exceed the cap, EncodeHeaders drops Extensions, shrinks
// DeathError to a 512-byte floor, drops DeathDetails, drops unknown Forwarded
// fields, then removes tracestate, dataschema, subject, and traceparent in that
// order. It then truncates DeathError without a floor. datacontenttype is never
// removed because the consumer selects its decoder from it.
// Required CloudEvents and F1 protocol headers are never removed. If the
// remaining headers still exceed the cap, EncodeHeaders returns
// ErrEnvelopeTooLarge. Invalid priorities and reserved extension keys return
// errors.
func (e Envelope) EncodeHeaders(maxHeaderBytes int) (map[string]string, error) {
	if !e.Priority.Valid() {
		return nil, ErrInvalidPriority
	}
	for k := range e.Extensions {
		if isReservedExtensionKey(k) || isBrokerReserved(k) {
			return nil, ErrReservedExtension
		}
	}

	limit := CoreMaxHeaderBytes
	if maxHeaderBytes > 0 && maxHeaderBytes < limit {
		limit = maxHeaderBytes
	}

	h := make(map[string]string, len(e.Forwarded)+32)
	maps.Copy(h, e.Forwarded)
	h[wire.SpecVersion] = e.SpecVersion
	h[wire.ID] = e.ID
	h[wire.Source] = e.Source
	h[wire.Type] = e.Type
	h[wire.Time] = e.Time.Format(timeLayout)
	h[wire.IdempotencyKey] = e.IdempotencyKey
	h[wire.Priority] = e.Priority.String()
	h[wire.Attempt] = strconv.Itoa(e.Attempt)
	setOpt(h, wire.DataContentType, e.DataContentType)
	setOpt(h, wire.Subject, e.Subject)
	setOpt(h, wire.DataSchema, e.DataSchema)
	if e.MaxAttempts != 0 {
		h[wire.MaxAttempts] = strconv.Itoa(e.MaxAttempts)
	}
	setOptTime(h, wire.DueTime, e.DueTime)
	setOpt(h, wire.OriginalDest, e.OriginalDest)
	if e.CorrelationID != "" {
		h[wire.CorrelationID] = e.CorrelationID
	} else {
		h[wire.CorrelationID] = e.ID // Root events use their ID as the correlation ID.
	}
	setOpt(h, wire.CausationID, e.CausationID)
	setOpt(h, wire.Producer, e.Producer)
	setOpt(h, wire.PartitionKey, e.PartitionKey)
	setOptTime(h, wire.Expiry, e.Expiry)
	setOpt(h, wire.DeathError, e.DeathError)
	if e.DeathReason != ReasonUnspecified {
		h[wire.DeathReason] = e.DeathReason.String()
	}
	setOptTime(h, wire.DeathTime, e.DeathTime)
	for key, value := range e.DeathDetails {
		if !validDeathDetailKey(key) {
			// This final wire guard has no logger; discarded keys are reported when the dead-letter copy is built.
			continue
		}
		h[wire.DetailPrefix+key] = value
	}
	setOpt(h, wire.TraceParent, e.TraceParent)
	setOpt(h, wire.TraceState, e.TraceState)

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
		if strings.HasPrefix(k, wire.DetailPrefix) {
			delete(h, k)
		}
	}
	if headerBytes(h) <= limit {
		return h, nil
	}
	for k := range e.Forwarded {
		if !knownHeaders[k] {
			delete(h, k)
		}
	}
	if headerBytes(h) <= limit {
		return h, nil
	}
	for _, k := range []string{wire.TraceState, wire.DataSchema, wire.Subject, wire.TraceParent} {
		delete(h, k)
		if headerBytes(h) <= limit {
			return h, nil
		}
	}
	shrinkDeathError(h, limit)
	if headerBytes(h) > limit {
		return nil, fmt.Errorf("f1: envelope headers exceed cap for id=%q source=%q type=%q: %w", e.ID, e.Source, e.Type, ErrEnvelopeTooLarge)
	}
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
	size := headerBytes(h)
	for size > limit {
		v, ok := h[wire.DeathError]
		if !ok || v == "" {
			return // There is no remaining field to shrink.
		}
		previousLen := len(v)
		over := size - limit
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
		h[wire.DeathError] = v[:cut]
		size = headerBytes(h)
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

// DecodeHeaders parses canonical wire headers into an Envelope. It requires
// specversion "1.0", id, source, and type, and returns errors for malformed
// values or an invalid priority. Unknown broker-internal headers are dropped;
// f1detail headers populate DeathDetails, other reserved-prefix headers are
// kept in Forwarded, and other unknown headers are stored in Extensions.
// DeathReason is preserved as received.
func DecodeHeaders(h map[string]string) (Envelope, error) {
	var e Envelope
	e.SpecVersion = h[wire.SpecVersion]
	e.ID = h[wire.ID]
	e.Source = h[wire.Source]
	e.Type = h[wire.Type]
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
	if v, ok := h[wire.Time]; ok {
		t, err := time.Parse(timeLayout, v)
		if err != nil {
			return Envelope{}, fmt.Errorf("f1: header time: %w", err)
		}
		e.Time = t
	}
	e.DataContentType = h[wire.DataContentType]
	e.Subject = h[wire.Subject]
	e.DataSchema = h[wire.DataSchema]
	e.IdempotencyKey = h[wire.IdempotencyKey]

	if v, ok := h[wire.Priority]; ok {
		p, err := ParsePriority(v)
		if err != nil {
			return Envelope{}, err
		}
		e.Priority = p
	}
	if v, ok := h[wire.Attempt]; ok {
		n, err := strconv.Atoi(v)
		if err != nil {
			return Envelope{}, fmt.Errorf("f1: header f1attempt: %w", err)
		}
		e.Attempt = n
	}
	if v, ok := h[wire.MaxAttempts]; ok {
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
	if e.DueTime, err = parseOptTime(h, wire.DueTime); err != nil {
		return Envelope{}, err
	}
	e.OriginalDest = h[wire.OriginalDest]
	e.CorrelationID = h[wire.CorrelationID]
	e.CausationID = h[wire.CausationID]
	e.Producer = h[wire.Producer]
	e.PartitionKey = h[wire.PartitionKey]
	if e.Expiry, err = parseOptTime(h, wire.Expiry); err != nil {
		return Envelope{}, err
	}
	e.DeathError = h[wire.DeathError]
	e.DeathReason = DeathReason(h[wire.DeathReason])
	if e.DeathTime, err = parseOptTime(h, wire.DeathTime); err != nil {
		return Envelope{}, err
	}
	e.TraceParent = h[wire.TraceParent]
	e.TraceState = h[wire.TraceState]

	for k, v := range h {
		if knownHeaders[k] {
			continue
		}
		if strings.HasPrefix(k, wire.DetailPrefix) {
			if e.DeathDetails == nil {
				e.DeathDetails = map[string]string{}
			}
			e.DeathDetails[strings.TrimPrefix(k, wire.DetailPrefix)] = v
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
	return knownHeaders[k] || strings.HasPrefix(k, wire.Prefix) || strings.HasPrefix(k, "ce_") || strings.HasPrefix(k, "ce-")
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
	wire.SpecVersion: true, wire.ID: true, wire.Source: true, wire.Type: true, wire.Time: true,
	wire.DataContentType: true, wire.Subject: true, wire.DataSchema: true,
	wire.IdempotencyKey: true, wire.Priority: true, wire.Attempt: true,
	wire.MaxAttempts: true, wire.DueTime: true,
	wire.OriginalDest: true, wire.CorrelationID: true, wire.CausationID: true,
	wire.Producer: true, wire.PartitionKey: true, wire.Expiry: true,
	wire.DeathError: true, wire.DeathReason: true, wire.DeathTime: true,
	wire.TraceParent: true, wire.TraceState: true,
}
