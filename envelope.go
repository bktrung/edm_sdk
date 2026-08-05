package f1

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Envelope is the wire format, CloudEvents 1.0-compatible with the f1
// extension set (doc 03 §1). Attribute names below are the CANONICAL,
// broker-agnostic namespace doc 03 §2 defines; a driver maps these onto its
// own header namespace (Kafka's ce_ prefix, AMQP's cloudEvents: prefix -
// doc 03 §3.1/§3.2). EncodeHeaders/DecodeHeaders operate at this canonical
// level, not the broker-specific one.
type Envelope struct {
	// CloudEvents core
	SpecVersion     string
	ID              string
	Source          string
	Type            string
	Time            time.Time
	DataContentType string
	Subject         string
	DataSchema      string

	// F1 extensions
	IdempotencyKey string
	Priority       Priority // int in Go, string on the wire - see Priority.String
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

	// DLQ only
	DeathError  string
	DeathReason DeathReason // string in Go and on the wire; zero value "" is "not dead-lettered"
	DeathTime   *time.Time

	// W3C trace context (doc 03 §2.4/§2.5). These are NOT f1 extensions and
	// are NOT free-form: they ride every traced message and must survive
	// every hop, so they get typed fields rather than living in Extensions.
	// Giving them no home was F-P46 - decode had nowhere to put them but
	// Extensions, which rule 4 below then rejected on the way back out, so a
	// retry or DLQ copy of any traced message could not be built.
	TraceParent string
	TraceState  string

	// Unrecognised reserved-prefix attributes received from the wire. These
	// are kept separate from caller-written Extensions so a decoded envelope
	// can always be re-encoded for retry or DLQ forwarding.
	Forwarded map[string]string

	// Free-form, propagated untouched. The "f1", "ce_" and "ce-" prefixes are
	// reserved (doc 03 §2.3 opens "All prefixed f1" - the whole namespace is
	// reserved, not the roster of attributes declared so far), as are every
	// CloudEvents attribute and the two trace fields above, and RabbitMQ's
	// own death-history headers (doc 03 §3.3 rule 3 - stripped on decode,
	// never entered here). Reserved is enforced on encode, not merely
	// documented (rule 4, F-P45/F-P46).
	Extensions map[string]string
}

// timeLayout is RFC3339Nano, not RFC3339: doc 03 says "RFC3339 UTC" for the
// wire type, but Nano is what makes the round trip lossless for a producer
// that stamped sub-second precision - Parse accepts a bare-second value just
// as well, so nothing that already conforms to doc 03 §2 is rejected.
const timeLayout = time.RFC3339Nano

// CoreMaxHeaderBytes is the core's own header cap (doc 03 §3.3). The
// effective cap for a publish is min(CoreMaxHeaderBytes, the connected
// driver's Capabilities.MaxHeaderBytes) - EncodeHeaders takes the driver
// side of that min as a parameter, since the driver port (doc 04) does not
// exist yet at this milestone (M1-05 depends on M1-04/M1-03c only).
const CoreMaxHeaderBytes = 8 * 1024

// EncodeHeaders serialises e to its wire header form. driverMaxBytes is the
// connected driver's declared limit; pass 0 (or anything >= CoreMaxHeaderBytes)
// to apply only the core's own cap.
//
// Doc 03 §3.3's five rules: the cap applies here regardless of whether this
// is a first-publish, retry or DLQ copy (rule 1 - this function does not
// distinguish, so it is applied uniformly); over the cap, Extensions is
// dropped first, then DeathError is truncated further, and encoding never
// fails on SIZE (rule 2 - the DLQ is the one destination a copy that refuses
// to shrink must still reach); broker-internal headers are handled on the
// decode side, not here (rule 3, see DecodeHeaders); Forwarded is written first so canonical
// attributes overwrite it (rule 5, see DecodeHeaders); an Extensions key that
// collides with a canonical attribute, or begins with the reserved f1, ce_ or ce-
// prefix, is rejected, not merged over it (rule 4, F-P45/F-P46) - unlike
// rule 2, this is a first-publish programming error, not a DLQ copy that has
// to reach its destination regardless.
//
// F-P45 also closes the encode side's other unguarded case: an invalid
// Priority (Valid() false) is rejected with ErrInvalidPriority rather than
// letting String()'s "invalid" reach the wire.
//
// F-P36: a root event (one with no CorrelationID set) gets f1correlationid
// defaulted to the event's own id here, the same treatment f1idempotencykey
// already gets - the field can then never be silently empty.
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
		h["f1correlationid"] = e.ID // F-P36: root default
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

// shrinkDeathError truncates f1deatherror until h fits limit, never failing:
// a truncated error beats an absent message (doc 03 §3.3 rule 2).
func shrinkDeathError(h map[string]string, limit int) {
	for headerBytes(h) > limit {
		v, ok := h["f1deatherror"]
		if !ok || v == "" {
			return // cannot shrink further; best effort
		}
		over := headerBytes(h) - limit
		cut := len(v) - over
		if cut < 0 {
			cut = 0
		}
		h["f1deatherror"] = v[:cut]
	}
}

// DecodeHeaders parses the canonical wire header form back into an Envelope.
// Everything not recognised falls into Extensions, except a broker-internal
// key (x-death, x-first-death-*, see isBrokerReserved) - those are stripped,
// never entered (doc 03 §3.3 rule 3); unrecognised reserved-prefix keys
// go to Forwarded for round-trip preservation (rule 5): otherwise a message that
// transited a broker backstop re-emits a flattened x-death table on every
// subsequent hop and headers grow monotonically along the ladder.
//
// f1deathreason is preserved exactly as read, never validated -
// TestDLQ_UnknownReasonSurvivesReplay pins this; see DeathReason.Valid's
// godoc for why. f1priority is validated: an unrecognised value is an error,
// since a lane this build cannot schedule must not be silently misrouted.
//
// traceparent/tracestate are read into their own fields (F-P46), never into
// Extensions - see Envelope's own doc comment for why they need a home at
// all.
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
			continue // broker-internal, stripped (doc 03 §3.3 rule 3)
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

// isBrokerReserved names the specific broker-internal keys doc 03 §3.3 rule
// 3 calls out (RabbitMQ's death-history headers) - NOT a blanket "x-"
// prefix, which would also eat a legitimate user extension that happens to
// start with "x-".
func isBrokerReserved(k string) bool {
	return k == "x-death" || strings.HasPrefix(k, "x-first-death-")
}

// isReservedExtensionKey reports whether an Extensions key collides with a
// canonical attribute (doc 03 §3.3 rule 4, as amended by F-P46): a match
// against a known header name - which now includes traceparent/tracestate,
// so a caller setting one by hand collides with the typed fields above - or
// the reserved "f1", "ce_" or "ce-" PREFIX. A prefix, not a name list: doc 03 §2.3 opens
// "All prefixed f1", so the whole namespace is reserved, not the roster of
// attributes declared today - probed, f1bogus and ce_type both used to reach
// the wire.
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
