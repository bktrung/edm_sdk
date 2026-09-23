// Package wire names the message headers F1 writes on the wire, so the
// core that writes them and the drivers that map some of them onto broker
// properties share one spelling. A rename here moves every writer and reader
// together; a typo in one of them is a compile error rather than a header
// nobody reads.
package wire

// CloudEvents attributes, carried as headers in binary content mode.
const (
	// SpecVersion is the CloudEvents specversion attribute.
	SpecVersion = "specversion"
	// ID is the CloudEvents id attribute, the event's own identifier.
	ID = "id"
	// Source is the CloudEvents source attribute.
	Source = "source"
	// Type is the CloudEvents type attribute.
	Type = "type"
	// Time is the CloudEvents time attribute, in RFC 3339 form.
	Time = "time"
	// DataContentType is the CloudEvents datacontenttype attribute.
	DataContentType = "datacontenttype"
	// DataSchema is the CloudEvents dataschema attribute.
	DataSchema = "dataschema"
	// Subject is the CloudEvents subject attribute.
	Subject = "subject"
)

// W3C trace context headers.
const (
	// TraceParent is the W3C traceparent header.
	TraceParent = "traceparent"
	// TraceState is the W3C tracestate header.
	TraceState = "tracestate"
)

// F1 extension headers.
const (
	// Prefix starts every F1 extension header name.
	Prefix = "f1"
	// IdempotencyKey carries the publisher's idempotency key.
	IdempotencyKey = "f1idempotencykey"
	// Priority carries the message priority name.
	Priority = "f1priority"
	// Attempt carries the delivery attempt number.
	Attempt = "f1attempt"
	// MaxAttempts carries the attempt limit the message was sent with.
	MaxAttempts = "f1maxattempts"
	// DueTime carries the time a delayed message becomes due.
	DueTime = "f1duetime"
	// OriginalDest carries the destination a retry or dead-letter copy came from.
	OriginalDest = "f1originaldest"
	// CorrelationID carries the correlation identifier shared along a causal chain.
	CorrelationID = "f1correlationid"
	// CausationID carries the identifier of the event that caused this one.
	CausationID = "f1causationid"
	// Producer carries the service/env/instanceID of the client that published
	// the message.
	Producer = "f1producer"
	// PartitionKey carries the message key the publisher set.
	PartitionKey = "f1partitionkey"
	// Expiry carries the time after which the message is dead-lettered
	// instead of handled.
	Expiry = "f1expiry"
	// DeathError carries the error text of a dead-lettered message.
	DeathError = "f1deatherror"
	// DeathReason carries why a message was dead-lettered.
	DeathReason = "f1deathreason"
	// DeathTime carries when a message was dead-lettered.
	DeathTime = "f1deathtime"
	// DetailPrefix starts one header per death detail; the detail's key
	// follows it.
	DetailPrefix = "f1detail"
)
