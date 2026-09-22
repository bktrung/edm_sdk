package f1

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sort"
	"strings"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

// Publisher publishes encoded events through a Client's connected driver.
type Publisher struct {
	client *Client
}

type publishOptions struct {
	topic          string
	key            string
	subject        string
	priority       Priority
	idempotencyKey string
	correlationID  string
	causedBy       *Event
	headers        map[string]string
	expiry         *time.Time
	maxAttempts    int
}

// PublishOption configures one Publish or PublishBatch call. Construct values
// with the With* functions; the concrete operation is intentionally private.
type PublishOption struct {
	apply func(*publishOptions) error
}

// Message is one item in a PublishBatch call, carrying the event type, payload,
// and options that apply only to that item.
type Message struct {
	EventType string
	Payload   any
	Opts      []PublishOption
}

// BatchResult reports each message's publish outcome in input order. A partial
// failure is represented by the corresponding MessageResult entries rather
// than by an atomicity claim.
type BatchResult struct {
	Results []MessageResult
}

// MessageResult is one message's publish outcome; ID is empty when Err is set.
type MessageResult struct {
	ID  string
	Err error
}

// Failed returns indexes of messages that did not publish.
func (r BatchResult) Failed() []int {
	failed := make([]int, 0)
	for i, result := range r.Results {
		if result.Err != nil {
			failed = append(failed, i)
		}
	}
	return failed
}

// WithTopic overrides what the topic is derived from. The value goes through
// the same derivation as an event type.
func WithTopic(topic string) PublishOption {
	return publishOption(func(options *publishOptions) error { options.topic = topic; return nil })
}

// WithKey sets the partition key; the same envelope value is used to derive
// the broker routing key. It defaults to the subject and then the event ID.
func WithKey(key string) PublishOption {
	return publishOption(func(options *publishOptions) error { options.key = key; return nil })
}

// WithSubject sets the business subject and default partition key.
func WithSubject(subject string) PublishOption {
	return publishOption(func(options *publishOptions) error { options.subject = subject; return nil })
}

// WithPriority selects the delivery lane.
func WithPriority(priority Priority) PublishOption {
	return publishOption(func(options *publishOptions) error { options.priority = priority; return nil })
}

// WithIdempotencyKey sets the application's stable deduplication key.
func WithIdempotencyKey(key string) PublishOption {
	return publishOption(func(options *publishOptions) error { options.idempotencyKey = key; return nil })
}

// WithCorrelationID sets the workflow correlation identifier.
func WithCorrelationID(id string) PublishOption {
	return publishOption(func(options *publishOptions) error { options.correlationID = id; return nil })
}

// WithCausedBy links the new event to an earlier event and inherits its
// correlation and trace context when those values are not overridden.
func WithCausedBy(event *Event) PublishOption {
	return publishOption(func(options *publishOptions) error { options.causedBy = event; return nil })
}

// WithHeader adds a user extension header.
func WithHeader(key, value string) PublishOption {
	return publishOption(func(options *publishOptions) error {
		if options.headers == nil {
			options.headers = make(map[string]string)
		}
		options.headers[key] = value
		return nil
	})
}

// WithExpiry sets the event expiration time.
func WithExpiry(expiry time.Time) PublishOption {
	return publishOption(func(options *publishOptions) error { options.expiry = &expiry; return nil })
}

// WithMaxAttempts caps this event's retry ladder below the subscription policy.
func WithMaxAttempts(attempts int) PublishOption {
	return publishOption(func(options *publishOptions) error {
		if attempts < 1 {
			return fmt.Errorf("f1: WithMaxAttempts requires attempts >= 1")
		}
		options.maxAttempts = attempts
		return nil
	})
}

func publishOption(apply func(*publishOptions) error) PublishOption {
	return PublishOption{apply: apply}
}

// Publish encodes payload, builds its envelope, and waits for durable broker
// acknowledgement before returning the event ID.
//
// The topic is derived from eventType, unless WithTopic supplies a value to
// derive instead. A trailing version segment of the form .v<N> is stripped,
// and a value without that suffix is used as-is.
func (p *Publisher) Publish(ctx context.Context, eventType string, payload any, opts ...PublishOption) (string, error) {
	result, err := p.PublishBatch(ctx, []Message{{EventType: eventType, Payload: payload, Opts: opts}})
	if err != nil {
		return "", err
	}
	if len(result.Results) == 0 || result.Results[0].Err != nil {
		if len(result.Results) == 0 {
			return "", errors.New("f1: publish produced no result")
		}
		return "", result.Results[0].Err
	}
	return result.Results[0].ID, nil
}

// PublishBatch publishes messages synchronously without an atomicity claim.
// It waits for durable acknowledgement, reports per-message partial failures
// in BatchResult, and returns transport-wide failures as the method error.
func (p *Publisher) PublishBatch(ctx context.Context, messages []Message) (BatchResult, error) {
	result := BatchResult{Results: make([]MessageResult, len(messages))}
	if p == nil || p.client == nil {
		return result, errors.New("f1: publisher is not connected")
	}
	if len(messages) == 0 {
		return result, nil
	}

	// Close is a barrier: a publish admitted here, before closing is set,
	// is counted as in flight for Close's idle wait for the rest of this
	// call, however long buildOutbound's caller-supplied codec work takes.
	// A publish that reaches this check after Close has already set closing
	// is rejected immediately; it is never given the chance to slip through
	// a later check while Close is already tearing the connection down.
	p.client.mu.Lock()
	if err := p.client.admit(workPublishEntry, 0); err != nil {
		p.client.mu.Unlock()
		return result, err
	}
	// The connection and its incarnation are one value, read in the section
	// that records this publish as in flight: the admission the producer build
	// later takes belongs to the connection that was current when the publish
	// was admitted, and the epoch travels with it as the claim the build is
	// re-checked against. Reading the epoch anywhere else would compare a claim
	// about one incarnation against another.
	current := p.client.current
	epoch := current.epoch
	effective := p.client.effective
	headerMaxBytes := effectiveHeaderLimit(p.client.config.Codec.MaxHeaderBytes, effective.MaxHeaderBytes)
	options := p.client.options
	source := p.client.source
	producerIdentity := p.client.producer
	maxBodyBytes := p.client.config.Codec.MaxBodyBytes
	beginPublish(p.client)
	p.client.mu.Unlock()
	defer endPublish(p.client)

	// The observer is read once after the admission section releases c.mu.
	// recordObserverPanic takes c.mu, so the publish start must come after
	// the Unlock above. Values only; nothing escapes on the nil path.
	observer := p.client.observer
	var publishGuard observerFinishGuard
	observed := false
	if observer != nil {
		start := StartEvent{
			Kind:      ObserverPublish,
			At:        options.clock.Now(),
			Route:     PublishRoutePrimary,
			BatchSize: len(messages),
		}
		nextCtx, token := p.client.observeStart(ctx, start)
		ctx = nextCtx
		publishGuard = p.client.newObserverGuard(ObserverPublish, token)
		observed = true
		defer publishGuard.abandon()
	}

	outbound := make([]driver.OutboundMessage, len(messages))
	ids := make([]string, len(messages))
	var publishFields primaryPublishFields
	for i, message := range messages {
		outboundMessage, id, err := buildOutbound(ctx, p.client, options, effective, headerMaxBytes, source, producerIdentity, message, &publishFields)
		if err != nil {
			methodErr := fmt.Errorf("f1: message %d: %w", i, err)
			if observed {
				publishGuard.finishWith(FinishEvent{Outcome: ObserverOutcomeError, ErrorClass: errorClassOf(methodErr), Results: result.Results})
			}
			return result, methodErr
		}
		// codec.maxBodyBytes is a caller-facing guardrail: it exists so an
		// application publish with an oversized payload fails fast and
		// visibly, the same way an oversized inbound delivery is
		// dead-lettered. It is enforced only here, on a publish this call
		// originated. The SDK's own retry, DLQ, and backstop republishes
		// (worker.go) carry the already-received body forward unchanged and
		// are deliberately exempt: applying this limit to a successor copy
		// would turn a body that was already accepted once into a new,
		// silent message-loss path, which is exactly the class of bug the
		// SDK's own retry and DLQ handling exists to prevent.
		if maxBodyBytes > 0 && len(outboundMessage.Body) > maxBodyBytes {
			methodErr := fmt.Errorf("f1: message %d: body exceeds codec.maxBodyBytes (%d)", i, maxBodyBytes)
			if observed {
				publishGuard.finishWith(FinishEvent{Outcome: ObserverOutcomeError, ErrorClass: errorClassOf(methodErr), Results: result.Results})
			}
			return result, methodErr
		}
		outbound[i] = outboundMessage
		ids[i] = id
	}
	var publishTopic string
	var publishPriority Priority
	if publishFields.set && !publishFields.mixed {
		publishTopic = publishFields.topic
		publishPriority = publishFields.priority
	}

	// This publish was already admitted by its entry gate above and is counted
	// in Close's idle wait, so the lifecycle may legitimately have moved to
	// Draining by now; only a closed client, a torn-down producer, or a failed
	// or replaced connection stop it from proceeding. The producer-teardown
	// refusal cannot fire on this path: beginPublish above already counted this
	// call as in flight, and producerTeardown is only set once the publish-idle
	// wait observes zero in-flight publishes, which cannot happen while this
	// call is one of them. Moving beginPublish to run after this check would
	// break that.
	built := p.client.sharedProducer(ctx, workPublish, epoch, nil)
	if built.refused != nil {
		if observed {
			publishGuard.finishWith(FinishEvent{Topic: publishTopic, Priority: publishPriority, Outcome: ObserverOutcomeError, ErrorClass: errorClassOf(built.refused), Results: result.Results})
		}
		return result, built.refused
	}
	if built.buildErr != nil {
		warnUnclassifiedWithContext(p.client.options.logger, ctx, built.buildErr)
		requestReconnectOnTransient(p.client, built.buildErr, epoch)
		methodErr := fmt.Errorf("f1: create publisher: %w", built.buildErr)
		if observed {
			publishGuard.finishWith(FinishEvent{Topic: publishTopic, Priority: publishPriority, Outcome: ObserverOutcomeError, ErrorClass: errorClassOf(methodErr), Results: result.Results})
		}
		return result, methodErr
	}
	publishErr := built.producer.Publish(ctx, outbound...)
	if publishErr == nil {
		for i, id := range ids {
			result.Results[i].ID = id
		}
		if observed {
			publishGuard.finishWith(FinishEvent{Topic: publishTopic, Priority: publishPriority, Outcome: ObserverOutcomeOK, Results: result.Results})
		}
		return result, nil
	}

	warnPublishError(p.client.options.logger, ctx, publishErr)
	requestReconnectOnTransient(p.client, publishErr, epoch)
	var partial *driver.PublishError
	if errors.As(publishErr, &partial) && len(partial.Failed) > 0 {
		for i, id := range ids {
			result.Results[i].ID = id
		}
		failed := make([]int, 0, len(partial.Failed))
		for index := range partial.Failed {
			failed = append(failed, index)
		}
		sort.Ints(failed)
		for _, index := range failed {
			if index >= 0 && index < len(result.Results) {
				result.Results[index].ID = ""
				result.Results[index].Err = partial.Failed[index]
			}
		}
		if observed {
			var firstErr error
			for _, r := range result.Results {
				if r.Err != nil {
					firstErr = r.Err
					break
				}
			}
			publishGuard.finishWith(FinishEvent{Topic: publishTopic, Priority: publishPriority, Outcome: ObserverOutcomeError, ErrorClass: errorClassOf(firstErr), Results: result.Results})
		}
		return result, nil
	}
	for i := range result.Results {
		result.Results[i].Err = publishErr
	}
	if observed {
		publishGuard.finishWith(FinishEvent{Topic: publishTopic, Priority: publishPriority, Outcome: ObserverOutcomeError, ErrorClass: errorClassOf(publishErr), Results: result.Results})
	}
	return result, publishErr
}

func warnPublishError(logger *slog.Logger, ctx context.Context, err error) {
	if logger == nil {
		return
	}
	if partial, ok := errors.AsType[*driver.PublishError](err); ok {
		indexes := make([]int, 0, len(partial.Failed))
		for index := range partial.Failed {
			indexes = append(indexes, index)
		}
		sort.Ints(indexes)
		for _, index := range indexes {
			warnUnclassifiedWithContext(logger, ctx, partial.Failed[index])
		}
		return
	}
	warnUnclassifiedWithContext(logger, ctx, err)
}

func warnUnclassifiedWithContext(logger *slog.Logger, ctx context.Context, err error) {
	if logger == nil || err == nil {
		return
	}
	if _, classified := driver.Classify(err); !classified {
		logger.LogAttrs(ctx, slog.LevelWarn, "f1 unclassified publish error", slog.Any("error", err))
	}
}

type primaryPublishFields struct {
	topic    string
	priority Priority
	set      bool
	mixed    bool
}

func (f *primaryPublishFields) record(topic string, priority Priority) {
	if f == nil {
		return
	}
	if !f.set {
		f.topic = topic
		f.priority = priority
		f.set = true
		return
	}
	if f.topic != topic || f.priority != priority {
		f.mixed = true
	}
}

func buildOutbound(ctx context.Context, c *Client, options clientOptions, effective driver.Capabilities, headerMaxBytes int, source, producerIdentity string, message Message, fields *primaryPublishFields) (driver.OutboundMessage, string, error) {
	if err := ctx.Err(); err != nil {
		return driver.OutboundMessage{}, "", err
	}
	if message.EventType == "" {
		return driver.OutboundMessage{}, "", errors.New("event type must not be empty")
	}
	for _, option := range message.Opts {
		if option.apply == nil {
			return driver.OutboundMessage{}, "", errors.New("nil publish option")
		}
	}
	publish := publishOptions{priority: PriorityMedium}
	for _, option := range message.Opts {
		if err := option.apply(&publish); err != nil {
			return driver.OutboundMessage{}, "", err
		}
	}
	if !publish.priority.Valid() {
		return driver.OutboundMessage{}, "", ErrInvalidPriority
	}
	now := options.clock.Now().UTC()
	id, err := newEventID(now)
	if err != nil {
		return driver.OutboundMessage{}, "", err
	}
	body, err := options.codec.Encode(message.Payload)
	if err != nil {
		return driver.OutboundMessage{}, "", fmt.Errorf("encode payload: %w", err)
	}
	topicInput := message.EventType
	if publish.topic != "" {
		topicInput = publish.topic
	}
	topic := topicFor(topicInput)
	if err := validatePublishTopic(options, topic, topicInput); err != nil {
		return driver.OutboundMessage{}, "", err
	}
	fields.record(topic, publish.priority)
	partitionKey := publish.key
	if partitionKey == "" {
		partitionKey = publish.subject
	}
	if partitionKey == "" {
		partitionKey = id
	}
	envelope := Envelope{
		SpecVersion:     "1.0",
		ID:              id,
		Source:          source,
		Type:            message.EventType,
		Time:            now,
		DataContentType: options.codec.ContentType(),
		Subject:         publish.subject,
		IdempotencyKey:  publish.idempotencyKey,
		Priority:        publish.priority,
		Attempt:         1,
		CorrelationID:   publish.correlationID,
		Producer:        producerIdentity,
		PartitionKey:    publish.key,
		Expiry:          publish.expiry,
		MaxAttempts:     publish.maxAttempts,
		Extensions:      publish.headers,
	}
	if envelope.IdempotencyKey == "" {
		envelope.IdempotencyKey = id
	}
	if publish.causedBy != nil {
		envelope.CausationID = eventID(publish.causedBy)
		if publish.correlationID == "" {
			envelope.CorrelationID = publish.causedBy.envelope.CorrelationID
			if envelope.CorrelationID == "" {
				envelope.CorrelationID = eventID(publish.causedBy)
			}
		}
		envelope.TraceParent = publish.causedBy.envelope.TraceParent
		envelope.TraceState = publish.causedBy.envelope.TraceState
	}
	// The message_built stage starts after the ID and the envelope identity
	// are known and before EncodeHeaders. The observer and the injector are
	// read once from the client; options stays the by-value snapshot the
	// publish uses. The guard is a value, and nothing escapes when the
	// observer is nil.
	observer := c.observer
	injector := c.traceInjector
	var guard observerFinishGuard
	var base FinishEvent
	observed := false
	entryPoint := publishEntryPoint(source, topic, publish.priority)
	if observer != nil {
		correlationID := envelope.CorrelationID
		if correlationID == "" {
			correlationID = id
		}
		destination := entryPoint
		start := StartEvent{
			Kind:          ObserverMessageBuilt,
			At:            options.clock.Now(),
			Topic:         topic,
			Destination:   destination,
			MessageID:     id,
			EventType:     message.EventType,
			Priority:      publish.priority,
			Attempt:       1,
			CorrelationID: correlationID,
		}
		nextCtx, token := c.observeStart(ctx, start)
		ctx = nextCtx
		base = FinishEvent{
			Topic:         topic,
			Destination:   destination,
			MessageID:     id,
			EventType:     message.EventType,
			Priority:      publish.priority,
			Attempt:       1,
			CorrelationID: correlationID,
		}
		guard = c.newObserverGuard(ObserverMessageBuilt, token)
		observed = true
		defer guard.abandon()
	}
	if injector != nil {
		traceParent, traceState := c.injectTrace(ctx)
		if traceParent != "" {
			envelope.TraceParent = traceParent
			envelope.TraceState = traceState
		}
	}
	headerMap, err := envelope.EncodeHeaders(headerMaxBytes)
	if err != nil {
		if observed {
			finish := base
			finish.Outcome = ObserverOutcomeError
			finish.ErrorClass = errorClassOf(err)
			guard.finishWith(finish)
		}
		return driver.OutboundMessage{}, "", err
	}
	if observed {
		finish := base
		finish.Outcome = ObserverOutcomeOK
		guard.finishWith(finish)
	}
	headers := make([]driver.Header, 0, len(headerMap))
	keys := make([]string, 0, len(headerMap))
	for key := range headerMap {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		headers = append(headers, driver.Header{Key: key, Value: []byte(headerMap[key])})
	}
	return driver.OutboundMessage{
		Destination: entryPoint,
		EntryPoint:  isFanoutEntryPoint(effective),
		Key:         []byte(partitionKey),
		Headers:     headers,
		Body:        body,
		Priority:    priorityHint(publish.priority),
	}, id, nil
}

func topicFor(eventType string) string {
	index := strings.LastIndex(eventType, ".v")
	if index < 0 || index+2 == len(eventType) {
		return eventType
	}
	for _, char := range eventType[index+2:] {
		if char < '0' || char > '9' {
			return eventType
		}
	}
	return eventType[:index]
}

func validatePublishTopic(options clientOptions, topic, topicInput string) error {
	if !options.publishTopicsSet {
		return nil
	}
	if slices.Contains(options.publishTopics, topic) {
		return nil
	}
	return fmt.Errorf("topic %q derived from input %q was not listed in WithPublishTopics; listed topics: %v", topic, topicInput, options.publishTopics)
}

func priorityHint(priority Priority) uint8 {
	switch priority {
	case PriorityHigh:
		return 1
	case PriorityLow:
		return 2
	default:
		return 0
	}
}

func sourceEnvironment(source string) string {
	env := strings.TrimPrefix(source, "/")
	if index := strings.IndexByte(env, '/'); index >= 0 {
		env = env[:index]
	}
	return env
}

func publishEntryPoint(source, topic string, priority Priority) string {
	return "f1." + sourceEnvironment(source) + "." + topic + "." + priority.String()
}

// isFanoutEntryPoint reports whether a publish entry point is declared as a
// fan-out exchange under the given effective capability set.
//
// This is the one place that decision is made, and both the publish path
// (buildOutbound, deciding OutboundMessage.EntryPoint) and the topology-declare
// path (publisherTopologySpec, deciding whether to declare an exchange or a
// plain destination) call it with the same effective value. Computing the
// same boolean twice from two independent reads would let a publish be
// marked as an entry point on a capability set where the exchange was never
// declared; funnelling both decisions through one function on one value
// makes that divergence structurally impossible rather than merely unlikely.
func isFanoutEntryPoint(effective driver.Capabilities) bool {
	return effective.Fanout == driver.FanoutAtPublish
}

func publisherTopologySpec(effective driver.Capabilities, source string, topics []string, priorities []Priority) driver.TopologySpec {
	result := driver.TopologySpec{Effective: effective}
	for _, topic := range topics {
		for _, priority := range priorities {
			entryPoint := publishEntryPoint(source, topic, priority)
			if isFanoutEntryPoint(effective) {
				result.Exchanges = append(result.Exchanges, driver.ExchangeSpec{Name: entryPoint, Kind: "fanout", Durable: true})
			} else {
				result.Destinations = append(result.Destinations, driver.DestinationSpec{Name: entryPoint, Kind: driver.DestMain, Durable: true})
			}
		}
	}
	return result
}

func newEventID(now time.Time) (string, error) {
	var bytes [16]byte
	if _, err := cryptorand.Read(bytes[:]); err != nil {
		return "", fmt.Errorf("generate event id: %w", err)
	}
	millis := uint64(now.UnixNano() / int64(time.Millisecond))
	var timestamp [8]byte
	binary.BigEndian.PutUint64(timestamp[:], millis)
	copy(bytes[:6], timestamp[2:])
	bytes[6] = (bytes[6] & 0x0f) | 0x70
	bytes[8] = (bytes[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", binary.BigEndian.Uint32(bytes[0:4]), binary.BigEndian.Uint16(bytes[4:6]), binary.BigEndian.Uint16(bytes[6:8]), binary.BigEndian.Uint16(bytes[8:10]), bytes[10:]), nil
}

func eventID(event *Event) string {
	if event == nil {
		return ""
	}
	return event.envelope.ID
}
