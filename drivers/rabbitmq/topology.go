package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

type bindingKey struct {
	source      string
	destination string
}

func (a *adminOperations) ensureTopology(ctx context.Context, spec driver.TopologySpec) (driver.TopologyDiff, error) {
	if spec.Policy == driver.TopologyNone {
		return driver.TopologyDiff{}, nil
	}
	if spec.Policy == driver.TopologyVerify {
		return a.verifyTopology(ctx, spec)
	}
	a.conn.topologyMu.Lock()
	defer a.conn.topologyMu.Unlock()

	var diff driver.TopologyDiff
	seenExchanges := make(map[string]struct{}, len(spec.Exchanges))
	for _, exchange := range spec.Exchanges {
		if err := ctx.Err(); err != nil {
			return driver.TopologyDiff{}, classify("ensure_topology", driver.KindTransient, err)
		}
		if _, seen := seenExchanges[exchange.Name]; seen {
			continue
		}
		seenExchanges[exchange.Name] = struct{}{}
		if err := validateExchange(exchange); err != nil {
			return driver.TopologyDiff{}, classify("ensure_topology", driver.KindFatal, err)
		}
		exists, err := a.exchangeExists(ctx, exchange)
		if err != nil {
			return driver.TopologyDiff{}, err
		}
		if exists {
			diff.ExistingExchanges = append(diff.ExistingExchanges, exchange.Name)
		} else {
			if err := a.declareExchange(ctx, exchange); err != nil {
				return driver.TopologyDiff{}, err
			}
			diff.CreatedExchanges = append(diff.CreatedExchanges, exchange.Name)
		}
	}

	seenDestinations := make(map[string]struct{}, len(spec.Destinations))
	for _, destination := range spec.Destinations {
		if err := ctx.Err(); err != nil {
			return driver.TopologyDiff{}, classify("ensure_topology", driver.KindTransient, err)
		}
		if destination.Name == "" {
			return driver.TopologyDiff{}, classify("ensure_topology", driver.KindFatal, errors.New("destination name is empty"))
		}
		if _, seen := seenDestinations[destination.Name]; seen {
			continue
		}
		seenDestinations[destination.Name] = struct{}{}
		a.conn.mu.Lock()
		if destination.Delay > 0 {
			a.conn.deferred[destination.Name] = destination.Delay
		} else {
			delete(a.conn.deferred, destination.Name)
		}
		if fixedDelay, isFixed := fixedParkDelay(destination); isFixed {
			a.conn.fixed[destination.Name] = fixedDelay
		} else {
			delete(a.conn.fixed, destination.Name)
		}
		a.conn.mu.Unlock()
		args := queueArguments(destination, a.conn.queueKind, a.conn.consumerTimeout)
		exists, err := a.queueExists(ctx, destination.Name, destination.Durable, args)
		if err != nil {
			return driver.TopologyDiff{}, err
		}
		if exists {
			diff.ExistingDestinations = append(diff.ExistingDestinations, destination.Name)
		} else {
			if err := a.declareQueue(ctx, destination.Name, destination.Durable, args); err != nil {
				return driver.TopologyDiff{}, err
			}
			diff.CreatedDestinations = append(diff.CreatedDestinations, destination.Name)
		}
		if destination.Delay > 0 {
			for _, parkName := range parkQueueNamesFor(destination) {
				parkArgs := parkArguments(destination.Name, a.conn.queueKind, parkName)
				parkExists, err := a.queueExists(ctx, parkName, true, parkArgs)
				if err != nil {
					return driver.TopologyDiff{}, err
				}
				if parkExists {
					diff.ExistingDestinations = append(diff.ExistingDestinations, parkName)
					continue
				}
				if err := a.declareQueue(ctx, parkName, true, parkArgs); err != nil {
					return driver.TopologyDiff{}, err
				}
				diff.CreatedDestinations = append(diff.CreatedDestinations, parkName)
			}
		}
	}

	seenBindings := make(map[bindingKey]struct{}, len(spec.Bindings))
	actualBindings := make(map[bindingKey]struct{})
	if len(spec.Bindings) > 0 {
		var err error
		actualBindings, err = a.currentBindings(ctx)
		if err != nil {
			return driver.TopologyDiff{}, classify("ensure_topology", driver.KindTransient, err)
		}
	}
	for _, binding := range spec.Bindings {
		if err := ctx.Err(); err != nil {
			return driver.TopologyDiff{}, classify("ensure_topology", driver.KindTransient, err)
		}
		if err := validateBinding(binding); err != nil {
			return driver.TopologyDiff{}, classify("ensure_topology", driver.KindFatal, err)
		}
		key := bindingKey{source: binding.Source, destination: binding.Destination}
		if _, seen := seenBindings[key]; seen {
			continue
		}
		seenBindings[key] = struct{}{}
		if _, known := actualBindings[key]; known {
			diff.ExistingBindings = append(diff.ExistingBindings, binding)
			continue
		}
		if err := a.bindQueue(ctx, binding); err != nil {
			return driver.TopologyDiff{}, err
		}
		diff.CreatedBindings = append(diff.CreatedBindings, binding)
	}
	a.scanOrphans(ctx, spec, &diff)
	return diff, nil
}

func (a *adminOperations) verifyTopology(ctx context.Context, spec driver.TopologySpec) (driver.TopologyDiff, error) {
	a.conn.topologyMu.Lock()
	defer a.conn.topologyMu.Unlock()
	var diff driver.TopologyDiff
	for _, exchange := range spec.Exchanges {
		if err := ctx.Err(); err != nil {
			return driver.TopologyDiff{}, classify("ensure_topology", driver.KindTransient, err)
		}
		if err := validateExchange(exchange); err != nil {
			return driver.TopologyDiff{}, classify("ensure_topology", driver.KindFatal, err)
		}
		exists, err := a.exchangeExists(ctx, exchange)
		if err != nil {
			return driver.TopologyDiff{}, err
		}
		if !exists {
			return driver.TopologyDiff{}, classify("ensure_topology", driver.KindNotFound, fmt.Errorf("exchange %q is missing: %w", exchange.Name, driver.ErrDestinationMissing))
		}
		diff.ExistingExchanges = append(diff.ExistingExchanges, exchange.Name)
	}
	for _, destination := range spec.Destinations {
		if err := ctx.Err(); err != nil {
			return driver.TopologyDiff{}, classify("ensure_topology", driver.KindTransient, err)
		}
		if destination.Name == "" {
			return driver.TopologyDiff{}, classify("ensure_topology", driver.KindFatal, errors.New("destination name is empty"))
		}
		mainArgs := queueArguments(destination, a.conn.queueKind, a.conn.consumerTimeout)
		if exists, err := a.queueExists(ctx, destination.Name, destination.Durable, mainArgs); err != nil {
			return driver.TopologyDiff{}, err
		} else if !exists {
			return driver.TopologyDiff{}, classify("ensure_topology", driver.KindNotFound, fmt.Errorf("destination %q is missing: %w", destination.Name, driver.ErrDestinationMissing))
		}
		diff.ExistingDestinations = append(diff.ExistingDestinations, destination.Name)
		drifted, err := a.argumentDrift(ctx, destination.Name, mainArgs)
		if err != nil {
			purpose := fmt.Sprintf("argument drift verification for destination %q", destination.Name)
			return driver.TopologyDiff{}, classify("ensure_topology", driver.KindTransient, a.managementUnavailable(purpose, err))
		}
		diff.Drifted = append(diff.Drifted, drifted...)
		a.conn.mu.Lock()
		if destination.Delay > 0 {
			a.conn.deferred[destination.Name] = destination.Delay
		}
		if fixedDelay, isFixed := fixedParkDelay(destination); isFixed {
			a.conn.fixed[destination.Name] = fixedDelay
		} else {
			delete(a.conn.fixed, destination.Name)
		}
		a.conn.mu.Unlock()
		if destination.Delay > 0 {
			for _, parkName := range parkQueueNamesFor(destination) {
				parkArgs := parkArguments(destination.Name, a.conn.queueKind, parkName)
				exists, err := a.queueExists(ctx, parkName, true, parkArgs)
				if err != nil {
					return driver.TopologyDiff{}, err
				}
				if !exists {
					return driver.TopologyDiff{}, classify("ensure_topology", driver.KindNotFound, fmt.Errorf("parking destination %q is missing: %w", parkName, driver.ErrDestinationMissing))
				}
				diff.ExistingDestinations = append(diff.ExistingDestinations, parkName)
				parkDrifted, err := a.argumentDrift(ctx, parkName, parkArgs)
				if err != nil {
					purpose := fmt.Sprintf("argument drift verification for parking destination %q", parkName)
					return driver.TopologyDiff{}, classify("ensure_topology", driver.KindTransient, a.managementUnavailable(purpose, err))
				}
				diff.Drifted = append(diff.Drifted, parkDrifted...)
			}
		}
	}
	actualBindings := make(map[bindingKey]struct{})
	if len(spec.Bindings) > 0 {
		var err error
		actualBindings, err = a.currentBindings(ctx)
		if err != nil {
			return driver.TopologyDiff{}, classify("ensure_topology", driver.KindTransient, err)
		}
	}
	seenBindings := make(map[bindingKey]struct{}, len(spec.Bindings))
	for _, binding := range spec.Bindings {
		if err := validateBinding(binding); err != nil {
			return driver.TopologyDiff{}, classify("ensure_topology", driver.KindFatal, err)
		}
		key := bindingKey{source: binding.Source, destination: binding.Destination}
		if _, seen := seenBindings[key]; seen {
			continue
		}
		seenBindings[key] = struct{}{}
		if _, exists := actualBindings[key]; !exists {
			return driver.TopologyDiff{}, classify("ensure_topology", driver.KindNotFound, fmt.Errorf("binding %q -> %q is missing: %w", binding.Source, binding.Destination, driver.ErrDestinationMissing))
		}
		diff.ExistingBindings = append(diff.ExistingBindings, binding)
	}
	return diff, nil
}

func (a *adminOperations) currentBindings(ctx context.Context) (map[bindingKey]struct{}, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	bindings, err := a.conn.management.listBindings(ctx)
	if err != nil {
		return nil, a.managementUnavailable("binding verification", err)
	}
	actual := make(map[bindingKey]struct{}, len(bindings))
	for _, binding := range bindings {
		if binding.DestinationType != "queue" || binding.RoutingKey != "" {
			continue
		}
		actual[bindingKey{source: binding.Source, destination: binding.Destination}] = struct{}{}
	}
	return actual, nil
}

// managementUnavailable wraps a failure to reach the broker's management HTTP
// API with the deployment action that resolves it. The subscription path
// cannot do this inspection over AMQP: a passive declare confirms a queue's
// name but reports none of its arguments, so bindings and argument values are
// visible only through management, and an unreachable API fails subscription
// topology under TopologyDeclare and TopologyVerify rather than degrading -
// the orphan scan is the one inspection that only records the limitation.
//
// The message names the endpoint that was tried because it is derived rather
// than configured - the AMQP host with the AMQP port plus 10000 - so the
// common failure on a broker whose management plugin is disabled, firewalled,
// or on another port is otherwise indistinguishable from a broker that is down.
func (a *adminOperations) managementUnavailable(purpose string, err error) error {
	return fmt.Errorf("rabbitmq: %s requires the management API at %s (enable the RabbitMQ management plugin, or set rabbitmq.managementPort when management does not listen on the AMQP port plus 10000): %w", purpose, a.conn.management.baseURL, err)
}

// argumentDrift compares the arguments a destination was declared with
// against what the broker actually holds, read through the management API
// because AMQP's QueueDeclarePassive checks the queue's name only and
// ignores the arguments passed to it. The comparison is one-directional:
// only keys present in want are checked, so broker-added arguments outside
// want never appear as drift.
//
// A failure reaching the management client is returned as an error rather
// than folded into an empty result: TopologyVerify must be able to say "this
// was not checked" instead of silently reporting a clean diff. The client
// itself is never absent, because newConn is the only construction of a conn
// the port hands out, it fails Open when newManagementClient cannot build
// one, and nothing reassigns the field, so the only failure to report here is
// reaching the broker rather than a missing client.
func (a *adminOperations) argumentDrift(ctx context.Context, name string, want amqp.Table) ([]driver.ArgumentDrift, error) {
	if len(want) == 0 {
		return nil, nil
	}
	queue, err := a.conn.management.getQueue(ctx, name)
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(want))
	for key := range want {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var drifted []driver.ArgumentDrift
	for _, key := range keys {
		wantValue := want[key]
		gotValue, present := queue.Arguments[key]
		if present && argumentValuesEqual(wantValue, gotValue) {
			continue
		}
		got := "<absent>"
		if present {
			got = fmt.Sprint(gotValue)
		}
		drifted = append(drifted, driver.ArgumentDrift{
			Name:     name,
			Argument: key,
			Want:     fmt.Sprint(wantValue),
			Got:      got,
		})
	}
	return drifted, nil
}

// argumentValuesEqual compares one declared argument value against the
// broker's reported value for the same key. The two never share a Go type
// even when they agree: AMQP integer arguments go over the wire as int32 or
// similar, and the management API's JSON response decodes every number to
// float64, so a plain interface{} equality check would report every
// integer argument as permanently drifted. Numeric types are compared by
// value; everything else falls back to fmt.Sprint, which is enough for the
// string arguments this driver declares (queue type, exchange and routing
// key names).
func argumentValuesEqual(want, got any) bool {
	wantNumber, wantIsNumber := toFloat64(want)
	gotNumber, gotIsNumber := toFloat64(got)
	if wantIsNumber || gotIsNumber {
		return wantIsNumber && gotIsNumber && wantNumber == gotNumber
	}
	return fmt.Sprint(want) == fmt.Sprint(got)
}

func toFloat64(value any) (float64, bool) {
	switch v := value.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	default:
		return 0, false
	}
}

func validateBinding(binding driver.BindingSpec) error {
	if binding.Source == "" || binding.Destination == "" {
		return errors.New("binding source and destination are required")
	}
	return nil
}

func (a *adminOperations) scanOrphans(ctx context.Context, spec driver.TopologySpec, diff *driver.TopologyDiff) {
	if len(spec.Scope) == 0 {
		diff.OrphanScanError = "orphan scan disabled because no scope was supplied"
		return
	}
	queues, err := a.freshQueueList(ctx, spec.Scope)
	if err != nil {
		diff.OrphanScanError = err.Error()
		return
	}
	known := make(map[string]struct{}, len(spec.Destinations))
	byName := make(map[string]managementQueue, len(queues))
	for _, destination := range spec.Destinations {
		known[destination.Name] = struct{}{}
	}
	expectedPark := make(map[string]struct{})
	for _, destination := range spec.Destinations {
		if destination.Delay > 0 {
			for _, parkName := range parkQueueNamesFor(destination) {
				expectedPark[parkName] = struct{}{}
			}
		}
	}
	for _, queue := range queues {
		byName[queue.Name] = queue
	}
	orphans := make(map[string]int64)
	order := make([]string, 0)
	add := func(name string, messages int64) {
		if _, exists := orphans[name]; !exists {
			order = append(order, name)
		}
		orphans[name] += messages
	}
	for _, queue := range queues {
		if !matchesScope(queue.Name, spec.Scope) {
			continue
		}
		if _, exists := known[queue.Name]; exists {
			continue
		}
		if parent, _, ok := parkQueueParts(queue.Name); ok {
			if _, parentKnown := known[parent]; parentKnown {
				if _, expected := expectedPark[queue.Name]; !expected {
					add(queue.Name, queue.totalMessages())
					continue
				}
			}
			if _, parentExists := byName[parent]; parentExists {
				if _, parentKnown := known[parent]; !parentKnown {
					add(parent, queue.totalMessages())
				}
				continue
			}
		}
		add(queue.Name, queue.totalMessages())
	}
	for _, name := range order {
		diff.Orphaned = append(diff.Orphaned, driver.OrphanedDestination{Name: name, Messages: orphans[name]})
	}
}

func (a *adminOperations) freshQueueList(ctx context.Context, scopes []string) ([]managementQueue, error) {
	queues, err := a.conn.management.listQueues(ctx)
	if err != nil {
		return nil, err
	}
	for index := range queues {
		queue := &queues[index]
		if !matchesScope(queue.Name, scopes) || queue.totalMessages() > 0 {
			continue
		}
		ready, inspectErr := a.inspectQueue(ctx, queue.Name)
		if inspectErr == nil && ready > 0 {
			if int64(ready) > queue.MessagesReady {
				queue.MessagesReady = int64(ready)
			}
			if int64(ready) > queue.Messages {
				queue.Messages = int64(ready)
			}
		}
	}
	return queues, nil
}

func matchesScope(name string, scopes []string) bool {
	for _, scope := range scopes {
		if strings.HasPrefix(name, scope) {
			return true
		}
	}
	return false
}

func validateExchange(exchange driver.ExchangeSpec) error {
	if exchange.Name == "" {
		return errors.New("exchange name is empty")
	}
	switch exchange.Kind {
	case "direct", "topic", "fanout":
		return nil
	default:
		return fmt.Errorf("unsupported exchange kind %q", exchange.Kind)
	}
}

// consumerTimeoutOption is the DriverOptions key carrying the per-queue
// consumer timeout.
const consumerTimeoutOption = "rabbitmq.consumerTimeout"

// resolveConsumerTimeout reads the consumer timeout this driver declares as
// x-consumer-timeout on every quorum destination queue. An absent option
// declares nothing, leaving the broker's own timeout in force instead of
// imposing this driver's choice on a deployment that never made one.
//
// A present option must be at least a millisecond. The broker reads the
// argument as whole milliseconds, and a value that truncates to zero cancels
// every consumer on the queue on its first delivery, so accepting it would
// turn a typo into a subscription that never keeps a message.
func resolveConsumerTimeout(options map[string]string) (time.Duration, error) {
	configured, present := options[consumerTimeoutOption]
	if !present {
		return 0, nil
	}
	timeout, err := time.ParseDuration(configured)
	if err != nil {
		return 0, fmt.Errorf("rabbitmq: invalid %s %q: %w", consumerTimeoutOption, configured, err)
	}
	if timeout < time.Millisecond {
		return 0, fmt.Errorf("rabbitmq: invalid %s %q: want at least 1ms, because the broker reads x-consumer-timeout in whole milliseconds and zero cancels every consumer immediately", consumerTimeoutOption, configured)
	}
	return timeout, nil
}

// queueArguments builds the declare-time arguments for a destination queue.
// When the queue is quorum-kind and spec carries a dead-letter route, it adds
// x-dead-letter-strategy and x-overflow alongside the route so the broker's
// at-least-once dead-letter delivery guarantee actually applies. RabbitMQ
// requires all three (the DLX/DLRK pair plus both of these) together;
// dropping any one of them downgrades dead-lettering to at-most-once with no
// error from the broker, so a future edit that removes one of the three
// reintroduces silent message loss and no test outside this file will fail.
//
// x-consumer-timeout is quorum-only. RabbitMQ 4.3 answers a classic queue
// declared with it with a 406 PRECONDITION_FAILED - invalid arg
// 'x-consumer-timeout' - which closes the channel, so declaring it on a
// classic-configured connection would fail every declare rather than being
// quietly ignored.
func queueArguments(spec driver.DestinationSpec, kind queueKind, consumerTimeout time.Duration) amqp.Table {
	args := amqp.Table{}
	args["x-queue-type"] = string(kind)
	if kind == queueKindQuorum && consumerTimeout > 0 {
		args["x-consumer-timeout"] = consumerTimeout.Milliseconds()
	}
	if spec.DeliveryLimit > 0 {
		if spec.DeliveryLimit > 2147483647 {
			spec.DeliveryLimit = 2147483647
		}
		args["x-delivery-limit"] = int32(spec.DeliveryLimit) //nolint:gosec // bounded above before conversion
	}
	if spec.DeadLetter != nil {
		args["x-dead-letter-exchange"] = spec.DeadLetter.Exchange
		args["x-dead-letter-routing-key"] = spec.DeadLetter.Key
		if kind == queueKindQuorum {
			args["x-dead-letter-strategy"] = "at-least-once"
			args["x-overflow"] = "reject-publish"
		}
	}
	return args
}

// parkRungs is the delay ladder the parking queues of one deferred
// destination are cut along, ascending.
//
// A per-message expiration expires only when the message reaches the head of
// its queue, so a message parked behind a later one waits for the one in
// front of it and the lateness is the difference between two due times, which
// is unbounded. A queue-level x-message-ttl expires in FIFO order for every
// message in the queue and for every queue, so a bucket whose messages share
// one TTL cannot construct that case at all. Routing rounds the remaining
// delay up to the smallest rung at least as large, which buys two properties:
// a due time is never released early, because every rung is at least its own
// delay, and due-time order is preserved across buckets, because rounding up
// is non-decreasing. The cost is lateness below one rung, which the caller
// absorbs: the conformance band for a due time is wider than the rung its
// delay rounds into.
//
// Eight rungs cover every delay the retry path can produce with a rung to
// spare. Config.DelayFor tops out at MaxInterval, and ResolveRetryAfter
// returns the nominal delay of the tier it picks, so the largest delay at the
// shipped defaults is MaxInterval 30s while the top rung is 64s. A delay
// above the top rung routes to no rung at all: see target.
var parkRungs = [...]time.Duration{
	500 * time.Millisecond,
	1 * time.Second,
	2 * time.Second,
	4 * time.Second,
	8 * time.Second,
	16 * time.Second,
	32 * time.Second,
	64 * time.Second,
}

// parkRungTags names each rung inside a parking queue name. A rung is spelled
// as a fixed tag rather than as a rendering of its duration, so that reading
// a name back is a lookup in a closed set and never a wildcard tail: the
// conformance suite declares destinations named topology.prune.park.eligible,
// and a parser that accepted anything after ".park." would read that name as
// a rung queue of topology.prune.
var parkRungTags = map[time.Duration]string{
	500 * time.Millisecond: "500ms",
	1 * time.Second:        "1s",
	2 * time.Second:        "2s",
	4 * time.Second:        "4s",
	8 * time.Second:        "8s",
	16 * time.Second:       "16s",
	32 * time.Second:       "32s",
	64 * time.Second:       "64s",
}

// parkRungByTag is parkRungTags reversed, so parsing is one map read.
var parkRungByTag = func() map[string]time.Duration {
	byTag := make(map[string]time.Duration, len(parkRungTags))
	for rung, tag := range parkRungTags {
		byTag[tag] = rung
	}
	return byTag
}()

// parkRung rounds a remaining delay up to the rung that holds it, or returns
// zero when the delay is above the ladder and belongs on the per-message path
// instead. Rounding up is what keeps a parked message from being released
// before its due time.
func parkRung(delay time.Duration) time.Duration {
	for _, rung := range parkRungs {
		if delay <= rung {
			return rung
		}
	}
	return 0
}

// fixedParkDelay reports whether a destination parks every message in one
// queue with the destination's own delay. It is true only when the spec asks
// for a fixed delay, the delay is positive, and the delay rounded up to whole
// milliseconds fits the broker's per-message expiration limit.
func fixedParkDelay(spec driver.DestinationSpec) (time.Duration, bool) {
	if !spec.FixedDelay {
		return 0, false
	}
	if spec.Delay <= 0 {
		return 0, false
	}
	millis := int64(spec.Delay / time.Millisecond)
	if spec.Delay%time.Millisecond != 0 {
		millis++
	}
	if millis < 1 || millis > maxExpirationMillis {
		return 0, false
	}
	return spec.Delay, true
}

// fixedParkQueueName names the single parking queue of a fixed-delay
// destination. The delay goes into the name in whole milliseconds, rounded
// up, so a changed delay declares a new queue instead of conflicting on the
// old one's TTL.
func fixedParkQueueName(destination string, delay time.Duration) string {
	millis := int64(delay / time.Millisecond)
	if delay%time.Millisecond != 0 {
		millis++
	}
	return destination + parkingSuffix + ".fixed-" + strconv.FormatInt(millis, 10) + "ms"
}

// parseFixedParkTag parses the tag after ".park." of a fixed-delay parking
// queue. It accepts only "fixed-<ms>ms" where <ms> is decimal, at least 1,
// has no leading zero, and is at most the broker's expiration limit.
func parseFixedParkTag(tag string) (time.Duration, bool) {
	const prefix = "fixed-"
	if !strings.HasPrefix(tag, prefix) {
		return 0, false
	}
	if !strings.HasSuffix(tag, "ms") {
		return 0, false
	}
	digits := tag[len(prefix) : len(tag)-len("ms")]
	if len(digits) == 0 {
		return 0, false
	}
	if digits[0] < '1' || digits[0] > '9' {
		return 0, false
	}
	for i := 1; i < len(digits); i++ {
		if digits[i] < '0' || digits[i] > '9' {
			return 0, false
		}
	}
	millis, err := strconv.ParseInt(digits, 10, 64)
	if err != nil {
		return 0, false
	}
	if millis < 1 || millis > maxExpirationMillis {
		return 0, false
	}
	return time.Duration(millis) * time.Millisecond, true
}

// parkQueueName is the queue one rung of one destination parks in.
func parkQueueName(destination string, rung time.Duration) string {
	return destination + parkingSuffix + "." + parkRungTags[rung]
}

// parkQueueNames lists every parking queue of a deferred destination: one per
// rung, then the queue that a delay above the ladder parks in.
func parkQueueNames(destination string) []string {
	names := make([]string, 0, len(parkRungs)+1)
	for _, rung := range parkRungs {
		names = append(names, parkQueueName(destination, rung))
	}
	return append(names, destination+parkingSuffix)
}

// parkQueueParts splits a parking queue name into the destination it parks for
// and the rung it belongs to. A rung of zero means the queue for delays above
// the ladder, and ok is false for a name that is not a parking queue at all.
//
// The rung suffix has to be one of the known tags. A name that merely carries
// something after ".park." is not a parking queue, which is what keeps a
// destination named "orders.park.eligible" a destination rather than a rung of
// "orders". The tag is read from the last separator, so a destination whose own
// name contains ".park." still resolves to itself.
func parkQueueParts(name string) (destination string, rung time.Duration, ok bool) {
	if index := strings.LastIndex(name, parkingSuffix+"."); index >= 0 {
		known, isRung := parkRungByTag[name[index+len(parkingSuffix)+1:]]
		if !isRung {
			fixed, isFixed := parseFixedParkTag(name[index+len(parkingSuffix)+1:])
			if !isFixed {
				return "", 0, false
			}
			return name[:index], fixed, true
		}
		return name[:index], known, true
	}
	if before, found := strings.CutSuffix(name, parkingSuffix); found {
		return before, 0, true
	}
	return "", 0, false
}

// parkQueueNamesFor lists the parking queues of one destination spec: the
// single fixed queue plus the per-message queue for a fixed-delay
// destination, and the ladder plus the per-message queue otherwise.
func parkQueueNamesFor(spec driver.DestinationSpec) []string {
	if delay, isFixed := fixedParkDelay(spec); isFixed {
		return []string{fixedParkQueueName(spec.Name, delay), spec.Name + parkingSuffix}
	}
	return parkQueueNames(spec.Name)
}

// parkingOf lists the parking queues this connection knows a destination's
// messages may sit in: none for a destination without a delay, the ladder and
// the per-message queue for a deferred one, and also the single fixed queue
// for a fixed-delay one. The ladder stays listed for a fixed-delay destination
// because rung queues left by an earlier version still drain into it. It takes
// the read lock itself, so a caller must not hold c.mu.
func (c *conn) parkingOf(destination string) []string {
	c.mu.RLock()
	_, isDeferred := c.deferred[destination]
	fixedDelay, isFixed := c.fixed[destination]
	c.mu.RUnlock()
	if !isDeferred {
		return nil
	}
	names := parkQueueNames(destination)
	if isFixed {
		names = append(names, fixedParkQueueName(destination, fixedDelay))
	}
	return names
}

// parkingArguments builds the declare-time arguments for a destination's
// parking queue, the holding queue a delayed or retried message sits in until
// its per-message TTL expires and RabbitMQ dead-letters it onward to
// destination. This is the queue a delay above the ladder parks in; a delay on
// the ladder parks in a rung queue declared by rungParkingArguments instead.
// kind follows the connection's configured queue kind so a classic-configured
// deployment gets classic park queues rather than a hardcoded quorum type it
// may not support.
//
// On quorum, x-dead-letter-strategy and x-overflow are added alongside the
// dead-letter exchange and routing key. All three are required together to
// get RabbitMQ's at-least-once dead-letter delivery guarantee; the park
// queue is the delay mechanism itself, not a failure path, so losing a
// dead-lettered message here silently drops a retry. Removing any one of the
// three arguments downgrades to at-most-once with no error from the broker.
// Classic queues do not support at-least-once dead-lettering, so both are
// omitted there and the delay path stays at-most-once on a classic
// deployment.
func parkingArguments(destination string, kind queueKind) amqp.Table {
	args := amqp.Table{
		"x-queue-type":              string(kind),
		"x-dead-letter-exchange":    "",
		"x-dead-letter-routing-key": destination,
	}
	if kind == queueKindQuorum {
		args["x-dead-letter-strategy"] = "at-least-once"
		args["x-overflow"] = "reject-publish"
	}
	return args
}

// rungParkingArguments adds the queue-level TTL that makes expiry order FIFO
// order for every message in a rung queue. That TTL is the entire difference
// between this shape and the per-message one: a message parked here carries no
// expiration of its own, so nothing in the queue can outlive the message
// behind it and no message can be held past its own due time by one ahead of
// it. Rounding the delay up to the rung is what keeps that from releasing
// anything early.
func rungParkingArguments(destination string, kind queueKind, rung time.Duration) amqp.Table {
	args := parkingArguments(destination, kind)
	args["x-message-ttl"] = int32(rung.Milliseconds()) //nolint:gosec // the ladder is a fixed set of small durations
	return args
}

// parkArguments is the declare argument set of one parking queue, chosen by
// the queue's own name: a rung queue carries the rung's queue-level TTL, and
// the queue above the ladder carries a per-message expiration instead and
// declares none.
func parkArguments(destination string, kind queueKind, parkName string) amqp.Table {
	if _, rung, ok := parkQueueParts(parkName); ok && rung > 0 {
		return rungParkingArguments(destination, kind, rung)
	}
	return parkingArguments(destination, kind)
}

func (a *adminOperations) exchangeExists(ctx context.Context, spec driver.ExchangeSpec) (bool, error) {
	channel, err := a.openChannel(ctx)
	if err != nil {
		return false, err
	}
	defer channel.Close()
	err = channel.ExchangeDeclarePassive(spec.Name, spec.Kind, spec.Durable, false, false, false, nil)
	if err == nil {
		return true, nil
	}
	if isNotFound(err) {
		return false, nil
	}
	return false, classifyAMQP("ensure_topology", driver.KindFatal, err)
}

func (a *adminOperations) declareExchange(ctx context.Context, spec driver.ExchangeSpec) error {
	channel, err := a.openChannel(ctx)
	if err != nil {
		return err
	}
	defer channel.Close()
	if err := channel.ExchangeDeclare(spec.Name, spec.Kind, spec.Durable, false, false, false, nil); err != nil {
		return classifyAMQP("ensure_topology", driver.KindFatal, err)
	}
	return nil
}

func (a *adminOperations) queueExists(ctx context.Context, name string, durable bool, args amqp.Table) (bool, error) {
	channel, err := a.openChannel(ctx)
	if err != nil {
		return false, err
	}
	defer channel.Close()
	declaredDurable, autoDelete, exclusive := queueFlags(durable, a.conn.queueKind)
	_, err = channel.QueueDeclarePassive(name, declaredDurable, autoDelete, exclusive, false, args)
	if err == nil {
		return true, nil
	}
	if isNotFound(err) {
		return false, nil
	}
	return false, classifyAMQP("ensure_topology", driver.KindFatal, err)
}

func (a *adminOperations) declareQueue(ctx context.Context, name string, durable bool, args amqp.Table) error {
	channel, err := a.openChannel(ctx)
	if err != nil {
		return err
	}
	defer channel.Close()
	declaredDurable, autoDelete, exclusive := queueFlags(durable, a.conn.queueKind)
	if _, err := channel.QueueDeclare(name, declaredDurable, autoDelete, exclusive, false, args); err != nil {
		return classifyAMQP("ensure_topology", driver.KindFatal, err)
	}
	return nil
}

func queueFlags(durable bool, kind queueKind) (bool, bool, bool) {
	if kind == queueKindQuorum {
		return true, false, false
	}
	if durable {
		return true, false, false
	}
	return false, false, true
}

func (a *adminOperations) bindQueue(ctx context.Context, binding driver.BindingSpec) error {
	channel, err := a.openChannel(ctx)
	if err != nil {
		return err
	}
	defer channel.Close()
	if err := channel.QueueBind(binding.Destination, "", binding.Source, false, nil); err != nil {
		return classifyAMQP("ensure_topology", driver.KindFatal, err)
	}
	return nil
}

func (a *adminOperations) openChannel(ctx context.Context) (*amqp.Channel, error) {
	if err := ctx.Err(); err != nil {
		return nil, classify("ensure_topology", driver.KindTransient, err)
	}
	a.conn.mu.RLock()
	defer a.conn.mu.RUnlock()
	if a.conn.closed || a.conn.closing || a.conn.amqp.IsClosed() {
		return nil, classify("ensure_topology", driver.KindTransient, amqp.ErrClosed)
	}
	channel, err := a.conn.amqp.Channel()
	if err != nil {
		return nil, classifyAMQP("ensure_topology", driver.KindTransient, err)
	}
	return channel, nil
}

func isNotFound(err error) bool {
	var amqpErr *amqp.Error
	return errors.As(err, &amqpErr) && amqpErr.Code == 404
}

func isPermission(err error) bool {
	var amqpErr *amqp.Error
	return errors.As(err, &amqpErr) && amqpErr.Code == 403
}
