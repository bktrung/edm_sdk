package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

type bindingKey struct {
	source      string
	destination string
}

// recordDelays records the delay each destination declares, under every
// topology policy. The producer routes a delayed publish by it, and Purge,
// DescribeTopology and Prune find a destination's parking queue through it.
// Under TopologyNone the operator provisions the queues, but they are the
// queues this spec describes, so routing must follow the spec all the same.
// The first spec for a name wins, as it does when the queues are declared.
func (c *conn) recordDelays(destinations []driver.DestinationSpec) {
	c.mu.Lock()
	defer c.mu.Unlock()
	seen := make(map[string]struct{}, len(destinations))
	for _, destination := range destinations {
		if destination.Name == "" {
			continue
		}
		if _, ok := seen[destination.Name]; ok {
			continue
		}
		seen[destination.Name] = struct{}{}
		if destination.Delay > 0 {
			c.deferred[destination.Name] = destination.Delay
		} else {
			delete(c.deferred, destination.Name)
		}
	}
}

func (a *adminOperations) ensureTopology(ctx context.Context, spec driver.TopologySpec) (driver.TopologyDiff, error) {
	// Refused before anything is recorded, under every policy: a delay no
	// message expiration can carry would otherwise be released early.
	for _, destination := range spec.Destinations {
		if destination.Delay > 0 && !parkDelayFits(destination.Delay) {
			return driver.TopologyDiff{}, classify("ensure_topology", driver.KindFatal,
				fmt.Errorf("destination %q delay %s exceeds the longest message expiration, %dms", destination.Name, destination.Delay, maxParkDelayMillis))
		}
	}
	a.conn.recordDelays(spec.Destinations)
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
			parkName := parkQueueName(destination.Name)
			parkArgs := parkArguments(destination.Name, a.conn.queueKind)
			parkExists, err := a.queueExists(ctx, parkName, true, parkArgs)
			if err != nil {
				return driver.TopologyDiff{}, err
			}
			if parkExists {
				diff.ExistingDestinations = append(diff.ExistingDestinations, parkName)
			} else {
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
			return driver.TopologyDiff{}, classifyManagement("ensure_topology", err)
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
			return driver.TopologyDiff{}, classifyManagement("ensure_topology", a.managementUnavailable(purpose, err))
		}
		diff.Drifted = append(diff.Drifted, drifted...)
		if destination.Delay > 0 {
			parkName := parkQueueName(destination.Name)
			parkArgs := parkArguments(destination.Name, a.conn.queueKind)
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
				return driver.TopologyDiff{}, classifyManagement("ensure_topology", a.managementUnavailable(purpose, err))
			}
			diff.Drifted = append(diff.Drifted, parkDrifted...)
		}
	}
	actualBindings := make(map[bindingKey]struct{})
	if len(spec.Bindings) > 0 {
		var err error
		actualBindings, err = a.currentBindings(ctx)
		if err != nil {
			return driver.TopologyDiff{}, classifyManagement("ensure_topology", err)
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
			expectedPark[parkQueueName(destination.Name)] = struct{}{}
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
		if parent, ok := parkQueueParts(queue.Name); ok {
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

// parkDelayFits reports whether delay can be a parked message's expiration:
// positive, and no more than the broker's limit once rounded up to whole
// milliseconds.
func parkDelayFits(delay time.Duration) bool {
	millis := parkDelayMillis(delay)
	return millis >= 1 && millis <= maxParkDelayMillis
}

// parkDelayMillis rounds delay up to whole milliseconds, so a parked message
// is never released before its delay.
func parkDelayMillis(delay time.Duration) int64 {
	millis := int64(delay / time.Millisecond)
	if delay%time.Millisecond != 0 {
		millis++
	}
	return millis
}

// parkQueueName names the parking queue of a delayed destination. The name
// carries no delay: the wait is each message's own expiration, so a changed
// delay reuses the same queue rather than declaring a new one.
func parkQueueName(destination string) string {
	return destination + parkingSuffix
}

// parkQueueParts returns the destination a parking queue parks for, and ok is
// false for a name that is not a parking queue at all. The suffix is read from
// the end, so a destination whose own name contains ".park." still resolves to
// itself.
func parkQueueParts(name string) (destination string, ok bool) {
	destination, ok = strings.CutSuffix(name, parkingSuffix)
	return destination, ok && destination != ""
}

// parkingOf names the parking queue this connection knows a destination's
// messages may sit in, or returns false for a destination without a delay. It
// takes the read lock itself, so a caller must not hold c.mu.
func (c *conn) parkingOf(destination string) (string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if _, isDeferred := c.deferred[destination]; !isDeferred {
		return "", false
	}
	return parkQueueName(destination), true
}

// parkArguments builds the declare-time arguments for a destination's parking
// queue, the holding queue a delayed message sits in until its expiration
// passes and RabbitMQ dead-letters it onward to destination. The queue carries
// no TTL of its own: each message carries its destination's delay as its
// expiration, so a changed delay needs no change to the queue, which RabbitMQ
// would refuse anyway because a queue's arguments are fixed at declare.
//
// RabbitMQ expires a message only once it reaches the head of its queue. Every
// message on one destination normally carries the same delay, so expiry order
// is publish order and none is held past its due time by the one ahead of it.
// The exception is a delay change: messages published under the old delay
// still sit ahead of the new ones, so right after a decrease, or while a rolling
// deploy publishes both delays, a new message can wait behind an old one, for
// at most the old delay. kind follows the connection's configured queue kind so
// a classic-configured deployment gets classic park queues rather than a
// hardcoded quorum type it may not support.
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
func parkArguments(destination string, kind queueKind) amqp.Table {
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
	if declaredDurable != durable {
		a.conn.reportDurabilityUpgrade(name)
	}
	return nil
}

// queueFlags resolves the declare flags for one destination: the durability
// the spec asked for, except that a quorum queue is durable by definition, so
// kind upgrades a non-durable request instead of leaving the broker to refuse
// the declare. The caller reports that upgrade rather than applying it
// silently.
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
