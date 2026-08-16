package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"strings"

	amqp "github.com/rabbitmq/amqp091-go"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

type bindingKey struct {
	source      string
	destination string
}

func (a *admin) ensureTopology(ctx context.Context, spec driver.TopologySpec) (driver.TopologyDiff, error) {
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
			diff.Existing = append(diff.Existing, exchange.Name)
		} else {
			if err := a.declareExchange(ctx, exchange); err != nil {
				return driver.TopologyDiff{}, err
			}
			diff.CreatedExchanges = append(diff.CreatedExchanges, exchange.Name)
		}
		a.conn.mu.Lock()
		a.conn.exchanges[exchange.Name] = struct{}{}
		a.conn.mu.Unlock()
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
		a.conn.mu.Unlock()
		args := queueArguments(destination, a.conn.queueKind)
		exists, err := a.queueExists(ctx, destination.Name, destination.Durable, args)
		if err != nil {
			return driver.TopologyDiff{}, err
		}
		if exists {
			diff.Existing = append(diff.Existing, destination.Name)
		} else {
			if err := a.declareQueue(ctx, destination.Name, destination.Durable, args); err != nil {
				return driver.TopologyDiff{}, err
			}
			diff.CreatedDestinations = append(diff.CreatedDestinations, destination.Name)
		}
		if destination.Delay > 0 {
			parkName := destination.Name + ".park"
			parkArgs := parkingArguments(destination.Name)
			parkExists, err := a.queueExists(ctx, parkName, true, parkArgs)
			if err != nil {
				return driver.TopologyDiff{}, err
			}
			if parkExists {
				diff.Existing = append(diff.Existing, parkName)
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
			diff.Existing = append(diff.Existing, binding.Destination)
			continue
		}
		if err := a.bindQueue(ctx, binding); err != nil {
			return driver.TopologyDiff{}, err
		}
		diff.CreatedBindings = append(diff.CreatedBindings, binding.Destination)
	}
	a.scanOrphans(ctx, spec, &diff)
	return diff, nil
}

func (a *admin) verifyTopology(ctx context.Context, spec driver.TopologySpec) (driver.TopologyDiff, error) {
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
		diff.Existing = append(diff.Existing, exchange.Name)
		a.conn.mu.Lock()
		a.conn.exchanges[exchange.Name] = struct{}{}
		a.conn.mu.Unlock()
	}
	for _, destination := range spec.Destinations {
		if err := ctx.Err(); err != nil {
			return driver.TopologyDiff{}, classify("ensure_topology", driver.KindTransient, err)
		}
		if destination.Name == "" {
			return driver.TopologyDiff{}, classify("ensure_topology", driver.KindFatal, errors.New("destination name is empty"))
		}
		if exists, err := a.queueExists(ctx, destination.Name, destination.Durable, queueArguments(destination, a.conn.queueKind)); err != nil {
			return driver.TopologyDiff{}, err
		} else if !exists {
			return driver.TopologyDiff{}, classify("ensure_topology", driver.KindNotFound, fmt.Errorf("destination %q is missing: %w", destination.Name, driver.ErrDestinationMissing))
		}
		diff.Existing = append(diff.Existing, destination.Name)
		a.conn.mu.Lock()
		if destination.Delay > 0 {
			a.conn.deferred[destination.Name] = destination.Delay
		}
		a.conn.mu.Unlock()
		if destination.Delay > 0 {
			parkName := destination.Name + ".park"
			exists, err := a.queueExists(ctx, parkName, true, parkingArguments(destination.Name))
			if err != nil {
				return driver.TopologyDiff{}, err
			}
			if !exists {
				return driver.TopologyDiff{}, classify("ensure_topology", driver.KindNotFound, fmt.Errorf("parking destination %q is missing: %w", parkName, driver.ErrDestinationMissing))
			}
			diff.Existing = append(diff.Existing, parkName)
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
		diff.Existing = append(diff.Existing, binding.Destination)
	}
	return diff, nil
}

func (a *admin) currentBindings(ctx context.Context) (map[bindingKey]struct{}, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if a.conn.management == nil {
		return nil, errors.New("rabbitmq: binding verification unsupported: management client is unavailable")
	}
	bindings, err := a.conn.management.listBindings(ctx)
	if err != nil {
		return nil, fmt.Errorf("rabbitmq: binding verification unavailable: %w", err)
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

func validateBinding(binding driver.BindingSpec) error {
	if binding.Source == "" || binding.Destination == "" {
		return errors.New("binding source and destination are required")
	}
	return nil
}

func (a *admin) scanOrphans(ctx context.Context, spec driver.TopologySpec, diff *driver.TopologyDiff) {
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
		if strings.HasSuffix(queue.Name, ".park") {
			parent := strings.TrimSuffix(queue.Name, ".park")
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

func (a *admin) freshQueueList(ctx context.Context, scopes []string) ([]managementQueue, error) {
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

func queueArguments(spec driver.DestinationSpec, kind queueKind) amqp.Table {
	args := amqp.Table{}
	args["x-queue-type"] = string(kind)
	if spec.DeliveryLimit > 0 {
		if spec.DeliveryLimit > 2147483647 {
			spec.DeliveryLimit = 2147483647
		}
		args["x-delivery-limit"] = int32(spec.DeliveryLimit) //nolint:gosec // bounded above before conversion
	}
	if spec.DeadLetter != nil {
		args["x-dead-letter-exchange"] = spec.DeadLetter.Exchange
		args["x-dead-letter-routing-key"] = spec.DeadLetter.Key
	}
	return args
}

func parkingArguments(destination string) amqp.Table {
	return amqp.Table{
		"x-queue-type":              "quorum",
		"x-dead-letter-exchange":    "",
		"x-dead-letter-routing-key": destination,
	}
}

func (a *admin) exchangeExists(ctx context.Context, spec driver.ExchangeSpec) (bool, error) {
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

func (a *admin) declareExchange(ctx context.Context, spec driver.ExchangeSpec) error {
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

func (a *admin) queueExists(ctx context.Context, name string, durable bool, args amqp.Table) (bool, error) {
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

func (a *admin) declareQueue(ctx context.Context, name string, durable bool, args amqp.Table) error {
	channel, err := a.openChannel(ctx)
	if err != nil {
		return err
	}
	declaredDurable, autoDelete, exclusive := queueFlags(durable, a.conn.queueKind)
	if _, err := channel.QueueDeclare(name, declaredDurable, autoDelete, exclusive, false, args); err != nil {
		_ = channel.Close()
		return classifyAMQP("ensure_topology", driver.KindFatal, err)
	}
	if durable {
		_ = channel.Close()
		return nil
	}
	a.conn.mu.Lock()
	old := a.conn.ephemeral[name]
	a.conn.ephemeral[name] = channel
	a.conn.mu.Unlock()
	if old != nil {
		_ = old.Close()
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

func (a *admin) bindQueue(ctx context.Context, binding driver.BindingSpec) error {
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

func (a *admin) openChannel(ctx context.Context) (*amqp.Channel, error) {
	if err := ctx.Err(); err != nil {
		return nil, classify("ensure_topology", driver.KindTransient, err)
	}
	a.conn.mu.RLock()
	defer a.conn.mu.RUnlock()
	if a.conn.closed || a.conn.amqp.IsClosed() {
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
