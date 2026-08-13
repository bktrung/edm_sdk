package rabbitmq

import (
	"context"
	"errors"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

type bindingKey struct {
	source      string
	destination string
}

func (a *admin) ensureTopology(ctx context.Context, spec driver.TopologySpec) (driver.TopologyDiff, error) {
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
		args := queueArguments(destination)
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
	for _, binding := range spec.Bindings {
		if err := ctx.Err(); err != nil {
			return driver.TopologyDiff{}, classify("ensure_topology", driver.KindTransient, err)
		}
		if binding.Source == "" || binding.Destination == "" {
			return driver.TopologyDiff{}, classify("ensure_topology", driver.KindFatal, errors.New("binding source and destination are required"))
		}
		key := bindingKey{source: binding.Source, destination: binding.Destination}
		if _, seen := seenBindings[key]; seen {
			continue
		}
		seenBindings[key] = struct{}{}
		a.conn.mu.RLock()
		known := a.conn.bindings[key]
		a.conn.mu.RUnlock()
		if known {
			diff.Existing = append(diff.Existing, binding.Destination)
			continue
		}
		if err := a.bindQueue(ctx, binding); err != nil {
			return driver.TopologyDiff{}, err
		}
		a.conn.mu.Lock()
		a.conn.bindings[key] = true
		a.conn.mu.Unlock()
		diff.CreatedBindings = append(diff.CreatedBindings, binding.Destination)
	}
	return diff, nil
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

func queueArguments(spec driver.DestinationSpec) amqp.Table {
	args := amqp.Table{}
	if spec.Durable {
		args["x-queue-type"] = "quorum"
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
	declaredDurable, autoDelete, exclusive := queueFlags(durable)
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
	declaredDurable, autoDelete, exclusive := queueFlags(durable)
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

func queueFlags(durable bool) (bool, bool, bool) {
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
