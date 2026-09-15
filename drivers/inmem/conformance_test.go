package inmem

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver/conformance"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

func newInspector(raw driver.Conn) (conformance.Inspect, error) {
	conn, ok := raw.(*conn)
	if !ok {
		return nil, errors.New("inmem conformance inspector received a different connection")
	}
	return func(ctx context.Context, destination string) (conformance.BrokerView, error) {
		if err := ctx.Err(); err != nil {
			return conformance.BrokerView{}, err
		}
		conn.mu.Lock()
		defer conn.mu.Unlock()
		item, ok := conn.destinations[destination]
		if !ok {
			return conformance.BrokerView{}, driver.ErrDestinationMissing
		}
		var unsettled int64
		for consumer := range item.consumers {
			unsettled += int64(consumer.unsettled[destination])
		}
		now := conn.clock.Now()
		var ready, auxiliary int64
		for _, message := range item.messages {
			if !message.due.IsZero() && now.Before(message.due) {
				auxiliary++
				continue
			}
			ready++
		}
		return conformance.BrokerView{
			Ready:     ready,
			Unsettled: unsettled,
			Auxiliary: auxiliary,
		}, nil
	}, nil
}

func newFaultInjector(raw driver.Conn) (conformance.FaultInjector, error) {
	conn, ok := raw.(*conn)
	if !ok {
		return nil, errors.New("inmem conformance injector received a different connection")
	}
	return func(ctx context.Context, kind conformance.FaultKind) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if kind != conformance.FaultPublishFailure {
			switch kind {
			case conformance.FaultConnectionDrop:
				conn.mu.Lock()
				conn.dropInFlightLocked("connection")
				conn.mu.Unlock()
				return nil
			case conformance.FaultLaneChannelClose:
				conn.mu.Lock()
				conn.dropLaneLocked("lane channel")
				conn.mu.Unlock()
				return nil
			case conformance.FaultDeliveryFailure:
				conn.mu.Lock()
				conn.dropInFlightLocked("delivery")
				conn.mu.Unlock()
				return nil
			case conformance.FaultFatalPublish:
				conn.mu.Lock()
				conn.failPublishFatal = true
				conn.mu.Unlock()
				return nil
			case conformance.FaultCloseFailure:
				conn.mu.Lock()
				conn.closeFault = true
				conn.mu.Unlock()
				return nil
			default:
				return errors.New("unsupported conformance fault")
			}
		}
		conn.mu.Lock()
		defer conn.mu.Unlock()
		conn.failPublish = true
		return nil
	}, nil
}

func (c *conn) dropLaneLocked(op string) {
	var target *consumer
	var destination string
	for item := range c.consumers {
		if len(item.destinations) < 2 {
			continue
		}
		for _, name := range item.destinations {
			if destination == "" || name < destination {
				target = item
				destination = name
			}
		}
	}
	if target == nil {
		return
	}
	now := c.clock.Now()
	for delivery := range target.inflight {
		if delivery.message.message.Destination == destination {
			c.requeueDeliveryLocked(delivery, now)
		}
	}
	if dest, ok := c.destinations[destination]; ok {
		delete(dest.consumers, target)
		for i, item := range dest.order {
			if item == target {
				dest.order = append(dest.order[:i], dest.order[i+1:]...)
				break
			}
		}
		if len(dest.consumers) == 0 {
			c.history[destination] = nil
		}
	}
	select {
	case target.errs <- classify(op, driver.KindTransient, errors.New("injected lane channel closure")):
	default:
	}
	c.dispatchLocked()
	c.signalWake()
}

type deadlineFixture struct {
	conn *conn
	fake *clock.Fake
}

func newDeadlineFixture(raw driver.Conn) (conformance.DeadlineFixture, error) {
	conn, ok := raw.(*conn)
	if !ok {
		return nil, errors.New("inmem conformance deadline fixture received a different connection")
	}
	fake, ok := conn.clock.(*clock.Fake)
	if !ok {
		return nil, errors.New("inmem conformance deadline fixture requires a fake clock")
	}
	return &deadlineFixture{conn: conn, fake: fake}, nil
}

func (f *deadlineFixture) Consumer(ctx context.Context, deadline time.Duration, cfg driver.ConsumerConfig) (driver.Consumer, error) {
	return f.conn.newConsumer(ctx, cfg, deadline)
}

func (f *deadlineFixture) Now() time.Time { return f.fake.Now() }

func (f *deadlineFixture) Advance(duration time.Duration) {
	f.fake.Advance(duration)
	f.conn.mu.Lock()
	f.conn.expireDeadlinesLocked()
	f.conn.dispatchLocked()
	f.conn.signalWake()
	f.conn.mu.Unlock()
}

func TestInspectorSeparatesDeferredMessages(t *testing.T) {
	ctx := context.Background()
	raw, err := (Driver{}).Open(ctx, driver.Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = raw.Close(ctx) }()
	const destination = "conformance.inspector.deferred"
	if _, err := raw.Admin().EnsureTopology(ctx, driver.TopologySpec{
		Destinations: []driver.DestinationSpec{{Name: destination}},
	}); err != nil {
		t.Fatal(err)
	}
	producer, err := raw.Producer(ctx, driver.ProducerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = producer.Close(ctx) }()
	if err := producer.Publish(ctx,
		driver.OutboundMessage{Destination: destination},
		driver.OutboundMessage{Destination: destination, DelayUntil: time.Date(2100, time.January, 1, 0, 0, 0, 0, time.UTC)},
	); err != nil {
		t.Fatal(err)
	}
	inspect, err := newInspector(raw)
	if err != nil {
		t.Fatal(err)
	}
	view, err := inspect(ctx, destination)
	if err != nil {
		t.Fatal(err)
	}
	if view.Ready != 1 || view.Auxiliary != 1 {
		t.Fatalf("deferred message was not separated from ready state: %+v", view)
	}
}

func TestConformance(t *testing.T) {
	var output bytes.Buffer
	// The whole suite shares this fake clock so deferred checks can advance broker
	// time deterministically. Its origin is arbitrary now that receiveBefore spends
	// its own waitTimeout budget instead of time.Until of a fixture instant.
	fake := clock.NewFake(clock.NewReal().Now())
	report := runConformance(t, Driver{clock: fake})
	if err := report.WriteMarkdown(&output); err != nil {
		t.Fatal(err)
	}
	t.Log(output.String())
}

func TestConformanceMinimalCapabilities(t *testing.T) {
	fake := clock.NewFake(clock.NewReal().Now())
	// This named fixture only weakens native declarations and removes limits;
	// ScalingPartitionBound is the one preserved declaration needed to execute
	// that capability's branch, and the check uses one consumer accordingly.
	runConformance(t, Driver{clock: fake, minimal: true})
}

func runConformance(t *testing.T, candidate Driver) conformance.Report {
	t.Helper()
	return conformance.Run(t, conformance.Suite{
		Driver:             candidate,
		Config:             driver.Config{},
		NewInspector:       newInspector,
		NewFaultInjector:   newFaultInjector,
		NewDeadlineFixture: newDeadlineFixture,
	})
}
