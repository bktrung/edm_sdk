package conformance

import (
	"errors"
	"testing"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

func init() { registerGroup("lag", runLag) }

func runLag(group *groupContext) {
	group.Check("lag covers each subscribed destination and no others", func(t *testing.T) {
		first, second := "lag.coverage.first", "lag.coverage.second"
		firstProducer := newProducer(t, group, profileDestination(group, first), driver.ProducerConfig{Effective: group.effective})
		secondProducer := newProducer(t, group, profileDestination(group, second), driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumerFor(t, group, driver.ConsumerConfig{Destinations: []string{first, second}, Prefetch: 1, Effective: group.effective})
		if err := consumer.Drain(group.ctx); err != nil {
			t.Fatal(err)
		}
		if err := firstProducer.Publish(group.ctx, driver.OutboundMessage{Destination: first}); err != nil {
			t.Fatal(err)
		}
		publishCount(t, group, secondProducer, second, 3)
		lag, available := lagMap(t, group, consumer)
		if !available {
			return
		}
		if len(lag) != 2 || lag[first] != 1 || lag[second] != 3 {
			t.Fatalf("Lag=%v, want %q=1 and %q=3", lag, first, second)
		}
	})
	group.Check("lag rises across each published backlog step", func(t *testing.T) {
		name := "lag.rises"
		producer := newProducer(t, group, profileDestination(group, name), driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumer(t, group, profileDestination(group, name), 1)
		previous, available := lagFor(t, group, consumer, name)
		if !available {
			return
		}
		for range 3 {
			if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: name}); err != nil {
				t.Fatal(err)
			}
			lag, _ := lagFor(t, group, consumer, name)
			if lag < previous {
				t.Fatalf("lag fell from %d to %d while publishing", previous, lag)
			}
			previous = lag
		}
		ackAll(t, group, consumer, 3)
	})
	group.Check("lag falls across each settled backlog step", func(t *testing.T) {
		name := "lag.falls"
		producer := newProducer(t, group, profileDestination(group, name), driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumer(t, group, profileDestination(group, name), 1)
		publishCount(t, group, producer, name, 3)
		previous, available := lagFor(t, group, consumer, name)
		if !available {
			ackAll(t, group, consumer, 3)
			return
		}
		for range 3 {
			ackMessage(t, group, receiveMessage(t, group, consumer))
			lag, _ := lagFor(t, group, consumer, name)
			if lag > previous {
				t.Fatalf("lag rose from %d to %d while settling", previous, lag)
			}
			previous = lag
		}
	})
	group.Check("lag equals broker ready depth at the same observation", func(t *testing.T) {
		name := "lag.accuracy"
		producer := newProducer(t, group, profileDestination(group, name), driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumer(t, group, profileDestination(group, name), 1)
		if err := consumer.Drain(group.ctx); err != nil {
			t.Fatal(err)
		}
		publishCount(t, group, producer, name, 4)
		if lag, available := lagFor(t, group, consumer, name); available {
			view := inspectDestination(t, group, name)
			if lag != view.Ready {
				t.Fatalf("Lag=%d, Ready=%d", lag, view.Ready)
			}
		}
	})
	group.Check("unavailable lag returns a classified unsupported error", func(t *testing.T) {
		name := "lag.unsupported"
		newProducer(t, group, profileDestination(group, name), driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumer(t, group, profileDestination(group, name), 1)
		_, err := consumer.Lag(group.ctx)
		if group.effective.LagQueryable {
			if err != nil {
				t.Fatal(err)
			}
			return
		}
		kind, classified := driver.Classify(err)
		if !errors.Is(err, driver.ErrUnsupported) || !classified || kind != driver.KindFatal {
			t.Fatalf("Lag error=%v kind=%v classified=%t", err, kind, classified)
		}
	})
}

func lagMap(t *testing.T, group *groupContext, consumer driver.Consumer) (map[string]int64, bool) {
	t.Helper()
	if !group.effective.LagQueryable {
		_, err := consumer.Lag(group.ctx)
		if !errors.Is(err, driver.ErrUnsupported) {
			t.Fatalf("Lag error=%v", err)
		}
		return nil, false
	}
	lag, err := consumer.Lag(group.ctx)
	if err != nil {
		t.Fatal(err)
	}
	return lag, true
}

func lagFor(t *testing.T, group *groupContext, consumer driver.Consumer, destination string) (int64, bool) {
	t.Helper()
	if !group.effective.LagQueryable {
		_, err := consumer.Lag(group.ctx)
		if !errors.Is(err, driver.ErrUnsupported) {
			t.Fatalf("Lag error=%v", err)
		}
		return 0, false
	}
	lag, err := consumer.Lag(group.ctx)
	if err != nil {
		t.Fatal(err)
	}
	value, ok := lag[destination]
	if !ok {
		t.Fatalf("Lag keys=%v, missing %q", lag, destination)
	}
	return value, true
}
