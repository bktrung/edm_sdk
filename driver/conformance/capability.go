package conformance

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

func init() { registerGroup("capability", runCapability) }

func runCapability(group *groupContext) {
	capabilityDeclarationChecks(group)
	group.Check("connection capabilities do not widen factory capabilities", func(t *testing.T) {
		factory := group.factoryCapabilities
		connection := group.conn.Capabilities()
		if !factory.PerMessageAck && connection.PerMessageAck {
			t.Fatalf("PerMessageAck widened from false to true")
		}
		if !factory.OrderedByKey && connection.OrderedByKey {
			t.Fatalf("OrderedByKey widened from false to true")
		}
		if !factory.NativeDelay && connection.NativeDelay {
			t.Fatalf("NativeDelay widened from false to true")
		}
		if !factory.NativeDeliveryCount && connection.NativeDeliveryCount {
			t.Fatalf("NativeDeliveryCount widened from false to true")
		}
		if !factory.NativeDLQ && connection.NativeDLQ {
			t.Fatalf("NativeDLQ widened from false to true")
		}
		if !factory.LagQueryable && connection.LagQueryable {
			t.Fatalf("LagQueryable widened from false to true")
		}
	})

	group.Check("per-message settlement leaves sibling deliveries unsettled", func(t *testing.T) {
		name := "capability.ack.independent"
		producer := newPlacedProducer(t, group, name, driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumer(t, group, profileDestination(group, name), 2)
		if err := producer.Publish(group.ctx,
			driver.OutboundMessage{Destination: name, Key: []byte(placementKey(0)), Body: []byte("one")},
			driver.OutboundMessage{Destination: name, Key: []byte(placementKey(1)), Body: []byte("two")},
		); err != nil {
			t.Fatal(err)
		}
		first := receiveMessage(t, group, consumer)
		second := receiveMessage(t, group, consumer)
		ackMessage(t, group, first)
		waitFor(t, group, "one sibling delivery to remain unsettled", func() (bool, string) {
			view := inspectDestination(t, group, name)
			return view.Unsettled == 1, fmt.Sprintf("view=%+v", view)
		})
		ackMessage(t, group, second)
		waitFor(t, group, "all capability deliveries to settle", func() (bool, string) {
			view := inspectDestination(t, group, name)
			return view.Unsettled == 0, fmt.Sprintf("view=%+v", view)
		})
		group.capability("PerMessageAck/independent", strconv.FormatBool(group.effective.PerMessageAck), capabilityBoolStatus(group.effective.PerMessageAck), "one settlement leaves the sibling broker delivery unsettled")
	})

	group.Check("ordered receipt keeps equal keys in publish order", func(t *testing.T) {
		name := "capability.order"
		if !group.effective.OrderedByKey {
			group.capability("OrderedByKey/receipt", strconv.FormatBool(group.effective.OrderedByKey), "denied", "the effective declaration is denied; the port has no ordering request to probe")
			return
		}
		producer := newProducer(t, group, profileDestination(group, name), driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumer(t, group, profileDestination(group, name), 3)
		for i := range 3 {
			if err := producer.Publish(group.ctx, driver.OutboundMessage{
				Destination: name, Key: []byte("same"), Body: fmt.Appendf(nil, "order-%d", i),
			}); err != nil {
				t.Fatal(err)
			}
		}
		for i := range 3 {
			message := receiveMessage(t, group, consumer)
			if string(message.Body) != fmt.Sprintf("order-%d", i) {
				t.Fatalf("body=%q at position %d, want order-%d", message.Body, i, i)
			}
			ackMessage(t, group, message)
		}
		group.capability("OrderedByKey/receipt", strconv.FormatBool(group.effective.OrderedByKey), capabilityBoolStatus(group.effective.OrderedByKey), "three adjacent publishes with one non-nil key arrived in order")
	})

	group.Check("priority hints remain deliverable without scheduling delegation", func(t *testing.T) {
		name := "capability.priority"
		producer := newProducer(t, group, profileDestination(group, name), driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumer(t, group, profileDestination(group, name), 2)
		messages := []driver.OutboundMessage{
			{Destination: name, Priority: 1, Body: []byte("low")},
			{Destination: name, Priority: 200, Body: []byte("high")},
		}
		for _, message := range messages {
			if err := producer.Publish(group.ctx, message); err != nil {
				t.Fatal(err)
			}
		}
		seen := make(map[string]bool, len(messages))
		for range messages {
			message := receiveMessage(t, group, consumer)
			seen[string(message.Body)] = true
			ackMessage(t, group, message)
		}
		if len(seen) != len(messages) || !seen["low"] || !seen["high"] {
			t.Fatalf("priority messages delivered=%v, want both bodies", seen)
		}
		group.capability("NativePriority/delivery", group.effective.NativePriority.String(), capabilityPriorityStatus(group.effective.NativePriority), "priority hints were delivered without asserting broker scheduling")
	})

	group.Check("delayed delivery works with native and portable paths", func(t *testing.T) {
		name := "capability.delay"
		producer := newDeferredProducer(t, group, profileDestination(group, name), deferredDelay)
		consumer := deferredConsumer(t, group, []string{name}, map[string]time.Duration{name: deferredDelay}, 1)
		due := deferredNow(group).Add(deferredDelay)
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: name, Body: []byte("delayed")}); err != nil {
			t.Fatal(err)
		}
		assertNoDeliveryBefore(t, group, consumer, due, "delayed capability message before due time")
		advanceDeferredTo(group, due)
		message := receiveBefore(t, group, consumer, due.Add(deferredLateBound), "delayed capability message")
		if string(message.Body) != "delayed" || message.ReceivedAt.Before(due) {
			t.Fatalf("delivery body=%q at %s, want delayed at or after %s", message.Body, message.ReceivedAt, due)
		}
		ackMessage(t, group, message)
		group.capability("NativeDelay/delivery", strconv.FormatBool(group.effective.NativeDelay), capabilityBoolStatus(group.effective.NativeDelay), "the destination delay was honored with the declared native or portable path")
	})

	group.Check("delivery count reports first delivery and redelivery distinctly", func(t *testing.T) {
		name := "capability.delivery-count"
		producer := newProducer(t, group, profileDestination(group, name), driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumer(t, group, profileDestination(group, name), 1)
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: name, Body: []byte("count")}); err != nil {
			t.Fatal(err)
		}
		first := receiveMessage(t, group, consumer)
		if group.effective.NativeDeliveryCount {
			if first.DeliveryCount != 0 {
				t.Fatalf("first DeliveryCount=%d, want 0", first.DeliveryCount)
			}
		} else if first.DeliveryCount != -1 {
			t.Fatalf("first DeliveryCount=%d, want -1", first.DeliveryCount)
		}
		if err := first.Settle.Nack(group.ctx, driver.NackOptions{Requeue: true}); err != nil {
			t.Fatal(err)
		}
		second := receiveMessage(t, group, consumer)
		if group.effective.NativeDeliveryCount {
			if second.DeliveryCount <= first.DeliveryCount {
				t.Fatalf("redelivery DeliveryCount=%d, first=%d", second.DeliveryCount, first.DeliveryCount)
			}
		} else if second.DeliveryCount != -1 {
			t.Fatalf("redelivery DeliveryCount=%d, want -1", second.DeliveryCount)
		}
		ackMessage(t, group, second)
		group.capability("NativeDeliveryCount/redelivery", strconv.FormatBool(group.effective.NativeDeliveryCount), capabilityBoolStatus(group.effective.NativeDeliveryCount), "first delivery reports zero redeliveries and a requeue advances the count when available")
	})

	group.Check("zero-requeue settlement does not redeliver to the source", func(t *testing.T) {
		name := "capability.dlq"
		producer := newProducer(t, group, profileDestination(group, name), driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumer(t, group, profileDestination(group, name), 1)
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: name, Body: []byte("discard")}); err != nil {
			t.Fatal(err)
		}
		message := receiveMessage(t, group, consumer)
		if err := message.Settle.Nack(group.ctx, driver.NackOptions{}); err != nil {
			t.Fatal(err)
		}
		assertNoDelivery(t, group, consumer, "discarded capability message")
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: name, Body: []byte("control")}); err != nil {
			t.Fatal(err)
		}
		control := receiveMessage(t, group, consumer)
		if string(control.Body) != "control" {
			t.Fatalf("control body=%q, want control", control.Body)
		}
		ackMessage(t, group, control)
		group.capability("NackRequeueFalse/settlement", "n/a", "verified", "zero-value Nack does not redeliver the failed body and the consumer remains live")
	})

	group.Check("consumer scaling gives each attached consumer work", func(t *testing.T) {
		name := "capability.scaling"
		producer := newProducer(t, group, profileDestination(group, name), driver.ProducerConfig{Effective: group.effective})
		if group.effective.ConsumerScaling == driver.ScalingPartitionBound {
			consumer := newConsumer(t, group, profileDestination(group, name), 1)
			if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: name, Body: []byte("partition-bound")}); err != nil {
				t.Fatal(err)
			}
			message := receiveMessage(t, group, consumer)
			if string(message.Body) != "partition-bound" {
				t.Fatalf("body=%q, want partition-bound", message.Body)
			}
			ackMessage(t, group, message)
			group.capability("ConsumerScaling/delivery", group.effective.ConsumerScaling.String(), "claimed", "the declared partition-bound model delivered through one consumer")
			return
		}
		first := newConsumerFor(t, group, driver.ConsumerConfig{Destinations: []string{name}, Prefetch: 1, Effective: group.effective})
		second := newConsumerFor(t, group, driver.ConsumerConfig{Destinations: []string{name}, Prefetch: 1, Effective: group.effective})
		for _, body := range []string{"first", "second"} {
			if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: name, Body: []byte(body)}); err != nil {
				t.Fatal(err)
			}
		}
		one := receiveMessage(t, group, first)
		two := receiveMessage(t, group, second)
		if string(one.Body) == string(two.Body) {
			t.Fatalf("both consumers received %q", one.Body)
		}
		ackMessage(t, group, one)
		ackMessage(t, group, two)
		group.capability("ConsumerScaling/delivery", group.effective.ConsumerScaling.String(), "claimed", "two attached consumers each received one distinct message")
	})

	group.Check("lag query follows the effective declaration", func(t *testing.T) {
		name := "capability.lag"
		producer := newProducer(t, group, profileDestination(group, name), driver.ProducerConfig{Effective: group.effective})
		consumer := newConsumer(t, group, profileDestination(group, name), 2)
		if err := consumer.Drain(group.ctx); err != nil {
			t.Fatal(err)
		}
		if err := producer.Publish(group.ctx,
			driver.OutboundMessage{Destination: name, Body: []byte("one")},
			driver.OutboundMessage{Destination: name, Body: []byte("two")},
		); err != nil {
			t.Fatal(err)
		}
		lag, err := consumer.Lag(group.ctx)
		if group.effective.LagQueryable {
			if err != nil {
				t.Fatal(err)
			}
			if lag[name] != 2 || len(lag) != 1 {
				t.Fatalf("Lag=%v, want only %q=2", lag, name)
			}
		} else {
			kind, classified := driver.Classify(err)
			if !errors.Is(err, driver.ErrUnsupported) || !classified || kind != driver.KindFatal {
				t.Fatalf("Lag error=%v kind=%v classified=%t", err, kind, classified)
			}
		}
		group.capability("LagQueryable/query", strconv.FormatBool(group.effective.LagQueryable), capabilityBoolStatus(group.effective.LagQueryable), "Lag either reports the drained backlog or returns classified unsupported")
	})

	group.Check("declared message limit matches the accepted boundary", func(t *testing.T) {
		limit := group.effective.MaxMessageBytes
		if limit <= 0 {
			group.capability("MaxMessageBytes/probe", strconv.Itoa(limit), "not-declared", "driver declares no message limit to probe")
			return
		}
		name := "capability.max-message"
		producer := newProducer(t, group, profileDestination(group, name), driver.ProducerConfig{Effective: group.effective})
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: name, Body: bytes.Repeat([]byte{'x'}, limit)}); err != nil {
			t.Fatalf("exact message limit rejected: %v", err)
		}
		err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: name, Body: bytes.Repeat([]byte{'x'}, limit+1)})
		kind, classified := driver.Classify(err)
		if err == nil || !classified || kind != driver.KindTooLarge {
			t.Fatalf("over-limit publish error=%v kind=%v classified=%t", err, kind, classified)
		}
		group.capability("MaxMessageBytes/probe", strconv.Itoa(limit), "verified", fmt.Sprintf("%d bytes accepted and %d bytes classified too large", limit, limit+1))
	})

	group.Check("declared header limit counts keys and values", func(t *testing.T) {
		limit := group.effective.MaxHeaderBytes
		if limit <= 0 {
			group.capability("MaxHeaderBytes/probe", strconv.Itoa(limit), "not-declared", "driver declares no header limit to probe")
			return
		}
		name := "capability.max-header"
		producer := newProducer(t, group, profileDestination(group, name), driver.ProducerConfig{Effective: group.effective})
		key := "k"
		exact := driver.Header{Key: key, Value: bytes.Repeat([]byte{'v'}, limit-len(key))}
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: name, Headers: []driver.Header{exact}}); err != nil {
			t.Fatalf("exact header limit rejected: %v", err)
		}
		over := driver.Header{Key: key, Value: bytes.Repeat([]byte{'v'}, limit-len(key)+1)}
		err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: name, Headers: []driver.Header{over}})
		kind, classified := driver.Classify(err)
		if err == nil || !classified || kind != driver.KindTooLarge {
			t.Fatalf("over-limit header publish error=%v kind=%v classified=%t", err, kind, classified)
		}
		group.capability("MaxHeaderBytes/probe", strconv.Itoa(limit), "verified", fmt.Sprintf("header key+value total %d accepted and %d classified too large", limit, limit+1))
	})

	group.Check("priority level declaration is self-consistent", func(t *testing.T) {
		levels := group.effective.NativePriorityLevels
		mode := group.effective.NativePriority
		if levels < 0 || levels > 256 {
			t.Fatalf("NativePriorityLevels=%d, want uint8-compatible range", levels)
		}
		if mode == driver.PriorityNone && levels != 0 {
			t.Fatalf("NativePriority=%s with NativePriorityLevels=%d, want zero levels", mode, levels)
		}
		if mode != driver.PriorityNone && levels == 0 {
			t.Fatalf("NativePriority=%s with NativePriorityLevels=0, want positive levels", mode)
		}
		status := "denied"
		if mode != driver.PriorityNone {
			status = "claimed"
		}
		group.capability("NativePriorityLevels/probe", strconv.Itoa(levels), status, fmt.Sprintf("mode=%s; levels are declaration-only", mode))
	})

	group.Check("competing exclusive consumer is refused", func(t *testing.T) {
		name := profileDestination(group, "capability.exclusive")
		if _, err := group.conn.Admin().EnsureTopology(group.ctx, profileTopologySpec(group, driver.TopologySpec{
			Destinations: []driver.DestinationSpec{{Name: name}}, Effective: group.effective,
		})); err != nil {
			t.Fatal(err)
		}
		first, err := group.conn.Consumer(group.ctx, driver.ConsumerConfig{Destinations: []string{name}, Prefetch: 1, Exclusive: true, Effective: group.effective})
		if err != nil {
			t.Fatalf("first exclusive consumer: %v", err)
		}
		t.Cleanup(func() {
			if err := first.Stop(group.ctx); err != nil {
				t.Errorf("stop first exclusive consumer: %v", err)
			}
			if err := purgeIfSupported(group.ctx, group.conn, name); err != nil {
				t.Errorf("purge exclusive destination: %v", err)
			}
		})
		_, err = group.conn.Consumer(group.ctx, driver.ConsumerConfig{Destinations: []string{name}, Prefetch: 1, Exclusive: true, Effective: group.effective})
		kind, classified := driver.Classify(err)
		if err == nil || !classified || kind != driver.KindFatal || !strings.Contains(strings.ToLower(err.Error()), "exclusive") {
			t.Fatalf("competing exclusive consumer error=%v kind=%v classified=%t", err, kind, classified)
		}
		group.capability("Exclusive/competition", "n/a", "verified", "a second exclusive consumer is refused with a classified fatal error")
	})

	group.Check("capability report includes every declared field", func(t *testing.T) {
		want := map[string]struct{}{
			"PerMessageAck": {}, "OrderedByKey": {}, "Fanout": {}, "NativePriority": {},
			"NativePriorityLevels": {}, "NativeDelay": {}, "NativeDeliveryCount": {},
			"NativeDLQ": {}, "ConsumerScaling": {}, "MaxMessageBytes": {},
			"MaxHeaderBytes": {}, "LagQueryable": {},
			"PerMessageAck/independent": {}, "OrderedByKey/receipt": {},
			"NativePriority/delivery": {}, "NativeDelay/delivery": {},
			"Fanout/delivery":                {},
			"NativeDeliveryCount/redelivery": {}, "NackRequeueFalse/settlement": {},
			"ConsumerScaling/delivery": {}, "LagQueryable/query": {},
			"MaxMessageBytes/probe": {}, "MaxHeaderBytes/probe": {},
			"NativePriorityLevels/probe": {}, "Exclusive/competition": {},
		}
		seen := make(map[string]struct{}, len(want))
		for _, result := range group.report.Capabilities {
			if result.Profile != group.profile {
				continue
			}
			if _, duplicate := seen[result.Capability]; duplicate {
				t.Fatalf("capability report duplicated %q for %s", result.Capability, group.profile)
			}
			seen[result.Capability] = struct{}{}
			if _, expected := want[result.Capability]; !expected {
				t.Fatalf("capability report has unexpected %q for %s", result.Capability, group.profile)
			}
			delete(want, result.Capability)
		}
		if len(want) != 0 {
			t.Fatalf("capability report missing declarations: %v", want)
		}
		group.capability("Report/completeness", strconv.Itoa(len(seen)), "verified", "the exact profile-scoped capability evidence set is present")
	})

	group.Check("capability results stay outside the behavior vector", func(t *testing.T) {
		if len(group.vector) != 0 {
			t.Fatalf("capability group recorded %d behavior events", len(group.vector))
		}
		group.capability("Report/vector", "empty", "verified", "capability observations are report-only")
	})
}

func capabilityDeclarationChecks(group *groupContext) {
	checkCapabilityDeclaration(group, "declares per-message acknowledgement policy", "PerMessageAck", func(c driver.Capabilities) string { return strconv.FormatBool(c.PerMessageAck) })
	checkCapabilityDeclaration(group, "declares ordered-by-key policy", "OrderedByKey", func(c driver.Capabilities) string { return strconv.FormatBool(c.OrderedByKey) })
	checkFanoutDeclaration(group)
	checkCapabilityDeclaration(group, "declares native priority mode", "NativePriority", func(c driver.Capabilities) string { return c.NativePriority.String() })
	checkCapabilityDeclaration(group, "declares native priority levels", "NativePriorityLevels", func(c driver.Capabilities) string { return strconv.Itoa(c.NativePriorityLevels) })
	checkCapabilityDeclaration(group, "declares native delay policy", "NativeDelay", func(c driver.Capabilities) string { return strconv.FormatBool(c.NativeDelay) })
	checkCapabilityDeclaration(group, "declares native delivery count policy", "NativeDeliveryCount", func(c driver.Capabilities) string { return strconv.FormatBool(c.NativeDeliveryCount) })
	checkCapabilityDeclaration(group, "declares native dead-letter policy", "NativeDLQ", func(c driver.Capabilities) string { return strconv.FormatBool(c.NativeDLQ) })
	checkCapabilityDeclaration(group, "declares consumer scaling model", "ConsumerScaling", func(c driver.Capabilities) string { return c.ConsumerScaling.String() })
	checkCapabilityDeclaration(group, "declares maximum message bytes", "MaxMessageBytes", func(c driver.Capabilities) string { return strconv.Itoa(c.MaxMessageBytes) })
	checkCapabilityDeclaration(group, "declares maximum header bytes", "MaxHeaderBytes", func(c driver.Capabilities) string { return strconv.Itoa(c.MaxHeaderBytes) })
	checkCapabilityDeclaration(group, "declares lag query policy", "LagQueryable", func(c driver.Capabilities) string { return strconv.FormatBool(c.LagQueryable) })
}

func checkFanoutDeclaration(group *groupContext) {
	group.Check("declares fanout mode", func(t *testing.T) {
		want := strconv.Itoa(int(effectiveCapabilities(group.conn.Capabilities(), group.profile).Fanout))
		got := strconv.Itoa(int(group.effective.Fanout))
		if got != want {
			t.Fatalf("Fanout declaration=%q, want effective=%q", got, want)
		}
		group.capability("Fanout", got, "declared", "effective declaration reached the driver configuration")
		if group.effective.Fanout != driver.FanoutAtPublish {
			group.capability("Fanout/delivery", got, "denied", "the driver does not claim publish-time fanout")
			return
		}

		exchange := "capability.fanout.exchange"
		firstDestination := "capability.fanout.first"
		secondDestination := "capability.fanout.second"
		if _, err := group.conn.Admin().EnsureTopology(group.ctx, profileTopologySpec(group, driver.TopologySpec{
			Exchanges: []driver.ExchangeSpec{{Name: exchange, Kind: "fanout", Durable: true}},
			Destinations: []driver.DestinationSpec{
				{Name: firstDestination, Durable: true},
				{Name: secondDestination, Durable: true},
			},
			Bindings: []driver.BindingSpec{
				{Source: exchange, Destination: firstDestination},
				{Source: exchange, Destination: secondDestination},
			},
			Effective: group.effective,
		})); err != nil {
			t.Fatalf("EnsureTopology fanout probe: %v", err)
		}
		producer, err := group.conn.Producer(group.ctx, driver.ProducerConfig{
			Effective: group.effective,
		})
		if err != nil {
			t.Fatalf("Producer fanout probe: %v", err)
		}
		producer = &profileProducer{group: group, producer: producer, scoped: true}
		t.Cleanup(func() {
			if err := producer.Close(group.ctx); err != nil {
				t.Errorf("close fanout probe producer: %v", err)
			}
		})
		t.Cleanup(func() {
			for _, destination := range []string{firstDestination, secondDestination} {
				if err := purgeIfSupported(group.ctx, group.conn, profileDestination(group, destination)); err != nil {
					t.Errorf("purge fanout probe destination %q: %v", destination, err)
				}
			}
		})
		first := newConsumerFor(t, group, driver.ConsumerConfig{
			Destinations: []string{firstDestination}, Prefetch: 1, Effective: group.effective,
		})
		second := newConsumerFor(t, group, driver.ConsumerConfig{
			Destinations: []string{secondDestination}, Prefetch: 1, Effective: group.effective,
		})
		if err := producer.Publish(group.ctx, driver.OutboundMessage{
			Destination: exchange, EntryPoint: true, Body: []byte("fanout-copy"),
		}); err != nil {
			t.Fatalf("fanout publish: %v", err)
		}
		firstMessage := receiveMessage(t, group, first)
		secondMessage := receiveMessage(t, group, second)
		for name, message := range map[string]driver.InboundMessage{
			"first": firstMessage, "second": secondMessage,
		} {
			if string(message.Body) != "fanout-copy" {
				t.Fatalf("%s fanout body=%q, want fanout-copy", name, message.Body)
			}
			ackMessage(t, group, message)
		}
		group.capability("Fanout/delivery", got, "claimed", "one publish reached both independent subscription destinations")
	})
}

func checkCapabilityDeclaration(group *groupContext, name, field string, read func(driver.Capabilities) string) {
	group.Check(name, func(t *testing.T) {
		want := read(effectiveCapabilities(group.conn.Capabilities(), group.profile))
		got := read(group.effective)
		if got != want {
			t.Fatalf("%s declaration=%q, want effective=%q", field, got, want)
		}
		group.capability(field, got, "declared", "effective declaration reached the driver configuration")
	})
}

func capabilityBoolStatus(value bool) string {
	if value {
		return "claimed"
	}
	return "denied"
}

func capabilityPriorityStatus(mode driver.PriorityMode) string {
	if mode == driver.PriorityNone {
		return "denied"
	}
	return "claimed"
}
