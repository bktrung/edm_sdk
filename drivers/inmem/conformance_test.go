package inmem

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver/conformance"
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
	report := conformance.Run(t, conformance.Suite{
		Driver:       Driver{},
		Config:       driver.Config{},
		NewInspector: newInspector,
	})
	if err := report.WriteMarkdown(&output); err != nil {
		t.Fatal(err)
	}
	t.Log(output.String())
}

type countingDriver struct {
	Driver
	opens *int
}

func (d *countingDriver) Open(ctx context.Context, cfg driver.Config) (driver.Conn, error) {
	(*d.opens)++
	return d.Driver.Open(ctx, cfg)
}

func TestRunUsesOneConnectionAndInspector(t *testing.T) {
	opens := 0
	inspectors := 0
	driverUnderTest := &countingDriver{Driver: Driver{}, opens: &opens}
	report := conformance.Run(t, conformance.Suite{
		Driver: driverUnderTest,
		Config: driver.Config{},
		NewInspector: func(conn driver.Conn) (conformance.Inspect, error) {
			inspectors++
			return newInspector(conn)
		},
	})
	if opens != 1 {
		t.Fatalf("Run opened %d connections; want 1", opens)
	}
	if inspectors != 1 {
		t.Fatalf("Run built %d inspectors; want 1", inspectors)
	}
	if len(report.Profiles) != 2 {
		t.Fatalf("Run returned %d profile reports; want 2", len(report.Profiles))
	}
}
