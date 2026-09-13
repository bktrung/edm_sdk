package f1

import (
	"context"
	"testing"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

func TestOpenRunnerConsumerPropagatesOrderingAsExclusive(t *testing.T) {
	for _, test := range []struct {
		name string
		mode Mode
		want bool
	}{
		{name: "ordered", mode: OrderedByKey, want: true},
		{name: "unordered", mode: Unordered, want: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			conn := &orderingRecordingConn{
				testConn: &testConn{
					caps: driver.Capabilities{OrderedByKey: true},
					info: testBrokerInfo(),
				},
			}
			client, err := New(context.Background(), testClientConfig(t),
				WithDriver(&orderingRecordingDriver{conn: conn}),
				WithTopology(TopologyNone),
			)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = client.Close(context.Background()) })

			runner, err := client.Subscribe(context.Background(), Subscription{
				Name:        "orders",
				Topics:      []string{"orders.created"},
				Mode:        test.mode,
				Concurrency: 2,
			})
			if err != nil {
				t.Fatal(err)
			}
			consumer, err := openRunnerConsumer(runner, context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if err := consumer.Stop(context.Background()); err != nil {
				t.Fatal(err)
			}
			if len(conn.consumerConfigs) != 1 {
				t.Fatalf("Consumer() calls = %d, want 1", len(conn.consumerConfigs))
			}
			if got := conn.consumerConfigs[0].Exclusive; got != test.want {
				t.Fatalf("ConsumerConfig.Exclusive = %t, want %t", got, test.want)
			}
		})
	}
}

type orderingRecordingDriver struct {
	conn *orderingRecordingConn
}

func (*orderingRecordingDriver) Name() string { return "ordering-recording" }

func (d *orderingRecordingDriver) Capabilities() driver.Capabilities {
	return d.conn.caps
}

func (d *orderingRecordingDriver) Open(context.Context, driver.Config) (driver.Conn, error) {
	return d.conn, nil
}

type orderingRecordingConn struct {
	*testConn
	consumerConfigs []driver.ConsumerConfig
}

func (c *orderingRecordingConn) Consumer(_ context.Context, cfg driver.ConsumerConfig) (driver.Consumer, error) {
	c.consumerConfigs = append(c.consumerConfigs, cfg)
	return &orderingTestConsumer{}, nil
}

// Admin gives this fake the admin surface a subscription requires under every
// policy. Under TopologyNone the call is not a topology call: it is how a retry
// tier's delay reaches the consumer, so the core makes it rather than skipping
// the policy, and a nil admin is a missing surface we need.
func (c *orderingRecordingConn) Admin() driver.Admin { return &dispatchAdmin{} }

type orderingTestConsumer struct{}

func (*orderingTestConsumer) Messages() <-chan driver.InboundMessage { return nil }
func (*orderingTestConsumer) Errors() <-chan error                   { return nil }
func (*orderingTestConsumer) Pause(...string) error                  { return nil }
func (*orderingTestConsumer) Resume(...string) error                 { return nil }
func (*orderingTestConsumer) Drain(context.Context) error            { return nil }
func (*orderingTestConsumer) Stop(context.Context) error             { return nil }
func (*orderingTestConsumer) Release(context.Context) error          { return nil }
func (*orderingTestConsumer) Lag(context.Context) (map[string]int64, error) {
	return nil, driver.ErrUnsupported
}
