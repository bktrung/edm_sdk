package f1

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

func TestPublishTopicsOptionRejectsEmptyAndDuplicate(t *testing.T) {
	for name, option := range map[string]Option{
		"empty":     WithPublishTopics("orders.created", ""),
		"duplicate": WithPublishTopics("orders.created", "orders.created"),
	} {
		t.Run(name, func(t *testing.T) {
			options := clientOptions{}
			err := option(&options)
			require.Error(t, err)
			require.Contains(t, err.Error(), "WithPublishTopics")
		})
	}

	options := clientOptions{}
	require.NoError(t, WithPublishTopics("orders.created")(&options))
	require.Error(t, WithPublishTopics("orders.created")(&options))
	options = clientOptions{}
	require.NoError(t, WithPublishTopics("orders.created.v1")(&options))
	require.Error(t, WithPublishTopics("orders.created")(&options))
}

func TestVersionedPublishTopicUsesCanonicalEntryPoint(t *testing.T) {
	producer := &recordingProducer{}
	admin := &topologyRecordingAdmin{}
	conn := &topologyTestConn{
		testConn: &testConn{caps: driver.Capabilities{MaxHeaderBytes: CoreMaxHeaderBytes}},
		admin:    admin,
		producer: producer,
	}
	cfg := testClientConfig(t)
	cfg.Topology.Priorities = []Priority{PriorityNormal}
	client, err := New(context.Background(), cfg,
		WithDriver(&topologyTestDriver{conn: conn}),
		WithPublishTopics("orders.created.v1"),
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, client.Close(context.Background())) }()

	require.Len(t, admin.specs, 1)
	require.Equal(t, []string{"f1.test.orders.created.normal"}, destinationNames(admin.specs[0].Destinations))
	_, err = client.Publisher().Publish(context.Background(), "orders.created.v1", map[string]string{"id": "o1"})
	require.NoError(t, err)
	producer.mu.Lock()
	require.Len(t, producer.messages, 1)
	require.Equal(t, "f1.test.orders.created.normal", producer.messages[0].Destination)
	producer.mu.Unlock()
}

func TestPublisherTopologyHasOnlyEntryPoints(t *testing.T) {
	priorities := []Priority{PriorityHigh, PriorityNormal}
	wantNames := []string{"f1.test.orders.created.high", "f1.test.orders.created.normal"}

	for name, caps := range map[string]driver.Capabilities{
		"publish": {Fanout: driver.FanoutAtPublish, MaxHeaderBytes: CoreMaxHeaderBytes},
		"consume": {Fanout: driver.FanoutAtConsume, MaxHeaderBytes: CoreMaxHeaderBytes},
	} {
		t.Run(name, func(t *testing.T) {
			admin := &topologyRecordingAdmin{}
			conn := &topologyTestConn{testConn: &testConn{caps: caps}, admin: admin}
			cfg := testClientConfig(t)
			cfg.Topology.Priorities = priorities
			client, err := New(context.Background(), cfg,
				WithDriver(&topologyTestDriver{conn: conn}),
				WithPublishTopics("orders.created"),
			)
			require.NoError(t, err)
			require.NoError(t, client.Close(context.Background()))
			require.Len(t, admin.specs, 1)
			spec := admin.specs[0]
			require.Equal(t, TopologyDeclare, spec.Policy)
			require.Empty(t, spec.Bindings)
			require.Empty(t, spec.Scope)
			if caps.Fanout == driver.FanoutAtPublish {
				require.Equal(t, wantNames, exchangeNames(spec.Exchanges))
				require.Empty(t, spec.Destinations)
				for _, exchange := range spec.Exchanges {
					require.Equal(t, "fanout", exchange.Kind)
					require.True(t, exchange.Durable)
				}
			} else {
				require.Empty(t, spec.Exchanges)
				require.Equal(t, wantNames, destinationNames(spec.Destinations))
				for _, destination := range spec.Destinations {
					require.Equal(t, driver.DestMain, destination.Kind)
					require.True(t, destination.Durable)
				}
			}
		})
	}
}

func TestPublishRejectsTopicAbsentFromList(t *testing.T) {
	producer := &recordingProducer{}
	admin := &topologyRecordingAdmin{}
	conn := &topologyTestConn{
		testConn: &testConn{caps: driver.Capabilities{MaxHeaderBytes: CoreMaxHeaderBytes}},
		admin:    admin,
		producer: producer,
	}
	client, err := New(context.Background(), testClientConfig(t),
		WithDriver(&topologyTestDriver{conn: conn}),
		WithPublishTopics("orders.created"),
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, client.Close(context.Background())) }()

	_, err = client.Publisher().Publish(context.Background(), "payments.created.v1", map[string]string{"id": "p1"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "payments.created")
	require.Contains(t, err.Error(), "WithPublishTopics")
	producer.mu.Lock()
	require.Empty(t, producer.messages)
	producer.mu.Unlock()
}

func TestSubscriberTopologyPolicyControlsAdminCalls(t *testing.T) {
	for name, policy := range map[string]TopologyPolicy{
		"declare": TopologyDeclare,
		"none":    TopologyNone,
	} {
		t.Run(name, func(t *testing.T) {
			admin := &topologyRecordingAdmin{}
			consumer := newDispatchConsumer()
			conn := &topologyTestConn{
				testConn: &testConn{caps: driver.Capabilities{MaxHeaderBytes: CoreMaxHeaderBytes}},
				admin:    admin,
				consumer: consumer,
			}
			client, err := New(context.Background(), testClientConfig(t),
				WithDriver(&topologyTestDriver{conn: conn}),
				WithTopology(policy),
			)
			require.NoError(t, err)
			runner := &Runner{
				client: client,
				subscription: Subscription{
					Name:       "orders-worker",
					Topics:     []string{"orders.created"},
					Priorities: []Priority{PriorityNormal},
					Retry:      RetryConfig{MaxAttempts: 1},
				},
				config: SubscriptionConfig{Prefetch: 1},
			}
			_, err = openRunnerConsumer(runner, context.Background())
			require.NoError(t, err)
			require.NoError(t, client.Close(context.Background()))
			if policy == TopologyNone {
				require.Empty(t, admin.specs)
			} else {
				require.Len(t, admin.specs, 1)
			}
		})
	}
}

type topologyTestDriver struct {
	conn *topologyTestConn
}

func (*topologyTestDriver) Name() string { return "topology-test" }

func (d *topologyTestDriver) Capabilities() driver.Capabilities { return d.conn.caps }

func (d *topologyTestDriver) Open(context.Context, driver.Config) (driver.Conn, error) {
	return d.conn, nil
}

type topologyTestConn struct {
	*testConn
	admin          driver.Admin
	producer       driver.Producer
	consumer       driver.Consumer
	consumerOpened chan struct{}
}

func (c *topologyTestConn) Admin() driver.Admin { return c.admin }

func (c *topologyTestConn) Producer(context.Context, driver.ProducerConfig) (driver.Producer, error) {
	if c.producer == nil {
		return nil, errors.New("test producer is not configured")
	}
	return c.producer, nil
}

func (c *topologyTestConn) Consumer(context.Context, driver.ConsumerConfig) (driver.Consumer, error) {
	if c.consumerOpened != nil {
		select {
		case c.consumerOpened <- struct{}{}:
		default:
		}
	}
	if c.consumer == nil {
		return nil, errors.New("test consumer is not configured")
	}
	return c.consumer, nil
}

type topologyRecordingAdmin struct {
	mu     sync.Mutex
	specs  []driver.TopologySpec
	called chan struct{}
}

func (a *topologyRecordingAdmin) EnsureTopology(_ context.Context, spec driver.TopologySpec) (driver.TopologyDiff, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.specs = append(a.specs, spec)
	if a.called != nil {
		select {
		case a.called <- struct{}{}:
		default:
		}
	}
	return driver.TopologyDiff{}, nil
}

func (*topologyRecordingAdmin) DescribeTopology(context.Context, []string) (driver.TopologyState, error) {
	return driver.TopologyState{}, driver.ErrUnsupported
}

func (*topologyRecordingAdmin) Purge(context.Context, string) (int64, error) {
	return 0, driver.ErrUnsupported
}

func (*topologyRecordingAdmin) Prune(context.Context, []string) ([]driver.PruneResult, error) {
	return nil, driver.ErrUnsupported
}

func (a *topologyRecordingAdmin) snapshot() []driver.TopologySpec {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]driver.TopologySpec(nil), a.specs...)
}
