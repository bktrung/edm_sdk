package f1

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

func TestTopologyPolicyDerivesFromOptionAndConfig(t *testing.T) {
	cases := []struct {
		name          string
		autoCreate    bool
		verifyOnStart bool
		option        TopologyPolicy
		supplied      bool
		want          TopologyPolicy
	}{
		{"option wins", true, true, TopologyNone, true, TopologyNone},
		{"auto create", true, true, TopologyDeclare, false, TopologyDeclare},
		{"verify", false, true, TopologyDeclare, false, TopologyVerify},
		{"none", false, false, TopologyDeclare, false, TopologyNone},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			cfg := testClientConfig(t)
			cfg.Topology.AutoCreate = test.autoCreate
			cfg.Topology.VerifyOnStart = test.verifyOnStart
			testDriver := &topologyTestDriver{conn: &topologyTestConn{
				testConn: &testConn{caps: driver.Capabilities{MaxHeaderBytes: CoreMaxHeaderBytes}},
				admin:    &topologyRecordingAdmin{},
			}}
			opts := []Option{WithDriver(testDriver)}
			if test.supplied {
				opts = append(opts, WithTopology(test.option))
			}
			client, err := New(context.Background(), cfg, opts...)
			require.NoError(t, err)
			require.Equal(t, test.want, client.topologyPolicy())
			require.NoError(t, client.Close(context.Background()))
		})
	}
}

func TestPublisherTopologyPolicyUsesDerivedConfig(t *testing.T) {
	cases := []struct {
		name          string
		autoCreate    bool
		verifyOnStart bool
		wantSpecs     int
		wantPolicy    TopologyPolicy
	}{
		{"none", false, false, 0, TopologyNone},
		{"verify", false, true, 1, TopologyVerify},
		{"declare", true, false, 1, TopologyDeclare},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			admin := &topologyRecordingAdmin{}
			conn := &topologyTestConn{
				testConn: &testConn{caps: driver.Capabilities{MaxHeaderBytes: CoreMaxHeaderBytes}},
				admin:    admin,
			}
			cfg := testClientConfig(t)
			cfg.Topology.AutoCreate = test.autoCreate
			cfg.Topology.VerifyOnStart = test.verifyOnStart
			client, err := New(context.Background(), cfg,
				WithDriver(&topologyTestDriver{conn: conn}),
				WithPublishTopics("orders.created"),
			)
			require.NoError(t, err)
			require.NoError(t, client.Close(context.Background()))

			specs := admin.snapshot()
			require.Len(t, specs, test.wantSpecs)
			if test.wantSpecs != 0 {
				require.Equal(t, test.wantPolicy, specs[0].Policy)
			}
		})
	}
}

func TestConsumerTopologyPolicyUsesDerivedConfig(t *testing.T) {
	cases := []struct {
		name          string
		autoCreate    bool
		verifyOnStart bool
		wantSpecs     int
		wantPolicy    TopologyPolicy
	}{
		// The consumer path ensures topology under every policy. Under
		// TopologyNone the call still arrives: it does no broker work, but it
		// is how a retry tier's delay reaches the consumer, which the publisher
		// table above has no use for and where the row stays at 0.
		{"none", false, false, 1, TopologyNone},
		{"verify", false, true, 1, TopologyVerify},
		{"declare", true, false, 1, TopologyDeclare},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			admin := &topologyRecordingAdmin{called: make(chan struct{}, 1)}
			conn := &topologyTestConn{
				testConn:       &testConn{caps: driver.Capabilities{MaxHeaderBytes: CoreMaxHeaderBytes}},
				admin:          admin,
				consumer:       newDispatchConsumer(),
				consumerOpened: make(chan struct{}, 1),
			}
			cfg := testClientConfig(t)
			cfg.Topology.AutoCreate = test.autoCreate
			cfg.Topology.VerifyOnStart = test.verifyOnStart
			client, err := New(context.Background(), cfg, WithDriver(&topologyTestDriver{conn: conn}))
			require.NoError(t, err)
			runner, err := client.Subscribe(context.Background(), topologyTestSubscription())
			require.NoError(t, err)

			runCtx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- runner.Run(runCtx) }()
			if test.wantSpecs == 0 {
				select {
				case <-conn.consumerOpened:
				case <-clock.NewReal().Timer(time.Second).C:
					t.Fatal("consumer did not open")
				}
			} else {
				select {
				case <-admin.called:
				case <-clock.NewReal().Timer(time.Second).C:
					t.Fatal("topology was not ensured")
				}
			}
			cancel()
			require.NoError(t, <-done)
			require.NoError(t, client.Close(context.Background()))

			specs := admin.snapshot()
			require.Len(t, specs, test.wantSpecs)
			if test.wantSpecs != 0 {
				require.Equal(t, test.wantPolicy, specs[0].Policy)
			}
		})
	}
}

func TestConsumerTopologyErrorsWithoutAdmin(t *testing.T) {
	cfg := testClientConfig(t) // AutoCreate = true derives TopologyDeclare
	consumer := newDispatchConsumer()
	client, err := New(context.Background(), cfg,
		WithDriver(&dispatchDriver{conn: &dispatchConn{producer: &dispatchProducer{}, consumer: consumer}}))
	require.NoError(t, err)
	defer func() { _ = client.Close(context.Background()) }()

	runner := &Runner{
		client: client,
		subscription: Subscription{
			Name:       "orders",
			Topics:     []string{"orders.created"},
			Priorities: []Priority{PriorityMedium},
			Retry:      RetryConfig{MaxAttempts: 1},
		},
		config: SubscriptionConfig{Prefetch: 1},
	}
	_, err = openRunnerConsumer(runner, context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "requires driver admin")
}

func topologyTestSubscription() Subscription {
	return Subscription{
		Name:           "orders-worker",
		Topics:         []string{"orders.created"},
		Concurrency:    1,
		Prefetch:       1,
		Priorities:     []Priority{PriorityMedium},
		Retry:          RetryConfig{MaxAttempts: 1},
		HandlerTimeout: time.Second,
	}
}
