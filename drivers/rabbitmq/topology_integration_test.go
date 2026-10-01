//go:build integration

package rabbitmq

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

func TestEnsureTopologyDeclaresFanoutAndBinding(t *testing.T) {
	requireBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	const exchange = "rabbitmq-driver-topology-exchange"
	const queue = "rabbitmq-driver-topology-queue"
	raw, err := amqp.Dial(defaultEndpoint)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = raw.Close() })
	rawChannel, err := raw.Channel()
	if err != nil {
		t.Fatalf("Channel: %v", err)
	}
	t.Cleanup(func() { _ = rawChannel.Close() })
	_, _ = rawChannel.QueueDelete(queue, false, false, false)
	_ = rawChannel.ExchangeDelete(exchange, false, false)
	t.Cleanup(func() {
		_, _ = rawChannel.QueueDelete(queue, false, false, false)
		_ = rawChannel.ExchangeDelete(exchange, false, false)
	})

	conn, err := (Driver{}).Open(ctx, driver.Config{Endpoints: []string{defaultEndpoint}})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	spec := driver.TopologySpec{
		Exchanges:    []driver.ExchangeSpec{{Name: exchange, Kind: "fanout", Durable: true}},
		Destinations: []driver.DestinationSpec{{Name: queue, Durable: true}},
		Bindings:     []driver.BindingSpec{{Source: exchange, Destination: queue}},
	}
	wantBinding := driver.BindingSpec{Source: exchange, Destination: queue}
	first, err := conn.Admin().EnsureTopology(ctx, spec)
	if err != nil {
		t.Fatalf("first EnsureTopology: %v", err)
	}
	if !containsString(first.CreatedExchanges, exchange) {
		t.Fatalf("first CreatedExchanges = %v, want %q", first.CreatedExchanges, exchange)
	}
	if !containsString(first.CreatedDestinations, queue) {
		t.Fatalf("first CreatedDestinations = %v, want %q", first.CreatedDestinations, queue)
	}
	if !containsBinding(first.CreatedBindings, wantBinding) {
		t.Fatalf("first CreatedBindings = %v, want binding %q -> %q", first.CreatedBindings, exchange, queue)
	}

	second, err := conn.Admin().EnsureTopology(ctx, spec)
	if err != nil {
		t.Fatalf("second EnsureTopology: %v", err)
	}
	if containsString(second.CreatedExchanges, exchange) || containsString(second.CreatedDestinations, queue) || containsBinding(second.CreatedBindings, wantBinding) {
		t.Fatalf("second diff reports creation: %#v", second)
	}
	if !containsString(second.ExistingExchanges, exchange) || !containsString(second.ExistingDestinations, queue) || !containsBinding(second.ExistingBindings, wantBinding) {
		t.Fatalf("second topology diff = %#v, want exchange, queue, and binding", second)
	}

	deliveries, err := rawChannel.Consume(queue, "topology-test", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if err := rawChannel.PublishWithContext(ctx, exchange, "", false, false, amqp.Publishing{Body: []byte("routed")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	select {
	case delivery := <-deliveries:
		if string(delivery.Body) != "routed" {
			t.Fatalf("routed body = %q, want routed", delivery.Body)
		}
		if err := delivery.Ack(false); err != nil {
			t.Fatalf("Ack: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("waiting for routed message: %v", ctx.Err())
	}
}

func TestVerifyTopologyReportsDeletedBinding(t *testing.T) {
	requireBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	const exchange = "rabbitmq-driver-verify-binding-exchange"
	const queue = "rabbitmq-driver-verify-binding-queue"
	raw, err := amqp.Dial(defaultEndpoint)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = raw.Close() })
	rawChannel, err := raw.Channel()
	if err != nil {
		t.Fatalf("Channel: %v", err)
	}
	t.Cleanup(func() { _ = rawChannel.Close() })
	_, _ = rawChannel.QueueDelete(queue, false, false, false)
	_ = rawChannel.ExchangeDelete(exchange, false, false)
	t.Cleanup(func() {
		_, _ = rawChannel.QueueDelete(queue, false, false, false)
		_ = rawChannel.ExchangeDelete(exchange, false, false)
	})

	conn, err := (Driver{}).Open(ctx, driver.Config{Endpoints: []string{defaultEndpoint}})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(ctx) })

	spec := driver.TopologySpec{
		Exchanges:    []driver.ExchangeSpec{{Name: exchange, Kind: "fanout", Durable: true}},
		Destinations: []driver.DestinationSpec{{Name: queue, Durable: true}},
		Bindings:     []driver.BindingSpec{{Source: exchange, Destination: queue}},
	}
	wantBinding := driver.BindingSpec{Source: exchange, Destination: queue}
	if _, err := conn.Admin().EnsureTopology(ctx, spec); err != nil {
		t.Fatalf("EnsureTopology: %v", err)
	}
	if err := rawChannel.QueueUnbind(queue, "", exchange, nil); err != nil {
		t.Fatalf("QueueUnbind: %v", err)
	}

	spec.Policy = driver.TopologyVerify
	_, err = conn.Admin().EnsureTopology(ctx, spec)
	if err == nil {
		t.Fatal("TopologyVerify() error = nil after binding deletion")
	}
	message := err.Error()
	if !strings.Contains(message, "binding") || !strings.Contains(message, exchange) || !strings.Contains(message, queue) {
		t.Fatalf("TopologyVerify() error = %q, want binding and endpoint names", message)
	}

	spec.Policy = driver.TopologyDeclare
	repaired, err := conn.Admin().EnsureTopology(ctx, spec)
	if err != nil {
		t.Fatalf("EnsureTopology after binding deletion: %v", err)
	}
	if !containsBinding(repaired.CreatedBindings, wantBinding) {
		t.Fatalf("EnsureTopology after binding deletion CreatedBindings = %v, want binding %q -> %q", repaired.CreatedBindings, exchange, queue)
	}
}

func TestVerifyTopologyReportsArgumentDrift(t *testing.T) {
	requireBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	const drifted = "rabbitmq-driver-verify-drift-queue"
	const clean = "rabbitmq-driver-verify-drift-clean-queue"
	raw, err := amqp.Dial(defaultEndpoint)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = raw.Close() })
	rawChannel, err := raw.Channel()
	if err != nil {
		t.Fatalf("Channel: %v", err)
	}
	t.Cleanup(func() { _ = rawChannel.Close() })
	_, _ = rawChannel.QueueDelete(drifted, false, false, false)
	_, _ = rawChannel.QueueDelete(clean, false, false, false)
	t.Cleanup(func() {
		_, _ = rawChannel.QueueDelete(drifted, false, false, false)
		_, _ = rawChannel.QueueDelete(clean, false, false, false)
	})

	// Declare the queue out of band with a delivery limit the spec below does
	// not agree with. QueueDeclarePassive would not catch this: it checks the
	// queue's name only. The management-API-backed drift check is what must
	// catch it.
	if _, err := rawChannel.QueueDeclare(drifted, true, false, false, false, amqp.Table{
		"x-queue-type":     "quorum",
		"x-delivery-limit": int32(3),
	}); err != nil {
		t.Fatalf("out-of-band QueueDeclare(%q): %v", drifted, err)
	}

	conn, err := (Driver{}).Open(ctx, driver.Config{Endpoints: []string{defaultEndpoint}})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(ctx) })

	driftedSpec := driver.TopologySpec{
		Policy:       driver.TopologyVerify,
		Destinations: []driver.DestinationSpec{{Name: drifted, Durable: true, DeliveryLimit: 7}},
	}
	diff, err := conn.Admin().EnsureTopology(ctx, driftedSpec)
	if err != nil {
		t.Fatalf("EnsureTopology(verify) for drifted queue: %v", err)
	}
	if len(diff.Drifted) == 0 {
		t.Fatalf("Drifted = %v, want an entry naming x-delivery-limit", diff.Drifted)
	}
	found := false
	for _, d := range diff.Drifted {
		if d.Name == drifted && d.Argument == "x-delivery-limit" {
			found = true
			if d.Want != "7" || d.Got != "3" {
				t.Fatalf("Drifted entry = %+v, want Want=7 Got=3", d)
			}
		}
	}
	if !found {
		t.Fatalf("Drifted = %v, want an entry for %q naming x-delivery-limit", diff.Drifted, drifted)
	}

	// A queue declared to match the spec must report no drift, so the check
	// is not simply always-fail.
	cleanSpec := driver.TopologySpec{
		Policy:       driver.TopologyDeclare,
		Destinations: []driver.DestinationSpec{{Name: clean, Durable: true, DeliveryLimit: 7}},
	}
	if _, err := conn.Admin().EnsureTopology(ctx, cleanSpec); err != nil {
		t.Fatalf("EnsureTopology(declare) for clean queue: %v", err)
	}
	cleanSpec.Policy = driver.TopologyVerify
	cleanDiff, err := conn.Admin().EnsureTopology(ctx, cleanSpec)
	if err != nil {
		t.Fatalf("EnsureTopology(verify) for clean queue: %v", err)
	}
	if len(cleanDiff.Drifted) != 0 {
		t.Fatalf("Drifted = %v, want empty for a correctly declared queue", cleanDiff.Drifted)
	}
}

// TestEnsureTopologyParkQueueArgumentsReachBroker proves the at-least-once
// dead-letter arguments reach the broker rather than only a Go map, and that
// the parking queue carries no TTL of its own: it reads the queue back through
// the management API, which is the only channel that reports a queue's real
// arguments (AMQP passive declare checks the name only). A queue TTL would cap
// every message's wait and could not follow a changed delay.
func TestEnsureTopologyParkQueueArgumentsReachBroker(t *testing.T) {
	requireBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	const destination = "rabbitmq-driver-park-args-queue"
	const delay = 2 * time.Second
	parkQueue := parkQueueName(destination)
	raw, err := amqp.Dial(defaultEndpoint)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = raw.Close() })
	rawChannel, err := raw.Channel()
	if err != nil {
		t.Fatalf("Channel: %v", err)
	}
	t.Cleanup(func() { _ = rawChannel.Close() })
	deleteParkQueues(rawChannel, destination)
	t.Cleanup(func() { deleteParkQueues(rawChannel, destination) })

	conn, err := (Driver{}).Open(ctx, driver.Config{Endpoints: []string{defaultEndpoint}})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(ctx) })

	if _, err := conn.Admin().EnsureTopology(ctx, driver.TopologySpec{
		Destinations: []driver.DestinationSpec{{Name: destination, Durable: true, Delay: delay}},
	}); err != nil {
		t.Fatalf("EnsureTopology: %v", err)
	}

	mgmt, err := newManagementClient(defaultEndpoint, driver.Config{})
	if err != nil {
		t.Fatalf("newManagementClient: %v", err)
	}
	queue, err := mgmt.getQueue(ctx, parkQueue)
	if err != nil {
		t.Fatalf("getQueue(%q): %v", parkQueue, err)
	}
	parkArguments := map[string]string{
		"x-queue-type":              "quorum",
		"x-dead-letter-exchange":    "",
		"x-dead-letter-routing-key": destination,
		"x-dead-letter-strategy":    "at-least-once",
		"x-overflow":                "reject-publish",
	}
	assertQueueArguments(t, "park", queue.Arguments, parkArguments, nil)
	if value, present := queue.Arguments["x-message-ttl"]; present {
		t.Fatalf("parking queue carries x-message-ttl=%v, want the wait on each message instead", value)
	}
}

// TestVerifyTopologyReportsDriftOnStaleParkQueue is the deployability
// control: it hand-declares a park queue with the pre-fix argument set (no
// x-dead-letter-strategy, no x-overflow) so an upgraded driver's
// TopologyVerify has to catch a queue that was never touched by this
// change. AMQP passive declare cannot see this drift; only the
// management-API-backed comparison can.
func TestVerifyTopologyReportsDriftOnStaleParkQueue(t *testing.T) {
	requireBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	const destination = "rabbitmq-driver-stale-park-queue"
	const delay = time.Second
	park := parkQueueName(destination)
	raw, err := amqp.Dial(defaultEndpoint)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = raw.Close() })
	rawChannel, err := raw.Channel()
	if err != nil {
		t.Fatalf("Channel: %v", err)
	}
	t.Cleanup(func() { _ = rawChannel.Close() })
	deleteParkQueues(rawChannel, destination)
	t.Cleanup(func() { deleteParkQueues(rawChannel, destination) })

	conn, err := (Driver{}).Open(ctx, driver.Config{Endpoints: []string{defaultEndpoint}})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(ctx) })

	// Declare the main destination through the driver so its arguments match
	// the verify spec below and only the park queue is left stale.
	if _, err := conn.Admin().EnsureTopology(ctx, driver.TopologySpec{
		Destinations: []driver.DestinationSpec{{Name: destination, Durable: true}},
	}); err != nil {
		t.Fatalf("EnsureTopology(declare main): %v", err)
	}

	// Hand-declare the park queue with the pre-fix argument set: quorum type
	// and the dead-letter route, but neither at-least-once argument.
	if _, err := rawChannel.QueueDeclare(park, true, false, false, false, amqp.Table{
		"x-queue-type":              "quorum",
		"x-dead-letter-exchange":    "",
		"x-dead-letter-routing-key": destination,
	}); err != nil {
		t.Fatalf("out-of-band QueueDeclare(%q): %v", park, err)
	}

	// Confirm through the management API that the hand-declared queue really
	// lacks x-dead-letter-strategy before verifying. A typo here that
	// accidentally included the key would turn the assertion below green for
	// the wrong reason.
	mgmt, err := newManagementClient(defaultEndpoint, driver.Config{})
	if err != nil {
		t.Fatalf("newManagementClient: %v", err)
	}
	staleQueue, err := mgmt.getQueue(ctx, park)
	if err != nil {
		t.Fatalf("getQueue(%q): %v", park, err)
	}
	if _, present := staleQueue.Arguments["x-dead-letter-strategy"]; present {
		t.Fatalf("hand-declared park queue arguments = %+v, want x-dead-letter-strategy absent before verify", staleQueue.Arguments)
	}

	spec := driver.TopologySpec{
		Policy:       driver.TopologyVerify,
		Destinations: []driver.DestinationSpec{{Name: destination, Durable: true, Delay: delay}},
	}
	diff, err := conn.Admin().EnsureTopology(ctx, spec)
	if err != nil {
		t.Fatalf("EnsureTopology(verify): %v", err)
	}
	found := false
	for _, d := range diff.Drifted {
		if d.Name == park && d.Argument == "x-dead-letter-strategy" {
			found = true
			if d.Got != "<absent>" {
				t.Fatalf("Drifted entry = %+v, want Got=<absent>", d)
			}
		}
	}
	if !found {
		t.Fatalf("Drifted = %v, want an entry for %q naming x-dead-letter-strategy", diff.Drifted, park)
	}
}

// TestEnsureTopologyParkQueueArgumentsClassicKind proves parkArguments
// honours a classic-configured connection: the park queue's x-queue-type
// must be classic, and neither at-least-once argument may appear, since
// RabbitMQ's at-least-once dead-letter strategy applies to quorum queues
// only.
func TestEnsureTopologyParkQueueArgumentsClassicKind(t *testing.T) {
	requireBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	const destination = "rabbitmq-driver-park-args-classic-queue"
	const delay = time.Second
	park := parkQueueName(destination)
	raw, err := amqp.Dial(defaultEndpoint)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = raw.Close() })
	rawChannel, err := raw.Channel()
	if err != nil {
		t.Fatalf("Channel: %v", err)
	}
	t.Cleanup(func() { _ = rawChannel.Close() })
	deleteParkQueues(rawChannel, destination)
	t.Cleanup(func() { deleteParkQueues(rawChannel, destination) })

	conn, err := (Driver{}).Open(ctx, driver.Config{
		Endpoints:     []string{defaultEndpoint},
		DriverOptions: map[string]string{"rabbitmq.queueType": "classic"},
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(ctx) })

	spec := driver.TopologySpec{
		Destinations: []driver.DestinationSpec{{Name: destination, Durable: true, Delay: delay}},
	}
	if _, err := conn.Admin().EnsureTopology(ctx, spec); err != nil {
		t.Fatalf("EnsureTopology: %v", err)
	}

	mgmt, err := newManagementClient(defaultEndpoint, driver.Config{})
	if err != nil {
		t.Fatalf("newManagementClient: %v", err)
	}
	queue, err := mgmt.getQueue(ctx, park)
	if err != nil {
		t.Fatalf("getQueue(%q): %v", park, err)
	}
	if got := fmt.Sprint(queue.Arguments["x-queue-type"]); got != "classic" {
		t.Fatalf("park queue x-queue-type = %v, want classic", got)
	}
	for _, key := range []string{"x-dead-letter-strategy", "x-overflow"} {
		if _, present := queue.Arguments[key]; present {
			t.Fatalf("classic park queue arguments = %+v, want %q absent", queue.Arguments, key)
		}
	}
}

// TestEnsureTopologyDeclaresConsumerTimeout proves rabbitmq.consumerTimeout
// reaches the queue as x-consumer-timeout and that the value read back from the
// broker is the configured one, rather than only having been passed to the
// declare call. The classic case is a boundary, not a variant: RabbitMQ 4.3
// refuses the argument on a classic queue with a 406 PRECONDITION_FAILED that
// closes the channel, so a missing kind gate fails the declare outright. The
// unset case pins the decision to declare nothing when the operator set
// nothing, since a driver-chosen default here would cancel consumers that the
// deployment never asked to bound.
func TestEnsureTopologyDeclaresConsumerTimeout(t *testing.T) {
	requireBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	raw, err := amqp.Dial(defaultEndpoint)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = raw.Close() })
	rawChannel, err := raw.Channel()
	if err != nil {
		t.Fatalf("Channel: %v", err)
	}
	t.Cleanup(func() { _ = rawChannel.Close() })

	mgmt, err := newManagementClient(defaultEndpoint, driver.Config{})
	if err != nil {
		t.Fatalf("newManagementClient: %v", err)
	}

	cases := []struct {
		name    string
		queue   string
		options map[string]string
		want    any
	}{
		{
			name:    "quorum declares the configured timeout",
			queue:   "rabbitmq-driver-consumer-timeout-quorum",
			options: map[string]string{"rabbitmq.consumerTimeout": "90s"},
			want:    int64(90000),
		},
		{
			name:    "classic omits the timeout",
			queue:   "rabbitmq-driver-consumer-timeout-classic",
			options: map[string]string{"rabbitmq.queueType": "classic", "rabbitmq.consumerTimeout": "90s"},
		},
		{
			name:  "unset declares nothing",
			queue: "rabbitmq-driver-consumer-timeout-unset",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _ = rawChannel.QueueDelete(tc.queue, false, false, false)
			t.Cleanup(func() { _, _ = rawChannel.QueueDelete(tc.queue, false, false, false) })

			conn, err := (Driver{}).Open(ctx, driver.Config{
				Endpoints:     []string{defaultEndpoint},
				DriverOptions: tc.options,
			})
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			t.Cleanup(func() { _ = conn.Close(ctx) })

			spec := driver.TopologySpec{Destinations: []driver.DestinationSpec{{Name: tc.queue, Durable: true}}}
			if _, err := conn.Admin().EnsureTopology(ctx, spec); err != nil {
				t.Fatalf("EnsureTopology: %v", err)
			}

			queue, err := mgmt.getQueue(ctx, tc.queue)
			if err != nil {
				t.Fatalf("getQueue(%q): %v", tc.queue, err)
			}
			got, present := queue.Arguments["x-consumer-timeout"]
			if tc.want == nil {
				if present {
					t.Fatalf("declared queue arguments = %+v, want x-consumer-timeout absent", queue.Arguments)
				}
				return
			}
			if !present {
				t.Fatalf("declared queue arguments = %+v, want x-consumer-timeout %v", queue.Arguments, tc.want)
			}
			if !argumentValuesEqual(tc.want, got) {
				t.Fatalf("declared x-consumer-timeout = %v (%T), want %v (%T)", got, got, tc.want, tc.want)
			}
		})
	}
}

// TestResolveConsumerTimeoutRejectsUnusableValues covers the values that cannot
// be declared: an unparsable duration, and anything that the broker would read
// as zero milliseconds, which cancels every consumer on the queue immediately.
func TestResolveConsumerTimeoutRejectsUnusableValues(t *testing.T) {
	cases := []struct {
		name    string
		options map[string]string
		want    time.Duration
		ok      bool
	}{
		{name: "absent declares nothing", options: nil, ok: true},
		{name: "configured", options: map[string]string{"rabbitmq.consumerTimeout": "90s"}, want: 90 * time.Second, ok: true},
		{name: "sub-millisecond", options: map[string]string{"rabbitmq.consumerTimeout": "500us"}},
		{name: "zero", options: map[string]string{"rabbitmq.consumerTimeout": "0s"}},
		{name: "negative", options: map[string]string{"rabbitmq.consumerTimeout": "-1s"}},
		{name: "unparsable", options: map[string]string{"rabbitmq.consumerTimeout": "garbage"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := resolveConsumerTimeout(tc.options)
			if !tc.ok {
				if err == nil || !strings.Contains(err.Error(), "rabbitmq.consumerTimeout") {
					t.Fatalf("resolveConsumerTimeout(%v) error = %v, want error naming the option", tc.options, err)
				}
				return
			}
			if err != nil || got != tc.want {
				t.Fatalf("resolveConsumerTimeout(%v) = %v, %v, want %v", tc.options, got, err, tc.want)
			}
		})
	}
}

func containsString(values []string, want string) bool {
	return slices.Contains(values, want)
}

func containsBinding(values []driver.BindingSpec, want driver.BindingSpec) bool {
	return slices.Contains(values, want)
}

// TestEnsureTopologyDelayChangeReusesTheParkingQueue proves a retry tier
// whose delay changes keeps its one parking queue: the redeclare reports the
// queue as existing, creates nothing, orphans nothing, and a message published
// afterwards waits the new delay. A delay carried as a queue argument would
// instead need a second queue, because RabbitMQ refuses to redeclare a queue
// with different arguments.
func TestEnsureTopologyDelayChangeReusesTheParkingQueue(t *testing.T) {
	requireBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	const destination = "rabbitmq-driver-park-delay-change"
	parkQueue := parkQueueName(destination)
	raw, err := amqp.Dial(defaultEndpoint)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = raw.Close() })
	rawChannel, err := raw.Channel()
	if err != nil {
		t.Fatalf("Channel: %v", err)
	}
	t.Cleanup(func() { _ = rawChannel.Close() })
	deleteParkQueues(rawChannel, destination)
	t.Cleanup(func() { deleteParkQueues(rawChannel, destination) })

	conn, err := (Driver{}).Open(ctx, driver.Config{Endpoints: []string{defaultEndpoint}})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(ctx) })

	if _, err := conn.Admin().EnsureTopology(ctx, driver.TopologySpec{
		Destinations: []driver.DestinationSpec{{Name: destination, Durable: true, Delay: 25 * time.Second}},
	}); err != nil {
		t.Fatalf("EnsureTopology(25s): %v", err)
	}
	diff, err := conn.Admin().EnsureTopology(ctx, driver.TopologySpec{
		Destinations: []driver.DestinationSpec{{Name: destination, Durable: true, Delay: time.Second}},
		Scope:        []string{destination},
	})
	if err != nil {
		t.Fatalf("EnsureTopology(1s): %v", err)
	}
	if len(diff.CreatedDestinations) != 0 {
		t.Fatalf("CreatedDestinations = %v, want none after a delay change", diff.CreatedDestinations)
	}
	if !slices.Contains(diff.ExistingDestinations, parkQueue) {
		t.Fatalf("ExistingDestinations = %v, want the parking queue %q", diff.ExistingDestinations, parkQueue)
	}
	if len(diff.Orphaned) != 0 {
		t.Fatalf("Orphaned = %v, want none after a delay change", diff.Orphaned)
	}

	deliveries, err := rawChannel.Consume(destination, "park-delay-change", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	producer, err := conn.Producer(ctx, driver.ProducerConfig{})
	if err != nil {
		t.Fatalf("Producer: %v", err)
	}
	t.Cleanup(func() { _ = producer.Close(ctx) })
	publishedAt := time.Now() //nolint:forbidigo // the timing assertion uses the real broker clock
	if err := producer.Publish(ctx, driver.OutboundMessage{Destination: destination, Body: []byte("new-delay")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	deadlineCtx, deadlineCancel := context.WithDeadline(ctx, publishedAt.Add(5*time.Second))
	defer deadlineCancel()
	select {
	case delivery := <-deliveries:
		if elapsed := time.Since(publishedAt); elapsed < time.Second { //nolint:forbidigo // the timing assertion uses the real broker clock
			t.Fatalf("delivery after %s, want at least the new 1s delay", elapsed)
		}
		if err := delivery.Ack(false); err != nil {
			t.Fatalf("Ack: %v", err)
		}
	case <-deadlineCtx.Done():
		t.Fatalf("delivery did not arrive within 5s under the new 1s delay: %v", deadlineCtx.Err())
	}
}

// TestEnsureTopologyDelayDeclaresOneParkingQueue proves a delayed destination
// gets exactly one parking queue, named for the destination alone, and no
// delay-tagged queue beside it.
func TestEnsureTopologyDelayDeclaresOneParkingQueue(t *testing.T) {
	requireBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	const destination = "rabbitmq-driver-fixed-park-queue"
	const delay = 5 * time.Second
	parkQueue := parkQueueName(destination)
	raw, err := amqp.Dial(defaultEndpoint)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = raw.Close() })
	rawChannel, err := raw.Channel()
	if err != nil {
		t.Fatalf("Channel: %v", err)
	}
	t.Cleanup(func() { _ = rawChannel.Close() })
	deleteParkQueues(rawChannel, destination)
	t.Cleanup(func() { deleteParkQueues(rawChannel, destination) })

	conn, err := (Driver{}).Open(ctx, driver.Config{Endpoints: []string{defaultEndpoint}})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(ctx) })

	diff, err := conn.Admin().EnsureTopology(ctx, driver.TopologySpec{
		Destinations: []driver.DestinationSpec{{Name: destination, Durable: true, Delay: delay}},
	})
	if err != nil {
		t.Fatalf("EnsureTopology: %v", err)
	}
	if want := []string{destination, parkQueue}; !slices.Equal(diff.CreatedDestinations, want) {
		t.Fatalf("CreatedDestinations = %v, want %v", diff.CreatedDestinations, want)
	}

	mgmt, err := newManagementClient(defaultEndpoint, driver.Config{})
	if err != nil {
		t.Fatalf("newManagementClient: %v", err)
	}
	park, err := mgmt.getQueue(ctx, parkQueue)
	if err != nil {
		t.Fatalf("getQueue(%q): %v", parkQueue, err)
	}
	if value, present := park.Arguments["x-message-ttl"]; present {
		t.Fatalf("parking queue x-message-ttl = %v, want none", value)
	}
	for _, absent := range []string{destination + ".park.fixed-5000ms", destination + ".park.8s"} {
		if _, err := mgmt.getQueue(ctx, absent); err == nil {
			t.Fatalf("getQueue(%q) succeeded, want no such queue", absent)
		}
	}
}

// assertQueueArguments fails unless arguments carries every entry of each want
// map, compared as printed values.
func assertQueueArguments(t *testing.T, queue string, arguments map[string]any, wants ...map[string]string) {
	t.Helper()
	for _, want := range wants {
		for key, wantValue := range want {
			got, present := arguments[key]
			if !present {
				t.Fatalf("%s queue arguments %+v missing %q", queue, arguments, key)
			}
			if fmt.Sprint(got) != wantValue {
				t.Fatalf("%s queue argument %q = %v, want %q", queue, key, got, wantValue)
			}
		}
	}
}
