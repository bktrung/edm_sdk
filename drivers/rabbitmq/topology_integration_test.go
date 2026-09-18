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
// dead-letter arguments reach the broker rather than only a Go map: it reads
// the declared park queue back through the management API, which is the only
// channel that reports a queue's real arguments (AMQP passive declare checks
// the name only).
func TestEnsureTopologyParkQueueArgumentsReachBroker(t *testing.T) {
	requireBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	const destination = "rabbitmq-driver-park-args-queue"
	const park = destination + ".park"
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

	spec := driver.TopologySpec{
		Destinations: []driver.DestinationSpec{{Name: destination, Durable: true, Delay: time.Second}},
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
	want := map[string]string{
		"x-queue-type":              "quorum",
		"x-dead-letter-exchange":    "",
		"x-dead-letter-routing-key": destination,
		"x-dead-letter-strategy":    "at-least-once",
		"x-overflow":                "reject-publish",
	}
	for key, wantValue := range want {
		got, present := queue.Arguments[key]
		if !present {
			t.Fatalf("park queue arguments %+v missing %q", queue.Arguments, key)
		}
		if fmt.Sprint(got) != wantValue {
			t.Fatalf("park queue argument %q = %v, want %q", key, got, wantValue)
		}
	}
}

// TestEnsureTopologyRungQueueArgumentsReachBroker pins the queue-level TTL that
// makes expiry order FIFO order, read back through the management API rather
// than from the Go map, because AMQP passive declare checks a name and not its
// arguments. The queue above the ladder is read in the same run and must not
// carry that TTL: it keeps the per-message expiration a due time beyond the
// ladder needs, and a TTL there would release such a message early.
func TestEnsureTopologyRungQueueArgumentsReachBroker(t *testing.T) {
	requireBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	const destination = "rabbitmq-driver-park-rung-args-queue"
	const rung = 2 * time.Second
	rungQueue := parkQueueName(destination, rung)
	const aboveLadder = destination + ".park"
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
		Destinations: []driver.DestinationSpec{{Name: destination, Durable: true, Delay: rung}},
	}); err != nil {
		t.Fatalf("EnsureTopology: %v", err)
	}

	mgmt, err := newManagementClient(defaultEndpoint, driver.Config{})
	if err != nil {
		t.Fatalf("newManagementClient: %v", err)
	}
	queue, err := mgmt.getQueue(ctx, rungQueue)
	if err != nil {
		t.Fatalf("getQueue(%q): %v", rungQueue, err)
	}
	want := map[string]string{
		"x-queue-type":              "quorum",
		"x-dead-letter-exchange":    "",
		"x-dead-letter-routing-key": destination,
		"x-dead-letter-strategy":    "at-least-once",
		"x-overflow":                "reject-publish",
		"x-message-ttl":             "2000",
	}
	for key, wantValue := range want {
		got, present := queue.Arguments[key]
		if !present {
			t.Fatalf("rung queue arguments %+v missing %q", queue.Arguments, key)
		}
		if fmt.Sprint(got) != wantValue {
			t.Fatalf("rung queue argument %q = %v, want %q", key, got, wantValue)
		}
	}
	above, err := mgmt.getQueue(ctx, aboveLadder)
	if err != nil {
		t.Fatalf("getQueue(%q): %v", aboveLadder, err)
	}
	if value, present := above.Arguments["x-message-ttl"]; present {
		t.Fatalf("the queue above the ladder carries x-message-ttl=%v, which would release a beyond-the-ladder due time early", value)
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
	const park = destination + ".park"
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

	// Declare the rung queues through the driver. The declare path checks a
	// parking queue by name and never rewrites its arguments, so the hand-made
	// queue above the ladder is left exactly as it is, while the ladder the
	// verify pass below requires now exists.
	if _, err := conn.Admin().EnsureTopology(ctx, driver.TopologySpec{
		Destinations: []driver.DestinationSpec{{Name: destination, Durable: true, Delay: time.Second}},
	}); err != nil {
		t.Fatalf("EnsureTopology(declare ladder): %v", err)
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
		Destinations: []driver.DestinationSpec{{Name: destination, Durable: true, Delay: time.Second}},
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

// TestEnsureTopologyParkQueueArgumentsClassicKind proves parkingArguments
// honours a classic-configured connection: the park queue's x-queue-type
// must be classic, and neither at-least-once argument may appear, since
// RabbitMQ's at-least-once dead-letter strategy applies to quorum queues
// only.
func TestEnsureTopologyParkQueueArgumentsClassicKind(t *testing.T) {
	requireBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	const destination = "rabbitmq-driver-park-args-classic-queue"
	const park = destination + ".park"
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
		Destinations: []driver.DestinationSpec{{Name: destination, Durable: true, Delay: time.Second}},
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

func TestEnsureTopologyFixedDelayDeclaresOneQueue(t *testing.T) {
	requireBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	const destination = "rabbitmq-driver-fixed-park-queue"
	fixedQueue := destination + ".park.fixed-5000ms"
	aboveLadder := destination + ".park"
	rungQueue := destination + ".park.8s"
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
	_, _ = rawChannel.QueueDelete(fixedQueue, false, false, false)
	t.Cleanup(func() {
		deleteParkQueues(rawChannel, destination)
		_, _ = rawChannel.QueueDelete(fixedQueue, false, false, false)
	})

	conn, err := (Driver{}).Open(ctx, driver.Config{Endpoints: []string{defaultEndpoint}})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(ctx) })

	if _, err := conn.Admin().EnsureTopology(ctx, driver.TopologySpec{
		Destinations: []driver.DestinationSpec{{Name: destination, Durable: true, Delay: 5 * time.Second, FixedDelay: true}},
	}); err != nil {
		t.Fatalf("EnsureTopology: %v", err)
	}

	mgmt, err := newManagementClient(defaultEndpoint, driver.Config{})
	if err != nil {
		t.Fatalf("newManagementClient: %v", err)
	}
	fixed, err := mgmt.getQueue(ctx, fixedQueue)
	if err != nil {
		t.Fatalf("getQueue(%q): %v", fixedQueue, err)
	}
	if got := fmt.Sprint(fixed.Arguments["x-message-ttl"]); got != "5000" {
		t.Fatalf("fixed queue x-message-ttl = %v, want 5000", got)
	}
	if _, err := mgmt.getQueue(ctx, aboveLadder); err != nil {
		t.Fatalf("getQueue(%q): %v", aboveLadder, err)
	}
	if _, err := mgmt.getQueue(ctx, rungQueue); err == nil {
		t.Fatalf("getQueue(%q) succeeded, want no such queue", rungQueue)
	}
}

func TestEnsureTopologyFixedDelayReportsOldRungsAsOrphans(t *testing.T) {
	requireBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const destination = "rabbitmq-driver-fixed-park-orphan"
	fixedQueue := destination + ".park.fixed-5000ms"
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
	_, _ = rawChannel.QueueDelete(fixedQueue, false, false, false)
	t.Cleanup(func() {
		deleteParkQueues(rawChannel, destination)
		_, _ = rawChannel.QueueDelete(fixedQueue, false, false, false)
	})

	conn, err := (Driver{}).Open(ctx, driver.Config{Endpoints: []string{defaultEndpoint}})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(ctx) })

	if _, err := conn.Admin().EnsureTopology(ctx, driver.TopologySpec{
		Destinations: []driver.DestinationSpec{{Name: destination, Durable: true, Delay: 5 * time.Second}},
	}); err != nil {
		t.Fatalf("EnsureTopology(ladder): %v", err)
	}
	diff, err := conn.Admin().EnsureTopology(ctx, driver.TopologySpec{
		Destinations: []driver.DestinationSpec{{Name: destination, Durable: true, Delay: 5 * time.Second, FixedDelay: true}},
		Scope:        []string{destination},
	})
	if err != nil {
		t.Fatalf("EnsureTopology(fixed): %v", err)
	}
	want := make(map[string]struct{}, len(parkRungs))
	for _, rung := range parkRungs {
		want[parkQueueName(destination, rung)] = struct{}{}
	}
	got := make(map[string]struct{}, len(diff.Orphaned))
	for _, orphan := range diff.Orphaned {
		got[orphan.Name] = struct{}{}
	}
	for name := range want {
		if _, found := got[name]; !found {
			t.Fatalf("Orphaned = %v, want old rung queue %q", diff.Orphaned, name)
		}
	}
	if len(diff.Orphaned) != len(want) {
		t.Fatalf("Orphaned = %v, want exactly the 8 old rung queues", diff.Orphaned)
	}
}
