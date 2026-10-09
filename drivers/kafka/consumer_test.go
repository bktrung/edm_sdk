package kafka

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

// TestConsumerFetchMaxWaitOption pins the driver's default fetch wait and its
// DriverOptions override on the consumer client itself. The option is read from
// the client because that is the value franz-go uses for fetch requests; a
// timing assertion would only prove that a much larger wait eventually works.
func TestConsumerFetchMaxWaitOption(t *testing.T) {
	for _, tc := range []struct {
		name    string
		options map[string]string
		want    time.Duration
	}{
		{name: "default", want: 50 * time.Millisecond},
		{
			name:    "override",
			options: map[string]string{fetchMaxWaitOption: "125ms"},
			want:    125 * time.Millisecond,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			connection := &conn{
				clientOpts:    []kgo.Opt{noDialKafkaOption()},
				driverOptions: tc.options,
			}
			opts, err := consumerClientOpts(connection, driver.ConsumerConfig{Destinations: []string{"topic"}}, "group", nil)
			if err != nil {
				t.Fatalf("consumerClientOpts error = %v", err)
			}
			client, err := kgo.NewClient(opts...)
			if err != nil {
				t.Fatalf("NewClient error = %v", err)
			}
			defer client.Close()

			if got, want := client.OptValues(kgo.FetchMaxWait), []any{tc.want}; !reflect.DeepEqual(got, want) {
				t.Fatalf("kgo.FetchMaxWait = %v, want %v", got, want)
			}
		})
	}
}

func TestDriverOpenRefusesInvalidFetchMaxWait(t *testing.T) {
	_, err := (Driver{}).Open(context.Background(), driver.Config{
		Endpoints: []string{"localhost:1"},
		DriverOptions: map[string]string{
			fetchMaxWaitOption: "not-a-duration",
		},
	})
	if err == nil {
		t.Fatal("Open with invalid kafka.fetchMaxWait returned nil error")
	}
	if !strings.Contains(err.Error(), fetchMaxWaitOption) {
		t.Fatalf("Open error = %v, want it to name %s", err, fetchMaxWaitOption)
	}
	var classified *driver.Error
	if !errors.As(err, &classified) || classified.Kind() != driver.KindFatal {
		t.Fatalf("Open error = %T/%v, want fatal classified error", err, err)
	}
}

func TestResolveFetchMaxWaitRejectsUnrepresentableDurations(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value string
	}{
		{name: "submillisecond", value: "1ns"},
		{name: "fractional millisecond", value: "1.5ms"},
		{name: "int32 overflow", value: (maxKafkaFetchMaxWait + time.Millisecond).String()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := resolveFetchMaxWait(map[string]string{fetchMaxWaitOption: tc.value})
			if err == nil {
				t.Fatalf("resolveFetchMaxWait(%q) returned nil error", tc.value)
			}
			for _, want := range []string{"1ms", maxKafkaFetchMaxWait.String()} {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("resolveFetchMaxWait(%q) error = %v, want %q", tc.value, err, want)
				}
			}
		})
	}
}

// TestAssignmentWarningSkipsRetryTiers pins that a destination fed by the retry
// ladder is not warned about however few of its partitions this member holds,
// while an ordinary destination of the same member in the same assignment still
// is. A retry tier's slots describe a delay budget rather than a throughput
// ceiling, so its partition count is not the lever the warning names, and
// warning about it would point an operator at the wrong knob.
func TestAssignmentWarningSkipsRetryTiers(t *testing.T) {
	retry, topic := "orders.retry.1", "orders"
	consumer := &consumer{
		clock: clock.NewReal(),
		cfg:   driver.ConsumerConfig{Delays: map[string]time.Duration{retry: time.Second}},
		budgets: map[string]int{
			retry: 4,
			topic: 4,
		},
		owned: map[partitionKey]bool{
			{destination: retry, partition: 0}: true,
			{destination: topic, partition: 0}: true,
		},
	}

	consumer.mu.Lock()
	warnings := consumer.assignmentWarningsLocked(map[string][]int32{retry: {0}, topic: {0}})
	consumer.mu.Unlock()

	if len(warnings) != 1 {
		t.Fatalf("warnings = %v, want one for the destination that is not a retry tier", warnings)
	}
	if warnings[0].destination != topic {
		t.Fatalf("warnings[0].destination = %q, want %q: the retry tier has a delay and must not warn", warnings[0].destination, topic)
	}
	if warnings[0].held != 1 || warnings[0].budget != 4 {
		t.Fatalf("warning = %+v, want held 1 of budget 4", warnings[0])
	}
}

// TestAssignmentWarningInTwoRounds pins what the warning does when one
// destination's assignment for a member arrives in two callbacks of the same
// rebalance, which is the shape a cooperative rebalance takes when the first
// round carries only the partitions no other member held.
//
// The decision is that the warning is decided on the callback that names the
// destination, on the member's accumulated owned count, and is not retracted:
// nothing a callback carries says whether another round follows it, and waiting
// for a round that may never come would not warn at all on the single-round
// rebalance that is the common case. The cost is that a member whose first
// callback is short and whose second fills its budget keeps a warning it does
// not deserve; this test is where that behaviour is recorded rather than
// discovered.
func TestAssignmentWarningInTwoRounds(t *testing.T) {
	topic := "orders"
	consumer := &consumer{
		clock:   clock.NewReal(),
		cfg:     driver.ConsumerConfig{},
		budgets: map[string]int{topic: 3},
		owned:   map[partitionKey]bool{{destination: topic, partition: 0}: true},
	}

	consumer.mu.Lock()
	warnings := consumer.assignmentWarningsLocked(map[string][]int32{topic: {0}})
	consumer.mu.Unlock()
	if len(warnings) != 1 {
		t.Fatalf("first round warnings = %v, want one: the member holds one of the three partitions its budget calls for", warnings)
	}

	// The second round of the same rebalance hands over the two partitions the
	// other member was holding, so the member ends the rebalance with its
	// budget. The warning above is not taken back, and the second callback adds
	// none of its own.
	consumer.mu.Lock()
	consumer.owned[partitionKey{destination: topic, partition: 1}] = true
	consumer.owned[partitionKey{destination: topic, partition: 2}] = true
	later := consumer.assignmentWarningsLocked(map[string][]int32{topic: {1, 2}})
	consumer.mu.Unlock()
	if len(later) != 0 {
		t.Fatalf("second round warnings = %v, want none: the destination is already recorded as warned", later)
	}
	if held := consumer.ownedPartitionsLocked(topic); held != 3 {
		t.Fatalf("the member holds %d partitions after both rounds, want its budget of 3", held)
	}
}

func TestInboundMessageEnqueueFields(t *testing.T) {
	t.Parallel()
	receivedAt := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	producerAt := receivedAt.Add(-time.Hour)
	brokerAt := receivedAt.Add(-2 * time.Hour)

	t.Run("producer timestamp", func(t *testing.T) {
		record := kafkaTestRecord(t, "orders", 0, 1, producerAt, 0, false)
		message := inboundMessage(record, nil, receivedAt)
		if !message.EnqueuedAt.Equal(producerAt) {
			t.Fatalf("EnqueuedAt = %v, want %v", message.EnqueuedAt, producerAt)
		}
		if message.EnqueuedAtSource != driver.EnqueueSourceProducer {
			t.Fatalf("EnqueuedAtSource = %v, want producer", message.EnqueuedAtSource)
		}
	})

	t.Run("broker timestamp", func(t *testing.T) {
		record := kafkaTestRecord(t, "orders", 0, 1, brokerAt, 1, false)
		message := inboundMessage(record, nil, receivedAt)
		if !message.EnqueuedAt.Equal(brokerAt) {
			t.Fatalf("EnqueuedAt = %v, want %v", message.EnqueuedAt, brokerAt)
		}
		if message.EnqueuedAtSource != driver.EnqueueSourceBroker {
			t.Fatalf("EnqueuedAtSource = %v, want broker", message.EnqueuedAtSource)
		}
	})

	t.Run("zero timestamp", func(t *testing.T) {
		record := &kgo.Record{Topic: "orders", Partition: 0, Offset: 1}
		message := inboundMessage(record, nil, receivedAt)
		if !message.EnqueuedAt.IsZero() {
			t.Fatalf("EnqueuedAt = %v, want zero", message.EnqueuedAt)
		}
		if message.EnqueuedAtSource != driver.EnqueueSourceUnknown {
			t.Fatalf("EnqueuedAtSource = %v, want unknown", message.EnqueuedAtSource)
		}
		if !message.ReceivedAt.Equal(receivedAt) {
			t.Fatalf("ReceivedAt = %v, want %v", message.ReceivedAt, receivedAt)
		}
	})

	t.Run("unknown timestamp type", func(t *testing.T) {
		record := kafkaTestRecord(t, "orders", 0, 1, producerAt, -1, false)
		message := inboundMessage(record, nil, receivedAt)
		if !message.EnqueuedAt.IsZero() {
			t.Fatalf("EnqueuedAt = %v, want zero", message.EnqueuedAt)
		}
		if message.EnqueuedAtSource != driver.EnqueueSourceUnknown {
			t.Fatalf("EnqueuedAtSource = %v, want unknown", message.EnqueuedAtSource)
		}
	})
}
