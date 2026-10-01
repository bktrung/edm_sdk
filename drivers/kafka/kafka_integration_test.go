//go:build integration

package kafka

import (
	"context"
	"errors"
	"fmt"
	"net"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

func TestOpenPingClose(t *testing.T) {
	requireBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	opened, err := (Driver{}).Open(ctx, driver.Config{
		Endpoints: []string{kafkaEndpoint},
		ClientID:  "f1-kafka-open-test",
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	conn := opened
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	if err := conn.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}

	info := conn.BrokerInfo()
	if info.Kind != "kafka" {
		t.Errorf("BrokerInfo().Kind = %q, want kafka", info.Kind)
	}
	if info.Version != "" {
		t.Errorf("BrokerInfo().Version = %q, want empty", info.Version)
	}
	if len(info.Nodes) == 0 {
		t.Fatal("BrokerInfo().Nodes is empty, want at least one broker")
	}
	for _, node := range info.Nodes {
		host, port, err := net.SplitHostPort(node)
		if err != nil || host == "" || port == "" {
			t.Fatalf("BrokerInfo().Nodes contains invalid host:port %q", node)
		}
	}
	metadataVersion, err := strconv.Atoi(info.Extra["metadata.version"])
	if err != nil || metadataVersion <= 0 {
		t.Fatalf("BrokerInfo().Extra[metadata.version] = %q, want positive integer", info.Extra["metadata.version"])
	}

	if err := conn.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := conn.Close(ctx); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestCapabilitiesReadBrokerLimits(t *testing.T) {
	requireBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	client, err := kgo.NewClient(kgo.SeedBrokers(kafkaEndpoint))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(client.Close)
	if err := client.Ping(ctx); err != nil {
		t.Fatalf("independent Ping: %v", err)
	}
	admin := kadm.NewClient(client)
	metadata, err := admin.Metadata(ctx)
	if err != nil {
		t.Fatalf("independent Metadata: %v", err)
	}
	configs, err := admin.DescribeBrokerConfigs(ctx, metadata.Controller)
	if err != nil {
		t.Fatalf("independent DescribeBrokerConfigs: %v", err)
	}
	var wantMessageBytes int
	for _, resource := range configs {
		if resource.Err != nil {
			t.Fatalf("independent broker config resource: %v", resource.Err)
		}
		for _, config := range resource.Configs {
			if config.Key != "message.max.bytes" {
				continue
			}
			wantMessageBytes, err = strconv.Atoi(config.MaybeValue())
			if err != nil {
				t.Fatalf("independent message.max.bytes = %q: %v", config.MaybeValue(), err)
			}
		}
	}
	if wantMessageBytes == 0 {
		t.Fatal("independent broker config did not return message.max.bytes")
	}

	opened, err := (Driver{}).Open(ctx, driver.Config{Endpoints: []string{kafkaEndpoint}})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = opened.Close(context.Background()) })
	caps := opened.Capabilities()
	if caps.MaxMessageBytes <= 0 {
		t.Fatalf("MaxMessageBytes = %d, want positive declaration", caps.MaxMessageBytes)
	}

	connection, ok := opened.(*conn)
	if !ok {
		t.Fatalf("Open() returned %T, want *conn", opened)
	}
	topic := kafkaTestTopic(t, "capability-boundary")
	cleanupKafkaTopics(t, admin, topic)
	createKafkaTopic(t, admin, ctx, topic, 1)

	producer, err := connection.Producer(ctx, driver.ProducerConfig{Effective: caps})
	if err != nil {
		t.Fatalf("Producer: %v", err)
	}
	t.Cleanup(func() { _ = producer.Close(context.Background()) })

	body := make([]byte, caps.MaxMessageBytes)
	if err := producer.Publish(ctx, driver.OutboundMessage{Destination: topic, Body: body}); err != nil {
		t.Fatalf("Publish(exact declared body): %v", err)
	}
	if caps.MaxMessageBytes >= wantMessageBytes {
		t.Fatalf("MaxMessageBytes = %d, want below raw message.max.bytes %d", caps.MaxMessageBytes, wantMessageBytes)
	}
	oversized := make([]byte, caps.MaxMessageBytes+1)
	err = producer.Publish(ctx, driver.OutboundMessage{Destination: topic, Body: oversized})
	kind, classified := driver.Classify(err)
	if err == nil || !classified || kind != driver.KindTooLarge {
		t.Fatalf("Publish(declared body plus one) classification = (%v, %t), want (too_large, true): %v", kind, classified, err)
	}

	starts, err := admin.ListStartOffsets(ctx, topic)
	if err != nil {
		t.Fatalf("ListStartOffsets(%q): %v", topic, err)
	}
	ends, err := admin.ListEndOffsets(ctx, topic)
	if err != nil {
		t.Fatalf("ListEndOffsets(%q): %v", topic, err)
	}
	depth, present, err := topicDepth(topic, starts, ends)
	if err != nil {
		t.Fatalf("topicDepth(%q): %v", topic, err)
	}
	if !present {
		t.Fatalf("topicDepth(%q) present = false, want true", topic)
	}
	if depth != 1 {
		t.Fatalf("topicDepth(%q) = %d, want exactly one retained boundary message", topic, depth)
	}
}

func TestOpenRefusesEmptyEndpoints(t *testing.T) {
	cases := []struct {
		name      string
		endpoints []string
	}{
		{name: "nil", endpoints: nil},
		{name: "empty", endpoints: []string{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := (Driver{}).Open(context.Background(), driver.Config{Endpoints: tc.endpoints})
			assertFatalOpenError(t, err, "broker endpoints must not be empty")
		})
	}
}

func TestOpenRefusesEmptySASLCredentials(t *testing.T) {
	for _, mechanism := range []string{"plain", "scram-sha-256", "scram-sha-512"} {
		for _, missing := range []string{"username", "password"} {
			t.Run(mechanism+"/"+missing, func(t *testing.T) {
				settings := &driver.SASLConfig{
					Mechanism: mechanism,
					Username:  "user",
					Password:  "password",
				}
				if missing == "username" {
					settings.Username = ""
				} else {
					settings.Password = ""
				}
				_, err := (Driver{}).Open(context.Background(), driver.Config{
					Endpoints:      []string{"127.0.0.1:1"},
					ConnectTimeout: 100 * time.Millisecond,
					SASL:           settings,
				})
				assertFatalOpenError(t, err, "SASL username and password must be non-empty")
			})
		}
	}
}

// kafkaOptionCases is one row per broker.kafka.* key this driver must read, the
// franz-go option it must reach, and the value read back from that option. Each
// row configures a value that is not the driver's default for its knob, so a
// test that observes the default fails rather than passing by coincidence.
var kafkaOptionCases = []struct {
	name  string
	key   string
	value string
	opt   any
	want  []any
}{
	{
		name:  "compression",
		key:   "kafka.compression",
		value: "lz4",
		opt:   kgo.ProducerBatchCompression,
		want:  []any{[]kgo.CompressionCodec{kgo.Lz4Compression()}},
	},
	{
		name:  "batchLinger",
		key:   "kafka.batchLinger",
		value: "250ms",
		opt:   kgo.ProducerLinger,
		want:  []any{250 * time.Millisecond},
	},
	{
		name:  "fetchMaxBytes",
		key:   "kafka.fetchMaxBytes",
		value: "1048576",
		opt:   kgo.FetchMaxBytes,
		want:  []any{int32(1048576)},
	},
	{
		name:  "sessionTimeout",
		key:   "kafka.sessionTimeout",
		value: "96s",
		opt:   kgo.SessionTimeout,
		want:  []any{96 * time.Second},
	},
	{
		name:  "rebalanceTimeout",
		key:   "kafka.rebalanceTimeout",
		value: "180s",
		opt:   kgo.RebalanceTimeout,
		want:  []any{180 * time.Second},
	},
}

func kafkaOptionValues() map[string]string {
	values := make(map[string]string, len(kafkaOptionCases))
	for _, tc := range kafkaOptionCases {
		values[tc.key] = tc.value
	}
	return values
}

// TestKafkaOptionReachesClientPerKey asserts, one key at a time, that a
// configured value arrives at a franz-go client. Each subtest names its key, so
// a key whose read is deleted fails that subtest: the option then carries the
// driver's default instead of the configured value. This is the fast suite's
// copy of the invariant; TestKafkaOptionsReachOpenedClients covers the same
// ground against the clients Open itself built.
func TestKafkaOptionReachesClientPerKey(t *testing.T) {
	for _, tc := range kafkaOptionCases {
		t.Run(tc.name, func(t *testing.T) {
			client := newKafkaOptionClient(t, map[string]string{tc.key: tc.value})
			values := client.OptValues(tc.opt)
			if !reflect.DeepEqual(values, tc.want) {
				t.Fatalf("client option for %s=%s = %s, want %s",
					tc.key, tc.value, kafkaOptionText(values), kafkaOptionText(tc.want))
			}
		})
	}
}

// TestKafkaOptionDefaultPerKey pins the value a client carries when the
// operator sets none of these keys. The driver chooses these values rather than
// inheriting them from the client library, so each one is a decision the
// repository owns: the session timeout in particular is the value config
// validation measures lifecycle.rebalanceDrainTimeout against, and the
// compression and linger defaults are what an operator who sets nothing already
// has.
func TestKafkaOptionDefaultPerKey(t *testing.T) {
	client := newKafkaOptionClient(t, nil)
	cases := []struct {
		name string
		opt  any
		want []any
	}{
		{
			name: "compression",
			opt:  kgo.ProducerBatchCompression,
			want: []any{[]kgo.CompressionCodec{kgo.SnappyCompression(), kgo.NoCompression()}},
		},
		{name: "batchLinger", opt: kgo.ProducerLinger, want: []any{10 * time.Millisecond}},
		{name: "fetchMaxBytes", opt: kgo.FetchMaxBytes, want: []any{int32(50 << 20)}},
		{name: "sessionTimeout", opt: kgo.SessionTimeout, want: []any{45 * time.Second}},
		{name: "rebalanceTimeout", opt: kgo.RebalanceTimeout, want: []any{60 * time.Second}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			values := client.OptValues(tc.opt)
			if !reflect.DeepEqual(values, tc.want) {
				t.Fatalf("default client option = %v, want %v", values, tc.want)
			}
		})
	}
}

// TestKafkaOptionRefusedAtOpenPerKey asserts that a value the driver cannot
// translate is refused by Open with its key named. The endpoint is a dead port,
// so a value that only failed after a dial would not produce an error naming
// the key: passing this test means the refusal happened before any broker was
// contacted.
func TestKafkaOptionRefusedAtOpenPerKey(t *testing.T) {
	cases := []struct {
		name  string
		key   string
		value string
	}{
		{name: "compression", key: "kafka.compression", value: "brotli"},
		{name: "batchLinger", key: "kafka.batchLinger", value: "250"},
		{name: "fetchMaxBytes", key: "kafka.fetchMaxBytes", value: "0"},
		{name: "sessionTimeout", key: "kafka.sessionTimeout", value: "garbage"},
		{name: "rebalanceTimeout", key: "kafka.rebalanceTimeout", value: "-1s"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, err := (Driver{}).Open(ctx, driver.Config{
				Endpoints:     []string{"localhost:1"},
				ClientID:      "f1-kafka-options-test",
				DriverOptions: map[string]string{tc.key: tc.value},
			})
			if err == nil {
				t.Fatalf("Open with %s=%s error = nil, want a refusal naming the key", tc.key, tc.value)
			}
			if !strings.Contains(err.Error(), tc.key) {
				t.Fatalf("Open with %s=%s error = %v, want the key named", tc.key, tc.value, err)
			}
			var classified *driver.Error
			if !errors.As(err, &classified) || classified.Kind() != driver.KindFatal {
				t.Fatalf("Open with %s=%s error = %T/%v, want a fatal driver error", tc.key, tc.value, err, err)
			}
			t.Logf("Open with %s=%s refused: %v", tc.key, tc.value, err)
		})
	}
}

// TestKafkaBatchLingerZeroAndNegative pins both ends of the batchLinger rule,
// which is the one refusal that is not "must be positive". Zero is a real
// setting: franz-go sends the batch without waiting, so a refusal written as
// "must be positive" would break a configuration the operator can legitimately
// want. A negative duration is refused rather than clamped to zero, because a
// clamp turns a mistyped key into a producer that silently never lingers.
func TestKafkaBatchLingerZeroAndNegative(t *testing.T) {
	t.Run("zero reaches the client", func(t *testing.T) {
		client := newKafkaOptionClient(t, map[string]string{"kafka.batchLinger": "0s"})
		values := client.OptValues(kgo.ProducerLinger)
		if want := []any{time.Duration(0)}; !reflect.DeepEqual(values, want) {
			t.Fatalf("client option for kafka.batchLinger=0s = %s, want %s",
				kafkaOptionText(values), kafkaOptionText(want))
		}
	})

	t.Run("negative is refused with the key named", func(t *testing.T) {
		_, err := resolveProducerOptions(map[string]string{"kafka.batchLinger": "-1ms"})
		if err == nil || !strings.Contains(err.Error(), "kafka.batchLinger") {
			t.Fatalf("resolveProducerOptions(kafka.batchLinger=-1ms) error = %v, want a refusal naming the key", err)
		}
	})
}

// TestKafkaOptionsReachOpenedClients asserts that Open hands all five keys to
// the client it builds. It reads the client Open created rather than assembling
// an option list, so an option dropped between resolution and construction
// fails here. The consumer's own client, which is the one that owns the group,
// is covered by TestKafkaGroupTimeoutsReachBrokerGroupMetadata.
func TestKafkaOptionsReachOpenedClients(t *testing.T) {
	_, connection, _ := openKafkaConsumerTest(t, kafkaOptionValues())
	for _, tc := range kafkaOptionCases {
		values := connection.client.OptValues(tc.opt)
		if !reflect.DeepEqual(values, tc.want) {
			t.Errorf("Open client option for %s=%s = %v, want %v", tc.key, tc.value, values, tc.want)
		}
	}
}

// TestKafkaGroupTimeoutsReachBrokerGroupMetadata proves the two group timeouts
// against the broker rather than against the client: the coordinator records
// each member's session and rebalance timeout in the group metadata record it
// appends to the internal offsets topic, and this reads that record back.
//
// A value that only ever reached the root client would leave this test reading
// the broker's default of 45s, because the client that owns a group is the
// consumer's; the configured 96s also covers that the consumer's client carries
// the settings. The fetch ceiling is asserted on the same client for the same
// reason.
func TestKafkaGroupTimeoutsReachBrokerGroupMetadata(t *testing.T) {
	ctx, connection, admin := openKafkaConsumerTest(t, map[string]string{
		"kafka.fetchMaxBytes":    "1048576",
		"kafka.sessionTimeout":   "96s",
		"kafka.rebalanceTimeout": "180s",
	})
	topic := kafkaTestTopic(t, "options-group-timeout")
	group := kafkaTestTopic(t, "options-group-timeout-group")
	cleanupKafkaTopics(t, admin, topic)
	cleanupKafkaGroups(t, admin, group)
	createKafkaTopic(t, admin, ctx, topic, 1)

	producer, err := connection.Producer(ctx, driver.ProducerConfig{
		Effective: connection.Capabilities(),
	})
	if err != nil {
		t.Fatalf("Producer: %v", err)
	}
	defer func() { _ = producer.Close(context.Background()) }()
	publishKafkaMessage(t, producer, ctx, topic, "group-timeout")

	value, err := connection.Consumer(ctx, driver.ConsumerConfig{
		Group: group, Destinations: []string{topic}, Prefetch: 1, Effective: connection.Capabilities(),
	})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	t.Cleanup(func() { closeKafkaConsumer(value) })
	concrete, ok := value.(*consumer)
	if !ok {
		t.Fatalf("Consumer() returned %T, want *consumer", value)
	}
	delivered := receiveKafkaMessage(t, value)
	if err := delivered.Settle.Ack(ctx); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	if got, want := concrete.client.OptValues(kgo.FetchMaxBytes), []any{int32(1048576)}; !reflect.DeepEqual(got, want) {
		t.Errorf("consumer client kafka.fetchMaxBytes = %v, want %v", got, want)
	}

	metadata := kafkaGroupMetadata(t, admin, group)
	if len(metadata.Members) == 0 {
		t.Fatalf("group metadata for %q has no members: %#v", group, metadata)
	}
	for _, member := range metadata.Members {
		if got, want := time.Duration(member.SessionTimeoutMillis)*time.Millisecond, 96*time.Second; got != want {
			t.Errorf("broker session timeout for member %q = %v, want %v", member.MemberID, got, want)
		}
		if got, want := time.Duration(member.RebalanceTimeoutMillis)*time.Millisecond, 180*time.Second; got != want {
			t.Errorf("broker rebalance timeout for member %q = %v, want %v", member.MemberID, got, want)
		}
	}
}

// kafkaGroupMetadata reads a classic group's metadata record back from the
// broker's internal offsets topic, where the coordinator appends the record on
// every group state change. This is the only readout of the timeouts the broker
// accepted: DescribeGroups reports membership and assignment, but no timeouts.
//
// It polls every partition of the topic rather than the one partition the group
// hashes to, so the fixture's partition count does not have to be assumed.
func kafkaGroupMetadata(t *testing.T, admin *kadm.Client, group string) kmsg.GroupMetadataValue {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	topics, err := admin.ListTopics(ctx, "__consumer_offsets")
	if err != nil {
		t.Fatalf("ListTopics(__consumer_offsets): %v", err)
	}
	offsets, ok := topics["__consumer_offsets"]
	if !ok {
		t.Fatal("broker has no __consumer_offsets topic after a group joined")
	}
	partitions := make(map[int32]kgo.Offset, len(offsets.Partitions))
	for partition := range offsets.Partitions {
		partitions[partition] = kgo.NewOffset().AtStart()
	}
	client, err := kgo.NewClient(
		kgo.SeedBrokers(kafkaEndpoint),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{"__consumer_offsets": partitions}),
	)
	if err != nil {
		t.Fatalf("NewClient for __consumer_offsets: %v", err)
	}
	defer client.Close()

	var (
		found  bool
		latest kmsg.GroupMetadataValue
	)
	for !found {
		if err := ctx.Err(); err != nil {
			t.Fatalf("group metadata for %q not found before the deadline: %v", group, err)
		}
		fetches := client.PollFetches(ctx)
		if fails := fetches.Errors(); len(fails) > 0 {
			t.Fatalf("poll __consumer_offsets: %v", fails)
		}
		fetches.EachRecord(func(record *kgo.Record) {
			var key kmsg.GroupMetadataKey
			if err := key.ReadFrom(record.Key); err != nil || key.Group != group {
				return
			}
			var metadata kmsg.GroupMetadataValue
			// An offset-commit record's key and value both parse as a versioned
			// group record, so the protocol type and a non-empty member list are
			// what separate the metadata record from a committed offset.
			if err := metadata.ReadFrom(record.Value); err != nil || metadata.ProtocolType != "consumer" || len(metadata.Members) == 0 {
				return
			}
			// The topic keeps every generation's record until compaction runs,
			// and the record written last is the state in force.
			latest, found = metadata, true
		})
	}
	return latest
}

// newKafkaOptionClient builds a franz-go client from the options Open would
// hand it for these driver options, with no broker involved: the knobs are read
// back through the client's own option introspection.
func newKafkaOptionClient(t *testing.T, options map[string]string) *kgo.Client {
	t.Helper()
	producerOpts, err := resolveProducerOptions(options)
	if err != nil {
		t.Fatalf("resolveProducerOptions(%v): %v", options, err)
	}
	consumerOpts, err := resolveConsumerOptions(options)
	if err != nil {
		t.Fatalf("resolveConsumerOptions(%v): %v", options, err)
	}
	client, err := kgo.NewClient(append(append([]kgo.Opt(nil), producerOpts...), consumerOpts...)...)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(client.Close)
	return client
}

// kafkaOptionText renders an observed option value for a failure message. A
// codec otherwise prints as the struct fields behind it, so a failure line
// would read "[{3 0}]" where "[lz4]" is what the reader needs.
func kafkaOptionText(values []any) string {
	text := make([]string, len(values))
	for index, value := range values {
		text[index] = fmt.Sprintf("%v", value)
		codecs, ok := value.([]kgo.CompressionCodec)
		if !ok {
			continue
		}
		names := make([]string, len(codecs))
		for codecIndex, codec := range codecs {
			names[codecIndex] = kafkaCodecName(codec)
		}
		text[index] = fmt.Sprintf("%v", names)
	}
	return fmt.Sprintf("%v", text)
}

// kafkaCodecName names one of the codecs the driver configures. franz-go
// exposes no accessor for a codec's type, so the constructors are the lookup.
func kafkaCodecName(codec kgo.CompressionCodec) string {
	switch {
	case reflect.DeepEqual(codec, kgo.NoCompression()):
		return "none"
	case reflect.DeepEqual(codec, kgo.GzipCompression()):
		return "gzip"
	case reflect.DeepEqual(codec, kgo.SnappyCompression()):
		return "snappy"
	case reflect.DeepEqual(codec, kgo.Lz4Compression()):
		return "lz4"
	case reflect.DeepEqual(codec, kgo.ZstdCompression()):
		return "zstd"
	default:
		return fmt.Sprintf("%v", codec)
	}
}
