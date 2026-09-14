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
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

func TestCapabilityCeilingAndReduction(t *testing.T) {
	ceiling := Driver{}.Capabilities()
	if !ceiling.PerMessageAck {
		t.Error("ceiling PerMessageAck = false, want true")
	}
	if !ceiling.OrderedByKey {
		t.Error("ceiling OrderedByKey = false, want true")
	}
	if !ceiling.NativeDeliveryCount {
		t.Error("ceiling NativeDeliveryCount = false, want true")
	}
	if ceiling.NativeDLQ {
		t.Error("ceiling NativeDLQ = true, want false")
	}
	if ceiling.ConsumerScaling != driver.ScalingFree {
		t.Errorf("ceiling ConsumerScaling = %v, want free", ceiling.ConsumerScaling)
	}
	if ceiling.Fanout != driver.FanoutAtConsume {
		t.Errorf("ceiling Fanout = %v, want consume", ceiling.Fanout)
	}
	if !ceiling.LagQueryable {
		t.Error("ceiling LagQueryable = false, want true")
	}
	if ceiling.NativePriority != driver.PriorityNone {
		t.Errorf("ceiling NativePriority = %v, want none", ceiling.NativePriority)
	}
	if ceiling.NativeDelay {
		t.Error("ceiling NativeDelay = true, want false")
	}

	const maxMessageBytes = 123456
	reduced := classicCapabilities(maxMessageBytes)
	if reduced.PerMessageAck {
		t.Error("classic PerMessageAck = true, want false")
	}
	if reduced.NativeDeliveryCount {
		t.Error("classic NativeDeliveryCount = true, want false")
	}
	if reduced.ConsumerScaling != driver.ScalingPartitionBound {
		t.Errorf("classic ConsumerScaling = %v, want partition-bound", reduced.ConsumerScaling)
	}
	if !reduced.OrderedByKey {
		t.Error("classic OrderedByKey = false, want true")
	}
	// Kafka exposes no header limit.
	if reduced.MaxHeaderBytes != 0 {
		t.Errorf("classic MaxHeaderBytes = %d, want 0", reduced.MaxHeaderBytes)
	}
	if reduced.MaxMessageBytes != maxMessageBytes {
		t.Errorf("classic MaxMessageBytes = %d, want %d", reduced.MaxMessageBytes, maxMessageBytes)
	}

	strict := reduced.Strict()
	wantStrict := driver.Capabilities{
		OrderedByKey:    true,
		Fanout:          driver.FanoutAtConsume,
		ConsumerScaling: driver.ScalingPartitionBound,
		MaxMessageBytes: maxMessageBytes,
	}
	if !reflect.DeepEqual(strict, wantStrict) {
		t.Fatalf("classic Strict() = %#v, want %#v", strict, wantStrict)
	}
}

func TestEffectiveProducerBatchBytes(t *testing.T) {
	const maxBatchBytes = 1 << 30
	cases := []struct {
		name        string
		brokerLimit int
		want        int
		wantErr     bool
	}{
		{name: "below minimum", brokerLimit: 511, wantErr: true},
		{name: "minimum", brokerLimit: 512, want: 512},
		{name: "broker limit", brokerLimit: 123456, want: 123456},
		{name: "above maximum", brokerLimit: maxBatchBytes + 1, want: maxBatchBytes},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := effectiveProducerBatchBytes(tc.brokerLimit)
			if tc.wantErr {
				if err == nil {
					t.Fatal("effectiveProducerBatchBytes() error = nil, want error")
				}
				if !errors.Is(err, errBrokerConfig) {
					t.Fatalf("effectiveProducerBatchBytes() error = %v, want errBrokerConfig", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("effectiveProducerBatchBytes() error = %v", err)
			}
			if got != tc.want {
				t.Errorf("effectiveProducerBatchBytes() = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestKafkaBodyLimitSizing(t *testing.T) {
	cases := []struct {
		name       string
		batchLimit int
		wantBody   int
	}{
		{name: "empty body", batchLimit: 72, wantBody: 0},
		{name: "record length varint first boundary", batchLimit: 131, wantBody: 58},
		{name: "body length varint first boundary", batchLimit: 138, wantBody: 64},
		{name: "record length varint second boundary", batchLimit: 8260, wantBody: 8185},
		{name: "body length varint second boundary", batchLimit: 8268, wantBody: 8192},
		{name: "record length varint third boundary", batchLimit: 1048645, wantBody: 1048568},
		{name: "body length varint third boundary", batchLimit: 1048654, wantBody: 1048576},
		{name: "record length varint fourth boundary", batchLimit: 134217798, wantBody: 134217719},
		{name: "body length varint fourth boundary", batchLimit: 134217808, wantBody: 134217728},
		{name: "maximum batch limit", batchLimit: 1 << 30, wantBody: (1 << 30) - 80},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := maxKafkaBodyBytes(tc.batchLimit)
			if got != tc.wantBody {
				t.Fatalf("maxKafkaBodyBytes(%d) = %d, want %d", tc.batchLimit, got, tc.wantBody)
			}
			if got > 0 {
				if previous := maxKafkaBodyBytes(tc.batchLimit - 1); previous != got-1 {
					t.Fatalf("maxKafkaBodyBytes(%d) = %d, want %d", tc.batchLimit-1, previous, got-1)
				}
			}
			if encoded := kafkaBareRecordBatchBytes(got); encoded > tc.batchLimit {
				t.Fatalf("kafkaBareRecordBatchBytes(%d) = %d, exceeds limit %d", got, encoded, tc.batchLimit)
			}
			if encoded := kafkaBareRecordBatchBytes(got + 1); encoded <= tc.batchLimit {
				t.Fatalf("kafkaBareRecordBatchBytes(%d) = %d, fits limit %d", got+1, encoded, tc.batchLimit)
			}
		})
	}
}

func TestKafkaErrorKind(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want driver.Kind
	}{
		{name: "permission/topic", err: fmt.Errorf("wrapped: %w", kerr.TopicAuthorizationFailed), want: driver.KindPermission},
		{name: "permission/group", err: fmt.Errorf("wrapped: %w", kerr.GroupAuthorizationFailed), want: driver.KindPermission},
		{name: "permission/cluster", err: fmt.Errorf("wrapped: %w", kerr.ClusterAuthorizationFailed), want: driver.KindPermission},
		{name: "permission/transactional-id", err: fmt.Errorf("wrapped: %w", kerr.TransactionalIDAuthorizationFailed), want: driver.KindPermission},
		{name: "permission/delegation-token", err: fmt.Errorf("wrapped: %w", kerr.DelegationTokenAuthorizationFailed), want: driver.KindPermission},
		{name: "too large", err: fmt.Errorf("wrapped: %w", kerr.MessageTooLarge), want: driver.KindTooLarge},
		{name: "not found", err: fmt.Errorf("wrapped: %w", kerr.UnknownTopicOrPartition), want: driver.KindNotFound},
		{name: "fatal/sasl-authentication", err: fmt.Errorf("wrapped: %w", kerr.SaslAuthenticationFailed), want: driver.KindFatal},
		{name: "fatal/unsupported-sasl", err: fmt.Errorf("wrapped: %w", kerr.UnsupportedSaslMechanism), want: driver.KindFatal},
		{name: "fatal/illegal-sasl-state", err: fmt.Errorf("wrapped: %w", kerr.IllegalSaslState), want: driver.KindFatal},
		{name: "fatal/unsupported-version", err: fmt.Errorf("wrapped: %w", kerr.UnsupportedVersion), want: driver.KindFatal},
		{name: "fatal/invalid-request", err: fmt.Errorf("wrapped: %w", kerr.InvalidRequest), want: driver.KindFatal},
		{name: "fatal/security-disabled", err: fmt.Errorf("wrapped: %w", kerr.SecurityDisabled), want: driver.KindFatal},
		{name: "fatal/broker-config", err: fmt.Errorf("wrapped: %w", errBrokerConfig), want: driver.KindFatal},
		{name: "fatal/protocol-response", err: fmt.Errorf("wrapped: %w", errProtocolResponse), want: driver.KindFatal},
		{name: "transient default", err: errors.New("unrecognised Kafka error"), want: driver.KindTransient},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := kafkaErrorKind(tc.err); got != tc.want {
				t.Fatalf("kafkaErrorKind(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestModeResolution(t *testing.T) {
	cases := []struct {
		name    string
		options map[string]string
		want    consumeMode
	}{
		{name: "absent", want: classicMode},
		{name: "auto", options: map[string]string{"kafka.useShareGroups": "auto"}, want: classicMode},
		{name: "never", options: map[string]string{"kafka.useShareGroups": "never"}, want: classicMode},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := resolveMode(tc.options)
			if err != nil {
				t.Fatalf("resolveMode() error = %v", err)
			}
			if got != tc.want {
				t.Fatalf("resolveMode() = %q, want %q", got, tc.want)
			}
		})
	}

	_, err := (Driver{}).Open(context.Background(), driver.Config{
		DriverOptions: map[string]string{"kafka.useShareGroups": "always"},
	})
	if err == nil || !strings.Contains(err.Error(), "share groups mode is not implemented") {
		t.Fatalf("Open(always) error = %v, want unimplemented share groups error", err)
	}

	_, err = (Driver{}).Open(context.Background(), driver.Config{
		DriverOptions: map[string]string{"kafka.useShareGroups": "unexpected"},
	})
	if err == nil || !strings.Contains(err.Error(), "invalid useShareGroups mode") {
		t.Fatalf("Open(unexpected) error = %v, want invalid mode error", err)
	}
}

func TestStaticMembershipResolution(t *testing.T) {
	cases := []struct {
		name    string
		options map[string]string
		want    bool
		wantErr bool
	}{
		{name: "default when missing", options: nil, want: true},
		{name: "empty options map", options: map[string]string{}, want: true},
		{name: "explicit true", options: map[string]string{"kafka.staticMembership": "true"}, want: true},
		{name: "explicit 1", options: map[string]string{"kafka.staticMembership": "1"}, want: true},
		{name: "explicit false", options: map[string]string{"kafka.staticMembership": "false"}, want: false},
		{name: "explicit 0", options: map[string]string{"kafka.staticMembership": "0"}, want: false},
		{name: "invalid boolean string", options: map[string]string{"kafka.staticMembership": "invalid"}, wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := resolveStaticMembership(tc.options)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("resolveStaticMembership(%v) wanted error, got nil", tc.options)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveStaticMembership(%v) unexpected error: %v", tc.options, err)
			}
			if got != tc.want {
				t.Fatalf("resolveStaticMembership(%v) = %v, want %v", tc.options, got, tc.want)
			}
		})
	}
}

func TestBalancerResolution(t *testing.T) {
	cases := []struct {
		name        string
		value       string
		want        string
		cooperative bool
		lane        bool
	}{
		{name: "default", want: "lane", lane: true},
		{name: "lane", value: "lane", want: "lane", lane: true},
		{name: "cooperative sticky", value: "cooperative-sticky", want: "cooperative-sticky", cooperative: true},
		{name: "sticky", value: "sticky", want: "sticky"},
		{name: "range", value: "range", want: "range"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			options := map[string]string{}
			if tc.value != "" {
				options["kafka.balancer"] = tc.value
			}
			balancer, err := resolveBalancer(options)
			if err != nil {
				t.Fatalf("resolveBalancer(%q) error = %v", tc.value, err)
			}
			if got := balancer.ProtocolName(); got != tc.want {
				t.Fatalf("resolveBalancer(%q) protocol = %q, want %q", tc.value, got, tc.want)
			}
			if got := balancer.IsCooperative(); got != tc.cooperative {
				t.Fatalf("resolveBalancer(%q) cooperative = %v, want %v", tc.value, got, tc.cooperative)
			}
			if tc.lane {
				if _, ok := balancer.(*laneBalancer); !ok {
					t.Fatalf("resolveBalancer(%q) type = %T, want *laneBalancer", tc.value, balancer)
				}
			}
		})
	}

	const invalid = "not-a-balancer"
	_, err := (Driver{}).Open(context.Background(), driver.Config{
		DriverOptions: map[string]string{"kafka.balancer": invalid},
	})
	if err == nil || !strings.Contains(err.Error(), "kafka.balancer") || !strings.Contains(err.Error(), invalid) {
		t.Fatalf("Open(%q) error = %v, want kafka.balancer and value", invalid, err)
	}
	var classified *driver.Error
	if !errors.As(err, &classified) || classified.Kind() != driver.KindFatal {
		t.Fatalf("Open(%q) error = %T/%v, want fatal classified error", invalid, err, err)
	}
}

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
		RequireDurableAck: true, Effective: connection.Capabilities(),
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
