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

func assertFatalOpenError(t *testing.T, err error, wantText string) {
	t.Helper()
	if err == nil {
		t.Fatal("Open() error = nil, want classified fatal error")
	}
	if !strings.Contains(err.Error(), wantText) {
		t.Fatalf("Open() error = %v, want text %q", err, wantText)
	}
	var classified *driver.Error
	if !errors.As(err, &classified) {
		t.Fatalf("Open() error = %T, want *driver.Error", err)
	}
	if classified.Op != "open" {
		t.Errorf("Open() error Op = %q, want open", classified.Op)
	}
	if classified.Kind() != driver.KindFatal {
		t.Errorf("Open() error Kind = %v, want fatal", classified.Kind())
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
