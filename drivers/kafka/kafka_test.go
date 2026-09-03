package kafka

import (
	"context"
	"net"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
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
	defer client.Close()
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
	defer opened.Close(context.Background())
	caps := opened.Capabilities()
	if caps.MaxMessageBytes != wantMessageBytes {
		t.Errorf("MaxMessageBytes = %d, want independently read %d", caps.MaxMessageBytes, wantMessageBytes)
	}
	if caps.MaxHeaderBytes != 0 {
		t.Errorf("MaxHeaderBytes = %d, want 0", caps.MaxHeaderBytes)
	}
}
