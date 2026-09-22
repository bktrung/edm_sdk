//go:build integration

package kafka

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

func TestOpenRejectsSessionTimeoutBelowBrokerMinimum(t *testing.T) {
	requireBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	client, err := kgo.NewClient(kgo.SeedBrokers(kafkaEndpoint))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(client.Close)
	admin := kadm.NewClient(client)
	metadata, err := admin.Metadata(ctx)
	if err != nil {
		t.Fatalf("Metadata: %v", err)
	}
	configs, err := admin.DescribeBrokerConfigs(ctx, metadata.Controller)
	if err != nil {
		t.Fatalf("DescribeBrokerConfigs: %v", err)
	}

	var minimumMillis int64
	for _, resource := range configs {
		if resource.Err != nil {
			t.Fatalf("broker config resource: %v", resource.Err)
		}
		for _, config := range resource.Configs {
			if config.Key != "group.min.session.timeout.ms" {
				continue
			}
			minimumMillis, err = strconv.ParseInt(config.MaybeValue(), 10, 64)
			if err != nil {
				t.Fatalf("group.min.session.timeout.ms = %q: %v", config.MaybeValue(), err)
			}
		}
	}
	if minimumMillis <= 0 {
		t.Fatal("broker config did not return group.min.session.timeout.ms")
	}
	minimum := time.Duration(minimumMillis) * time.Millisecond
	configured := minimum - time.Millisecond
	if configured <= 0 {
		t.Fatalf("broker group.min.session.timeout.ms = %s, cannot choose a lower positive timeout", minimum)
	}
	t.Logf("broker group.min.session.timeout.ms = %s; testing kafka.sessionTimeout = %s", minimum, configured)

	opened, err := (Driver{}).Open(ctx, driver.Config{
		Endpoints: []string{kafkaEndpoint},
		DriverOptions: map[string]string{
			"kafka.sessionTimeout": configured.String(),
		},
	})
	if err == nil {
		_ = opened.Close(context.Background())
		t.Fatalf("Open accepted kafka.sessionTimeout=%s below broker group.min.session.timeout.ms=%s", configured, minimum)
	}
	for _, want := range []string{
		"kafka.sessionTimeout",
		configured.String(),
		"group.min.session.timeout.ms",
		minimum.String(),
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("Open error = %v, want %q", err, want)
		}
	}
	var classified *driver.Error
	if !errors.As(err, &classified) || classified.Kind() != driver.KindFatal {
		t.Fatalf("Open error = %T/%v, want a fatal driver error", err, err)
	}
}
