package kafka

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

// TestResolveBalancerProtocols pins the protocol name and the cooperative flag
// of every accepted value of kafka.balancer, and that an unset option resolves
// to the cooperative default. The protocol a member advertises is what decides
// whether its group rebalances cooperatively at all, so a value that silently
// resolved to an eager balancer would remove the behaviour this driver ships
// without changing any configuration.
func TestResolveBalancerProtocols(t *testing.T) {
	cases := []struct {
		name        string
		value       string
		want        string
		cooperative bool
	}{
		{name: "default when unset", want: "cooperative-sticky", cooperative: true},
		{name: "cooperative-sticky", value: "cooperative-sticky", want: "cooperative-sticky", cooperative: true},
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
		})
	}
}

// TestResolveBalancerRefusesEmptyValue pins that naming the option with nothing
// in it is refused rather than taken as the cooperative default. An empty value
// is a config that meant to set the protocol and did not, and joining with the
// default on one is how a group gets moved to a protocol nobody selected: the
// value the option carries is resolved, and only an absent option defaults.
func TestResolveBalancerRefusesEmptyValue(t *testing.T) {
	_, err := resolveBalancer(map[string]string{"kafka.balancer": ""})
	if err == nil {
		t.Fatal("resolveBalancer with an empty kafka.balancer resolved a balancer, want a refusal naming the accepted values")
	}
	if !strings.Contains(err.Error(), "kafka.balancer") {
		t.Fatalf("refusal %v does not name the option", err)
	}
	for _, accepted := range []string{"cooperative-sticky", "sticky", "range"} {
		if !strings.Contains(err.Error(), accepted) {
			t.Fatalf("refusal %v does not name the accepted value %q", err, accepted)
		}
	}
}

// TestDriverOpenRefusesLaneBalancer pins that the protocol this driver used to
// advertise is refused rather than mapped onto a stock balancer, that the
// refusal names every accepted value, and that Open is what raises it.
//
// Both halves matter. Members of one group have to advertise protocols that
// intersect or the group cannot form at all, so a fleet still configured with
// the old value has to be told which values it may move to. And the refusal is
// only useful before the broker is touched: Open resolves the balancer ahead of
// its dial, which is what lets this test run with no fixture.
func TestDriverOpenRefusesLaneBalancer(t *testing.T) {
	for _, refused := range []string{"lane", "not-a-balancer"} {
		t.Run(refused, func(t *testing.T) {
			_, err := (Driver{}).Open(context.Background(), driver.Config{
				DriverOptions: map[string]string{"kafka.balancer": refused},
			})
			if err == nil {
				t.Fatalf("Open(kafka.balancer=%q) error = nil, want a refusal", refused)
			}
			if !strings.Contains(err.Error(), "kafka.balancer") || !strings.Contains(err.Error(), refused) {
				t.Fatalf("Open(kafka.balancer=%q) error = %v, want it to name the option and the value", refused, err)
			}
			for _, accepted := range []string{"cooperative-sticky", "sticky", "range"} {
				if !strings.Contains(err.Error(), accepted) {
					t.Fatalf("Open(kafka.balancer=%q) error = %v, want it to name the accepted value %q", refused, err, accepted)
				}
			}
			var classified *driver.Error
			if !errors.As(err, &classified) || classified.Kind() != driver.KindFatal {
				t.Fatalf("Open(kafka.balancer=%q) error = %T/%v, want fatal classified error", refused, err, err)
			}
		})
	}
}

// TestTopologyRefusesDestinationBelowMaxExpectedInstances pins the partition
// floor: a destination that would be created, or already exists, with fewer
// partitions than the instances the deployment expects cannot seat them all, so
// topology setup refuses it rather than creating a destination that silently
// caps consumer scaling.
func TestTopologyRefusesDestinationBelowMaxExpectedInstances(t *testing.T) {
	const (
		topic     = "low"
		instances = "3"
	)
	admin := &admin{conn: &conn{driverOptions: map[string]string{
		"kafka.maxExpectedInstances": instances,
	}}}
	_, err := admin.EnsureTopology(context.Background(), driver.TopologySpec{
		Destinations: []driver.DestinationSpec{{Name: topic, Partitions: 2}},
	})
	if err == nil || !strings.Contains(err.Error(), topic) || !strings.Contains(err.Error(), "maxExpectedInstances") {
		t.Fatalf("EnsureTopology underpartitioned destination error = %v, want topic and maxExpectedInstances", err)
	}
	var classified *driver.Error
	if !errors.As(err, &classified) || classified.Kind() != driver.KindFatal {
		t.Fatalf("EnsureTopology underpartitioned destination error = %T/%v, want fatal classified error", err, err)
	}

	if got, err := resolveMaxExpectedInstances(nil); err != nil || got != 0 {
		t.Fatalf("resolveMaxExpectedInstances(nil) = %d/%v, want 0/nil", got, err)
	}
}

func TestConsumerClientOptsUsesConfiguredBalancer(t *testing.T) {
	connection := &conn{
		clientOpts: []kgo.Opt{noDialKafkaOption()},
		balancer:   kgo.RangeBalancer(),
	}
	options, err := consumerClientOpts(connection, driver.ConsumerConfig{Destinations: []string{"topic"}}, "group", nil)
	if err != nil {
		t.Fatalf("consumerClientOpts error = %v", err)
	}
	client, err := kgo.NewClient(options...)
	if err != nil {
		t.Fatalf("NewClient error = %v", err)
	}
	defer client.Close()

	values := client.OptValues(kgo.Balancers)
	if len(values) != 1 {
		t.Fatalf("kgo.Balancers values = %v, want one configured value", values)
	}
	balancers, ok := values[0].([]kgo.GroupBalancer)
	if !ok || len(balancers) != 1 || balancers[0].ProtocolName() != "range" {
		t.Fatalf("kgo.Balancers value = %#v, want range balancer", values[0])
	}
}
