package f1

import (
	"reflect"
	"sort"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

func TestSubscriptionTopologyMatchesFanoutModes(t *testing.T) {
	source := "/prod/orders"
	sub := Subscription{
		Name:       "order-worker",
		Topics:     []string{"order.created"},
		Priorities: []Priority{PriorityHigh, PriorityMedium},
		Retry: RetryConfig{
			MaxAttempts: 3,
			Tiers:       []time.Duration{time.Second, 5 * time.Second},
		},
	}

	wantRetry := []string{
		"f1.prod.order.created.order-worker.high.retry.1",
		"f1.prod.order.created.order-worker.high.retry.2",
		"f1.prod.order.created.order-worker.medium.retry.1",
		"f1.prod.order.created.order-worker.medium.retry.2",
	}
	wantDLQ := "f1.prod.order.created.dlq.order-worker"
	wantBackstop := wantDLQ + ".backstop"
	wantUnknownDLQ := "f1.prod.unknown.dlq.order-worker"

	tests := []struct {
		name         string
		mode         driver.FanoutMode
		nativeDLQ    bool
		main         []string
		wantExchange bool
	}{
		{
			name: "consume",
			mode: driver.FanoutAtConsume,
			main: []string{
				"f1.prod.order.created.high",
				"f1.prod.order.created.medium",
			},
		},
		{
			name:      "consume_native_dlq",
			mode:      driver.FanoutAtConsume,
			nativeDLQ: true,
			main: []string{
				"f1.prod.order.created.high",
				"f1.prod.order.created.medium",
			},
		},
		{
			name: "publish",
			mode: driver.FanoutAtPublish,
			main: []string{
				"f1.prod.order.created.order-worker.high",
				"f1.prod.order.created.order-worker.medium",
			},
			wantExchange: true,
		},
		{
			name:      "publish_native_dlq",
			mode:      driver.FanoutAtPublish,
			nativeDLQ: true,
			main: []string{
				"f1.prod.order.created.order-worker.high",
				"f1.prod.order.created.order-worker.medium",
			},
			wantExchange: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			effective := driver.Capabilities{Fanout: test.mode, NativeDLQ: test.nativeDLQ}
			spec := subscriptionTopologySpecs(effective, source, sub)
			// The backstop receives what the broker dead-lettered on its own
			// delivery counter. A broker that cannot do that must not be asked
			// to create the queue, or it creates one that can never fill.
			wantLimit := 0
			wantRoute := ""
			if test.nativeDLQ {
				wantLimit = 8
				wantRoute = wantBackstop
			}
			wantSubscribed := append(append([]string(nil), test.main...), wantRetry...)
			sort.Strings(wantSubscribed)
			if got := subscriptionDestinations(effective, source, sub); !reflect.DeepEqual(got, wantSubscribed) {
				t.Fatalf("subscribed destinations = %v, want %v", got, wantSubscribed)
			}

			wantNames := append(append([]string(nil), test.main...), wantRetry...)
			wantNames = append(wantNames, wantDLQ, wantUnknownDLQ)
			if test.nativeDLQ {
				wantNames = append(wantNames, wantBackstop)
			}
			sort.Strings(wantNames)
			if got := destinationNames(spec.Destinations); !reflect.DeepEqual(got, wantNames) {
				t.Fatalf("declared destinations = %v, want %v", got, wantNames)
			}
			for _, name := range test.main {
				assertDestination(t, spec, name, driver.DestMain, wantRoute, wantLimit)
			}
			for _, name := range wantRetry {
				assertDestination(t, spec, name, driver.DestRetry, wantRoute, wantLimit)
			}
			assertDestination(t, spec, wantDLQ, driver.DestDLQ, "", 0)
			if test.nativeDLQ {
				assertDestination(t, spec, wantBackstop, driver.DestBackstopDLQ, "", 0)
			}
			assertDestination(t, spec, wantUnknownDLQ, driver.DestDLQ, "", 0)

			if test.wantExchange {
				wantExchanges := []string{
					"f1.prod.order.created.high",
					"f1.prod.order.created.medium",
				}
				if got := exchangeNames(spec.Exchanges); !reflect.DeepEqual(got, wantExchanges) {
					t.Fatalf("declared exchanges = %v, want %v", got, wantExchanges)
				}
				if len(spec.Bindings) != len(wantExchanges) {
					t.Fatalf("bindings = %d, want %d", len(spec.Bindings), len(wantExchanges))
				}
				for i, source := range wantExchanges {
					if spec.Bindings[i] != (driver.BindingSpec{Source: source, Destination: test.main[i]}) {
						t.Fatalf("binding[%d] = %+v, want source=%q destination=%q", i, spec.Bindings[i], source, test.main[i])
					}
				}
			} else if len(spec.Exchanges) != 0 || len(spec.Bindings) != 0 {
				t.Fatalf("consume topology has exchanges=%v bindings=%v", spec.Exchanges, spec.Bindings)
			}

			wantScope := []string{
				"f1.prod.order.created.dlq.order-worker.",
				"f1.prod.order.created.order-worker.",
			}
			if !reflect.DeepEqual(spec.Scope, wantScope) {
				t.Fatalf("scope = %v, want %v", spec.Scope, wantScope)
			}
		})
	}
}

func assertDestination(t *testing.T, spec driver.TopologySpec, name string, kind driver.DestKind, backstop string, limit int) {
	t.Helper()
	var found *driver.DestinationSpec
	for i := range spec.Destinations {
		if spec.Destinations[i].Name == name {
			found = &spec.Destinations[i]
			break
		}
	}
	if found == nil {
		t.Fatalf("destination %q was not declared", name)
	}
	if found.Kind != kind || !found.Durable {
		t.Fatalf("destination %q = kind %v durable=%t, want kind %v durable=true", name, found.Kind, found.Durable, kind)
	}
	if found.DeliveryLimit != limit {
		t.Fatalf("destination %q delivery limit = %d, want %d", name, found.DeliveryLimit, limit)
	}
	if backstop == "" {
		if found.DeadLetter != nil {
			t.Fatalf("destination %q has unexpected dead-letter route %+v", name, found.DeadLetter)
		}
		return
	}
	if found.DeadLetter == nil || found.DeadLetter.Exchange != "" || found.DeadLetter.Key != backstop {
		t.Fatalf("destination %q route = %+v, want default route to %q", name, found.DeadLetter, backstop)
	}
}

func destinationNames(destinations []driver.DestinationSpec) []string {
	result := make([]string, 0, len(destinations))
	for _, destination := range destinations {
		result = append(result, destination.Name)
	}
	sort.Strings(result)
	return result
}

func exchangeNames(exchanges []driver.ExchangeSpec) []string {
	result := make([]string, 0, len(exchanges))
	for _, exchange := range exchanges {
		result = append(result, exchange.Name)
	}
	sort.Strings(result)
	return result
}
