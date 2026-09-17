package kafka

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

func TestConsumerBuildChecksRebalanceTimeout(t *testing.T) {
	const explicitDrainTimeout = 250 * time.Millisecond

	cases := []struct {
		name             string
		drainTimeout     time.Duration
		rebalanceTimeout time.Duration
		wantErr          bool
	}{
		{
			name:             "equal explicit revoke wait bound",
			drainTimeout:     explicitDrainTimeout,
			rebalanceTimeout: explicitDrainTimeout,
			wantErr:          true,
		},
		{
			name:             "below default revoke wait bound",
			rebalanceTimeout: defaultKafkaRebalanceDrainTimeout - time.Second,
			wantErr:          true,
		},
		{
			name:             "above explicit revoke wait bound",
			drainTimeout:     explicitDrainTimeout,
			rebalanceTimeout: explicitDrainTimeout + time.Second,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bound := tc.drainTimeout
			if bound == 0 {
				bound = defaultKafkaRebalanceDrainTimeout
			}
			connection := &conn{
				clientOpts: []kgo.Opt{noDialKafkaOption()},
				driverOptions: map[string]string{
					"kafka.rebalanceTimeout": tc.rebalanceTimeout.String(),
				},
				rebalanceDrainTimeout: tc.drainTimeout,
			}

			value, err := connection.Consumer(context.Background(), driver.ConsumerConfig{
				Group:        "rebalance-timeout-test",
				Destinations: []string{"topic"},
				Prefetch:     1,
			})
			if tc.wantErr {
				if value != nil {
					if built, ok := value.(*consumer); !ok || built != nil {
						t.Fatalf("Consumer() value = %T, want nil *consumer", value)
					}
				}
				if err == nil {
					t.Fatal("Consumer() error = nil, want rebalance-timeout refusal")
				}
				for _, want := range []string{
					"kafka.rebalanceTimeout",
					tc.rebalanceTimeout.String(),
					bound.String(),
				} {
					if !strings.Contains(err.Error(), want) {
						t.Errorf("Consumer() error = %q, want %q", err, want)
					}
				}
				var classified *driver.Error
				if !errors.As(err, &classified) {
					t.Fatalf("Consumer() error = %T, want *driver.Error", err)
				}
				if got := classified.Kind(); got != driver.KindFatal {
					t.Fatalf("Consumer() error kind = %v, want %v", got, driver.KindFatal)
				}
				return
			}

			if err != nil {
				t.Fatalf("Consumer() error = %v, want nil", err)
			}
			built, ok := value.(*consumer)
			if !ok {
				t.Fatalf("Consumer() value = %T, want *consumer", value)
			}
			stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			if err := built.Stop(stopCtx); err != nil {
				t.Fatalf("Stop() error = %v", err)
			}
			select {
			case <-built.stopDone:
			default:
				t.Fatal("Stop() returned before consumer stopped")
			}
		})
	}
}
