package kafka

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

// franz-go v1.21.6 is pinned for the Kafka 4.2.1 driver.
// Later phases depend on its fetch, pause, commit, and rebalance behavior.

var _ driver.Driver = Driver{}

// Driver is a stateless Kafka driver factory.
type Driver struct{}

// conn will own live franz-go clients once Open is implemented.
type conn struct {
	client *kgo.Client
}

// Name returns the stable Kafka driver key.
func (Driver) Name() string {
	return "kafka"
}

// Capabilities reports the ceiling across classic and share-group modes.
func (Driver) Capabilities() driver.Capabilities {
	return driver.Capabilities{
		PerMessageAck:       true,
		OrderedByKey:        true,
		NativeDeliveryCount: true,
		NativeDLQ:           false,
		ConsumerScaling:     driver.ScalingFree,
		Fanout:              driver.FanoutAtConsume,
		LagQueryable:        true,
	}
}

// Open is intentionally unfinished in this first learning checkpoint.
func (Driver) Open(context.Context, driver.Config) (driver.Conn, error) {
	return nil, driver.ErrUnsupported
}
