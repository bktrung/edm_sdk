package rabbitmq

import (
	"context"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

type producer struct{}

var _ driver.Producer = (*producer)(nil)

func (p *producer) Publish(ctx context.Context, _ ...driver.OutboundMessage) error {
	if err := ctx.Err(); err != nil {
		return classify("publish", driver.KindTransient, err)
	}
	return classify("publish", driver.KindFatal, driver.ErrUnsupported)
}

func (p *producer) Flush(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return classify("flush", driver.KindTransient, err)
	}
	return classify("flush", driver.KindFatal, driver.ErrUnsupported)
}

func (p *producer) Close(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return classify("producer.close", driver.KindTransient, err)
	}
	return classify("producer.close", driver.KindFatal, driver.ErrUnsupported)
}
