package f1otel

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/semconv/v1.43.0/messagingconv"
)

type metrics struct {
	sent      *messagingconv.ClientSentMessages
	consumed  *messagingconv.ClientConsumedMessages
	operation *messagingconv.ClientOperationDuration
	process   *messagingconv.ProcessDuration

	brokerWait         metric.Float64Histogram
	backlogMessages    metric.Int64Gauge
	backlogOldestAge   metric.Float64Gauge
	deadlinePromotions metric.Int64Counter
	laneWait           metric.Float64Histogram
	retries            metric.Int64Counter
	deadLetters        metric.Int64Counter
}

func newMetrics(meter metric.Meter) *metrics {
	out := &metrics{}
	if instrument, err := messagingconv.NewClientSentMessages(meter); err != nil {
		otel.Handle(err)
	} else {
		out.sent = &instrument
	}
	if instrument, err := messagingconv.NewClientConsumedMessages(meter); err != nil {
		otel.Handle(err)
	} else {
		out.consumed = &instrument
	}
	if instrument, err := messagingconv.NewClientOperationDuration(meter); err != nil {
		otel.Handle(err)
	} else {
		out.operation = &instrument
	}
	if instrument, err := messagingconv.NewProcessDuration(meter); err != nil {
		otel.Handle(err)
	} else {
		out.process = &instrument
	}

	var err error
	if out.brokerWait, err = meter.Float64Histogram("f1.messaging.broker.wait.duration", metric.WithUnit("s")); err != nil {
		otel.Handle(err)
	}
	if out.backlogMessages, err = meter.Int64Gauge("f1.messaging.backlog.messages"); err != nil {
		otel.Handle(err)
	}
	if out.backlogOldestAge, err = meter.Float64Gauge("f1.messaging.backlog.oldest.age", metric.WithUnit("s")); err != nil {
		otel.Handle(err)
	}
	if out.deadlinePromotions, err = meter.Int64Counter("f1.messaging.deadline.promotions"); err != nil {
		otel.Handle(err)
	}
	if out.laneWait, err = meter.Float64Histogram("f1.messaging.lane.wait.duration", metric.WithUnit("s")); err != nil {
		otel.Handle(err)
	}
	if out.retries, err = meter.Int64Counter("f1.messaging.retries"); err != nil {
		otel.Handle(err)
	}
	if out.deadLetters, err = meter.Int64Counter("f1.messaging.dead_letters"); err != nil {
		otel.Handle(err)
	}
	return out
}
