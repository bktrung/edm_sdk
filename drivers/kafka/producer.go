package kafka

import (
	"context"
	"errors"

	"github.com/twmb/franz-go/pkg/kgo"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

// delayUntilHeader is reserved for deferred Kafka delivery. It is driver-internal,
// and the consume path must strip it before the message reaches the application.
const delayUntilHeader = "x-f1-delay-until"

type producer struct {
	client *kgo.Client
}

var _ driver.Producer = (*producer)(nil)

func (p *producer) Publish(ctx context.Context, msgs ...driver.OutboundMessage) error {
	if err := ctx.Err(); err != nil {
		return classify("publish", driver.KindTransient, err)
	}
	if len(msgs) == 0 {
		return nil
	}
	for _, msg := range msgs {
		if msg.EntryPoint {
			return classify("publish", driver.KindFatal, errors.New("kafka: entry-point publishing is unsupported"))
		}
	}

	records := make([]*kgo.Record, len(msgs))
	for i, msg := range msgs {
		record := &kgo.Record{
			Topic:   msg.Destination,
			Key:     append([]byte(nil), msg.Key...),
			Value:   append([]byte(nil), msg.Body...),
			Headers: make([]kgo.RecordHeader, len(msg.Headers)),
		}
		for j, header := range msg.Headers {
			record.Headers[j] = kgo.RecordHeader{
				Key:   header.Key,
				Value: append([]byte(nil), header.Value...),
			}
		}
		records[i] = record
	}

	results := p.client.ProduceSync(ctx, records...)
	failed, err := failedPublishIndexes(records, results)
	if err != nil {
		return classify("publish", driver.KindFatal, err)
	}
	if len(failed) != 0 {
		return &driver.PublishError{Failed: failed}
	}
	return nil
}

func failedPublishIndexes(records []*kgo.Record, results kgo.ProduceResults) (map[int]error, error) {
	indexes := make(map[*kgo.Record]int, len(records))
	for index, record := range records {
		indexes[record] = index
	}

	failed := make(map[int]error)
	accounted := make(map[int]struct{}, len(records))
	for _, result := range results {
		index, ok := indexes[result.Record]
		if !ok {
			return nil, errors.New("kafka: produce result references an unknown record")
		}
		if _, ok := accounted[index]; ok {
			return nil, errors.New("kafka: duplicate produce result for input record")
		}
		accounted[index] = struct{}{}
		if result.Err == nil {
			continue
		}
		kind := kafkaErrorKind(result.Err)
		cause := result.Err
		if kind == driver.KindNotFound {
			cause = errors.Join(driver.ErrDestinationMissing, result.Err)
		}
		failed[index] = classify("publish", kind, cause)
	}
	if len(accounted) != len(records) {
		return nil, errors.New("kafka: produce results omitted an input record")
	}
	return failed, nil
}

func (p *producer) Flush(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return classify("flush", driver.KindTransient, err)
	}
	if err := p.client.Flush(ctx); err != nil {
		return classify("flush", kafkaErrorKind(err), err)
	}
	return nil
}

func (p *producer) Close(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return classify("producer.close", driver.KindTransient, err)
	}
	// The Kafka client belongs to conn and is closed by Conn.Close after all
	// resources have been closed. A Producer only borrows that client.
	return nil
}
