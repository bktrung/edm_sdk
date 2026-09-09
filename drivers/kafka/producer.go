package kafka

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

// delayUntilHeader is reserved for deferred Kafka delivery. It is driver-internal,
// and the consume path must strip it before the message reaches the application.
const delayUntilHeader = "x-f1-delay-until"

type producer struct {
	client *kgo.Client
	conn   *conn
	cfg    driver.ProducerConfig
	clock  clock.Clock
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

	failed := make(map[int]error)
	records := make([]*kgo.Record, 0, len(msgs))
	recordIndexes := make([]int, 0, len(msgs))
	for i, msg := range msgs {
		if p.cfg.Effective.MaxMessageBytes > 0 && len(msg.Body) > p.cfg.Effective.MaxMessageBytes {
			failed[i] = classify("publish", driver.KindTooLarge, fmt.Errorf("message body exceeds MaxMessageBytes (%d)", p.cfg.Effective.MaxMessageBytes))
			continue
		}
		delay, known := p.destinationDelay(msg.Destination)
		records = append(records, recordForMessage(msg, delay, known, p.currentTime()))
		recordIndexes = append(recordIndexes, i)
	}
	if len(records) == 0 {
		return &driver.PublishError{Failed: failed}
	}

	results := p.client.ProduceSync(ctx, records...)
	brokerFailed, err := failedPublishIndexes(records, results)
	if err != nil {
		return classify("publish", driver.KindFatal, err)
	}
	for index, cause := range brokerFailed {
		failed[recordIndexes[index]] = cause
	}
	if len(failed) != 0 {
		return &driver.PublishError{Failed: failed}
	}
	return nil
}

func (p *producer) destinationDelay(destination string) (time.Duration, bool) {
	if p.conn == nil {
		return 0, false
	}
	return p.conn.destinationDelay(destination)
}

func (p *producer) currentTime() time.Time {
	return p.clock.Now()
}

func recordForMessage(msg driver.OutboundMessage, destinationDelay time.Duration, known bool, now time.Time) *kgo.Record {
	record := &kgo.Record{
		Topic:     msg.Destination,
		Key:       append([]byte(nil), msg.Key...),
		Value:     append([]byte(nil), msg.Body...),
		Timestamp: now,
		Headers:   make([]kgo.RecordHeader, 0, len(msg.Headers)+1),
	}
	for _, header := range msg.Headers {
		if header.Key == delayUntilHeader {
			continue
		}
		record.Headers = append(record.Headers, kgo.RecordHeader{
			Key:   header.Key,
			Value: append([]byte(nil), header.Value...),
		})
	}
	if due := outboundDue(msg.DelayUntil, destinationDelay, known, now); !due.IsZero() {
		record.Headers = append(record.Headers, kgo.RecordHeader{
			Key:   delayUntilHeader,
			Value: []byte(strconv.FormatInt(due.UnixNano(), 10)),
		})
	}
	return record
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
