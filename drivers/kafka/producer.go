package kafka

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

// delayUntilHeader is reserved for deferred Kafka delivery. It is driver-internal,
// and the consume path must strip it before the message reaches the application.
const delayUntilHeader = "x-f1-delay-until"

type producer struct {
	client     *kgo.Client
	conn       *conn
	cfg        driver.ProducerConfig
	clock      clock.Clock
	mu         sync.Mutex
	closed     bool
	closing    bool
	active     int
	activeDone chan struct{}
}

func (p *producer) beginOperation(operation string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || p.closing {
		return classify(operation, driver.KindTransient, errors.New("kafka: producer is closed"))
	}
	p.active++
	if p.activeDone == nil {
		p.activeDone = make(chan struct{})
	}
	return nil
}

func (p *producer) endOperation() {
	p.mu.Lock()
	p.active--
	if p.active == 0 {
		close(p.activeDone)
		p.activeDone = nil
	}
	p.mu.Unlock()
}

var _ driver.Producer = (*producer)(nil)

func (p *producer) Publish(ctx context.Context, msgs ...driver.OutboundMessage) error {
	if err := ctx.Err(); err != nil {
		return classify("publish", driver.KindTransient, err)
	}
	if len(msgs) == 0 {
		return nil
	}
	if err := p.beginOperation("publish"); err != nil {
		return err
	}
	defer p.endOperation()
	if p.conn != nil {
		if raw := p.conn.publishFault.Swap(0); raw != 0 {
			kind := driver.Kind(raw - 1)
			failed := make(map[int]error, len(msgs))
			for index := range msgs {
				failed[index] = classify("publish", kind, errors.New("injected publish fault"))
			}
			return &driver.PublishError{Failed: failed}
		}
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
	if err := p.beginOperation("flush"); err != nil {
		return err
	}
	defer p.endOperation()
	if err := p.client.Flush(ctx); err != nil {
		return classify("flush", kafkaErrorKind(err), err)
	}
	return nil
}

// defaultKafkaBatchLinger is franz-go's own producer linger default. It is
// passed explicitly rather than left unset so that the value in force is the
// driver's, and so a reader can see it without reading the client library.
const defaultKafkaBatchLinger = 10 * time.Millisecond

// resolveProducerOptions translates the broker.kafka.* keys that govern the
// produce path into franz-go options. It returns an option for every knob,
// including the ones the operator left unset, so the produce settings in force
// never depend on a client library default the driver did not choose.
func resolveProducerOptions(options map[string]string) ([]kgo.Opt, error) {
	codecs, err := resolveCompression(options)
	if err != nil {
		return nil, err
	}
	linger, err := resolveBatchLinger(options)
	if err != nil {
		return nil, err
	}
	return []kgo.Opt{
		kgo.ProducerBatchCompression(codecs...),
		kgo.ProducerLinger(linger),
	}, nil
}

// resolveCompression maps kafka.compression onto a franz-go codec preference.
// A named codec stands alone: adding a fallback would let the producer publish
// an encoding the configuration does not describe, and a batch encoded with a
// codec the operator did not choose is a surprise the operator cannot see.
//
// The unset default is franz-go's own preference, snappy with an uncompressed
// fallback, so an operator who sets nothing keeps the behaviour they had.
func resolveCompression(options map[string]string) ([]kgo.CompressionCodec, error) {
	value, ok := options["kafka.compression"]
	if !ok {
		return []kgo.CompressionCodec{kgo.SnappyCompression(), kgo.NoCompression()}, nil
	}
	switch strings.ToLower(value) {
	case "none":
		return []kgo.CompressionCodec{kgo.NoCompression()}, nil
	case "gzip":
		return []kgo.CompressionCodec{kgo.GzipCompression()}, nil
	case "snappy":
		return []kgo.CompressionCodec{kgo.SnappyCompression()}, nil
	case "lz4":
		return []kgo.CompressionCodec{kgo.Lz4Compression()}, nil
	case "zstd":
		return []kgo.CompressionCodec{kgo.ZstdCompression()}, nil
	default:
		return nil, fmt.Errorf("kafka: invalid kafka.compression %q; supported codecs are none, gzip, snappy, lz4, zstd", value)
	}
}

// resolveBatchLinger maps kafka.batchLinger onto the producer's linger. Zero is
// valid and disables lingering, which franz-go expresses as a zero duration. A
// negative duration is refused rather than clamped to zero: an operator who
// mistypes the key gets the key named back, not a producer that silently never
// lingers.
func resolveBatchLinger(options map[string]string) (time.Duration, error) {
	value, ok := options["kafka.batchLinger"]
	if !ok {
		return defaultKafkaBatchLinger, nil
	}
	linger, err := time.ParseDuration(value)
	if err != nil || linger < 0 {
		return 0, fmt.Errorf("kafka: invalid kafka.batchLinger %q; must be a non-negative duration", value)
	}
	return linger, nil
}

func (p *producer) Close(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return classify("producer.close", driver.KindTransient, err)
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closing = true
	done := p.activeDone
	p.mu.Unlock()

	if done != nil {
		select {
		case <-done:
		case <-ctx.Done():
			return classify("producer.close", driver.KindTransient, ctx.Err())
		}
	}

	p.mu.Lock()
	if !p.closed {
		p.closed = true
	}
	p.mu.Unlock()
	if p.conn != nil {
		p.conn.removeProducer(p)
	}
	return nil
}
