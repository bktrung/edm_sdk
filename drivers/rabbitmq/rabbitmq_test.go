package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"net"
	"slices"
	"strings"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

func TestDriverCapabilities(t *testing.T) {
	caps := Driver{}.Capabilities()
	if !caps.PerMessageAck || caps.NativeDelay || !caps.NativeDLQ {
		t.Fatalf("capabilities = %#v, want per-message ack, no native delay, and native DLQ", caps)
	}
	if caps.NativePriority != driver.PriorityStrict || caps.NativePriorityLevels != 32 {
		t.Fatalf("priority capabilities = %#v, want strict 32 levels", caps)
	}
	if caps.Fanout != driver.FanoutAtPublish || !caps.OrderedByKey {
		t.Fatalf("routing capabilities = %#v, want publish fanout and ordered keys", caps)
	}
}

func TestCopyBrokerInfoClonesExtra(t *testing.T) {
	source := driver.BrokerInfo{
		Extra: map[string]string{"region": "saigon"},
	}

	got := copyBrokerInfo(source)
	if got.Extra["region"] != "saigon" {
		t.Errorf("copyBrokerInfo().Extra[region] = %q, want saigon", got.Extra["region"])
	}

	got.Extra["region"] = "singapore"
	if source.Extra["region"] != "saigon" {
		t.Errorf("mutating copied Extra changed source: got %q, want saigon", source.Extra["region"])
	}
}

func TestConfiguredQueueKind(t *testing.T) {
	cases := []struct {
		name    string
		options map[string]string
		want    queueKind
		ok      bool
	}{
		{name: "default", want: queueKindQuorum, ok: true},
		{name: "quorum", options: map[string]string{"rabbitmq.queueType": "quorum"}, want: queueKindQuorum, ok: true},
		{name: "classic", options: map[string]string{"rabbitmq.queueType": "classic"}, want: queueKindClassic, ok: true},
		{name: "case and spaces", options: map[string]string{"rabbitmq.queueType": " CLASSIC "}, want: queueKindClassic, ok: true},
		{name: "unsupported", options: map[string]string{"rabbitmq.queueType": "stream"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := configuredQueueKind(tc.options)
			if tc.ok {
				if err != nil || got != tc.want {
					t.Fatalf("configuredQueueKind() = %q, %v, want %q", got, err, tc.want)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), "quorum, classic") {
				t.Fatalf("configuredQueueKind() error = %v, want supported queue list", err)
			}
		})
	}
}

func TestQueueKindCapabilities(t *testing.T) {
	quorum := capabilitiesForQueueKind(queueKindQuorum)
	if !quorum.NativeDeliveryCount {
		t.Fatal("quorum capabilities NativeDeliveryCount = false, want true")
	}
	if !quorum.NativeDLQ {
		t.Fatal("quorum capabilities NativeDLQ = false, want true")
	}
	classic := capabilitiesForQueueKind(queueKindClassic)
	if classic.NativeDeliveryCount {
		t.Fatal("classic capabilities NativeDeliveryCount = true, want false")
	}
	if classic.NativeDLQ {
		t.Fatal("classic capabilities NativeDLQ = true, want false")
	}
}

func TestExpirationMillisClampsLargeDelay(t *testing.T) {
	if got := expirationMillis(100 * 365 * 24 * time.Hour); got != "2147483647" {
		t.Fatalf("expirationMillis(100 years) = %q, want 2147483647", got)
	}
}

func TestOpenRejectsSCRAM(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := (Driver{}).Open(ctx, driver.Config{Endpoints: []string{defaultEndpoint}, SASL: &driver.SASLConfig{Mechanism: "scram-sha-256"}})
	if err == nil {
		t.Fatal("Open() error = nil, want unsupported SASL error")
	}
	if !strings.Contains(err.Error(), "PLAIN, AMQPLAIN, EXTERNAL") {
		t.Fatalf("Open() error = %v, want supported mechanism list", err)
	}
	if kind, ok := driver.Classify(err); !ok || kind != driver.KindFatal {
		t.Fatalf("Classify(Open error) = %v, %t, want fatal, true", kind, ok)
	}
}

func TestOpenRejectsEmptyEndpoints(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err := (Driver{}).Open(ctx, driver.Config{})
	if err == nil || !errors.Is(err, errMissingEndpoints) {
		t.Fatalf("Open() error = %v, want named empty-endpoint error", err)
	}
	if kind, ok := driver.Classify(err); !ok || kind != driver.KindFatal {
		t.Fatalf("Classify(Open error) = %v, %t, want fatal, true", kind, ok)
	}
}

func TestOpenRefusesPlaintextRemoteBeforeDialing(t *testing.T) {
	listener, accepted := listenerForOpenAttempt(t)
	endpoint := fmt.Sprintf("amqp://0.0.0.0:%d/", listener.Addr().(*net.TCPAddr).Port)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := (Driver{}).Open(ctx, driver.Config{Endpoints: []string{endpoint}, ConnectTimeout: time.Second})
	if err == nil {
		t.Fatal("Open() error = nil, want plaintext endpoint refusal")
	}
	if receivedOpenAttempt(accepted) {
		t.Fatal("Open() attempted a connection before refusing the plaintext remote endpoint")
	}
	if !strings.Contains(err.Error(), "amqps://") {
		t.Fatalf("Open() error = %v, want plaintext endpoint refusal", err)
	}
}

func TestOpenAllowsAmqpsEndpoint(t *testing.T) {
	for _, test := range []struct {
		name string
		tls  *driver.TLSConfig
	}{
		{name: "without TLS block"},
		{name: "with TLS block", tls: &driver.TLSConfig{Enabled: true}},
	} {
		t.Run(test.name, func(t *testing.T) {
			listener, accepted := listenerForOpenAttempt(t)
			endpoint := fmt.Sprintf("amqps://0.0.0.0:%d/", listener.Addr().(*net.TCPAddr).Port)
			ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
			defer cancel()

			_, _ = (Driver{}).Open(ctx, driver.Config{Endpoints: []string{endpoint}, TLS: test.tls})
			if !receivedOpenAttempt(accepted) {
				t.Fatal("Open() refused the amqps endpoint before dialing")
			}
		})
	}
}

func listenerForOpenAttempt(t *testing.T) (net.Listener, <-chan struct{}) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen() error = %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	accepted := make(chan struct{}, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		accepted <- struct{}{}
		_ = conn.Close()
	}()
	return listener, accepted
}

func receivedOpenAttempt(accepted <-chan struct{}) bool {
	select {
	case <-accepted:
		return true
	case <-time.After(100 * time.Millisecond): //nolint:forbidigo // bounded socket acceptance observation
		return false
	}
}

func TestClassifyAMQPNotFound(t *testing.T) {
	err := classifyAMQP("consume", driver.KindTransient, &amqp.Error{Code: 404, Reason: "not found"})
	if !errors.Is(err, driver.ErrDestinationMissing) {
		t.Fatalf("error = %v, want ErrDestinationMissing", err)
	}
	kind, ok := driver.Classify(err)
	if !ok || kind != driver.KindNotFound {
		t.Fatalf("Classify(%v) = %v, %t, want not_found, true", err, kind, ok)
	}
}

// TestClassifyAMQPTransportLoss covers the close codes amqp091-go raises on the
// client when the transport dies. The library marks those with Server false,
// and the caller's fallback cannot decide them: the Qos, Confirm and topology
// paths pass fatal, so a connection lost while a consumer reopens would end the
// subscription. A code in the same range that the broker sent stays fatal,
// because the broker sends those for protocol misuse by the client.
func TestClassifyAMQPTransportLoss(t *testing.T) {
	for _, test := range []struct {
		name        string
		err         error
		fallback    driver.Kind
		want        driver.Kind
		wantMissing bool
	}{
		{
			name:     "client frame error with a fatal fallback",
			err:      &amqp.Error{Code: 501, Reason: "connection reset by peer"},
			fallback: driver.KindFatal,
			want:     driver.KindTransient,
		},
		{
			name:     "client channel error with a fatal fallback",
			err:      amqp.ErrClosed,
			fallback: driver.KindFatal,
			want:     driver.KindTransient,
		},
		{
			name:     "client channel error with a not found fallback",
			err:      amqp.ErrClosed,
			fallback: driver.KindNotFound,
			want:     driver.KindTransient,
		},
		{
			name:     "wrapped client channel error",
			err:      fmt.Errorf("publish: %w", amqp.ErrClosed),
			fallback: driver.KindFatal,
			want:     driver.KindTransient,
		},
		{
			name:     "client syntax error with a fatal fallback",
			err:      amqp.ErrSyntax,
			fallback: driver.KindFatal,
			want:     driver.KindTransient,
		},
		{
			name:     "client command invalid with a fatal fallback",
			err:      amqp.ErrCommandInvalid,
			fallback: driver.KindFatal,
			want:     driver.KindTransient,
		},
		{
			name:     "server frame error with a transient fallback",
			err:      &amqp.Error{Code: 501, Reason: "frame could not be parsed", Server: true},
			fallback: driver.KindTransient,
			want:     driver.KindFatal,
		},
		{
			name:     "server channel error with a transient fallback",
			err:      &amqp.Error{Code: 504, Reason: "channel error", Server: true},
			fallback: driver.KindTransient,
			want:     driver.KindFatal,
		},
		{
			name:     "server connection forced",
			err:      &amqp.Error{Code: 320, Reason: "CONNECTION_FORCED", Server: true},
			fallback: driver.KindTransient,
			want:     driver.KindTransient,
		},
		{
			name:        "destination missing",
			err:         &amqp.Error{Code: 404, Reason: "NOT_FOUND"},
			fallback:    driver.KindTransient,
			want:        driver.KindNotFound,
			wantMissing: true,
		},
		{
			name:     "client credentials refusal",
			err:      amqp.ErrCredentials,
			fallback: driver.KindTransient,
			want:     driver.KindPermission,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := classifyAMQP("consume", test.fallback, test.err)
			kind, ok := driver.Classify(err)
			if !ok || kind != test.want {
				t.Fatalf("Classify(%v) = %v, %t, want %v, true", err, kind, ok, test.want)
			}
			if got := errors.Is(err, driver.ErrDestinationMissing); got != test.wantMissing {
				t.Fatalf("errors.Is(%v, ErrDestinationMissing) = %t, want %t", err, got, test.wantMissing)
			}
		})
	}
}

// TestSizeRefusalLimit covers the reply the size refusal is told apart by, and
// the number the window compares a body against. The code alone cannot decide
// it: the broker answers every publish precondition it fails with 406, so a
// message whose expiration it cannot parse is refused the same way, and only
// the reason separates an oversized message from that one. Both wordings of the
// refusal are here, because which one a broker sends is not something this
// driver chooses, and the limit is the number after "max size" rather than the
// one after "message size".
func TestSizeRefusalLimit(t *testing.T) {
	for _, test := range []struct {
		name      string
		err       error
		wantLimit int
		want      bool
	}{
		{
			name: "broker on a configured limit",
			err: &amqp.Error{
				Code:   406,
				Server: true,
				Reason: "PRECONDITION_FAILED - message size 16777217 is larger than configured max size 16777216",
			},
			wantLimit: 16777216,
			want:      true,
		},
		{
			name: "broker on a built-in limit",
			err: &amqp.Error{
				Code:   406,
				Server: true,
				Reason: "PRECONDITION_FAILED - message size 536870913 is larger than max size 536870912",
			},
			wantLimit: 536870912,
			want:      true,
		},
		{
			name: "another publish precondition",
			err: &amqp.Error{
				Code:   406,
				Server: true,
				Reason: "PRECONDITION_FAILED - invalid expiration 'tomorrow'",
			},
		},
		{
			name: "the phrases without a limit",
			err: &amqp.Error{
				Code:   406,
				Server: true,
				Reason: "PRECONDITION_FAILED - message size 16777217 is larger than max size",
			},
		},
		{
			name: "the same words under another code",
			err: &amqp.Error{
				Code:   404,
				Server: true,
				Reason: "NOT_FOUND - message size 16777217 is larger than configured max size 16777216",
			},
		},
		{
			name: "the client lost the connection",
			err:  amqp.ErrClosed,
		},
		{
			name: "the broker forced the connection closed",
			err: &amqp.Error{
				Code:   320,
				Server: true,
				Reason: "CONNECTION_FORCED - broker forced connection closure with reason 'shutdown'",
			},
		},
		{
			name: "the broker refused a frame",
			err: &amqp.Error{
				Code:   501,
				Server: true,
				Reason: "FRAME_ERROR - type 2, all octets = <<>>: {frame_too_large,200027,131064}",
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			limit, refused := sizeRefusalLimit(test.err)
			if refused != test.want {
				t.Fatalf("sizeRefusalLimit(%v) = %d, %t, want %t", test.err, limit, refused, test.want)
			}
			if limit != test.wantLimit {
				t.Fatalf("sizeRefusalLimit(%v) limit = %d, want %d", test.err, limit, test.wantLimit)
			}
		})
	}
}

// TestSizeRefusalKinds covers the decision a window makes once the broker has
// closed its channel for the size of one of its messages, with no broker and no
// channel in the test. The rows are the shapes that race produces: the message
// in front of the refused one is the one a quorum queue can leave unconfirmed,
// a window can hold more than one message over the limit, and a message exactly
// at the limit is publishable.
func TestSizeRefusalKinds(t *testing.T) {
	const limit = 1 << 10
	small := limit / 2
	over := limit + 1
	for _, test := range []struct {
		name    string
		lengths []int
		decided int
		want    []driver.Kind
	}{
		{
			name:    "the message in front of the refusal is unconfirmed",
			lengths: []int{small, over},
			decided: 0,
			want:    []driver.Kind{driver.KindTransient, driver.KindTooLarge},
		},
		{
			name:    "two oversized messages in one window",
			lengths: []int{over, small, over},
			decided: 0,
			want:    []driver.Kind{driver.KindTooLarge, driver.KindTransient, driver.KindTooLarge},
		},
		{
			name:    "only the last message is oversized",
			lengths: []int{small, small, over},
			decided: 0,
			want:    []driver.Kind{driver.KindTransient, driver.KindTransient, driver.KindTooLarge},
		},
		{
			name:    "a body exactly at the limit",
			lengths: []int{limit},
			decided: 0,
			want:    []driver.Kind{driver.KindTransient},
		},
		{
			name:    "the messages the window already decided",
			lengths: []int{small, over, over},
			decided: 1,
			want:    []driver.Kind{driver.KindTooLarge, driver.KindTooLarge},
		},
		{
			name:    "every message of the window decided",
			lengths: []int{small, over},
			decided: 2,
			want:    []driver.Kind{},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := sizeRefusalKinds(test.lengths, test.decided, limit)
			if !slices.Equal(got, test.want) {
				t.Fatalf("sizeRefusalKinds(%v, %d, %d) = %v, want %v", test.lengths, test.decided, limit, got, test.want)
			}
		})
	}
}

// TestClassifyPublishClose covers what a publish channel that closed is
// reported as. The size refusal is the message's own failure, and the broker's
// own words for it survive into the error the caller reads. Every other close
// is the transient failure a closed publish channel has always been, including
// the server-sent frame error: transient is what makes the core take the
// connection down and build it again, which is the only thing that recovers a
// connection the broker refused a frame on, and the broker's reason for it
// survives there too.
func TestClassifyPublishClose(t *testing.T) {
	for _, test := range []struct {
		name       string
		err        error
		want       driver.Kind
		wantReason string
	}{
		{
			name: "size refusal",
			err: &amqp.Error{
				Code:   406,
				Server: true,
				Reason: "PRECONDITION_FAILED - message size 16777217 is larger than configured max size 16777216",
			},
			want:       driver.KindTooLarge,
			wantReason: "message size 16777217 is larger than configured max size 16777216",
		},
		{
			name: "another publish precondition",
			err: &amqp.Error{
				Code:   406,
				Server: true,
				Reason: "PRECONDITION_FAILED - invalid expiration 'tomorrow'",
			},
			want: driver.KindTransient,
		},
		{
			name: "client lost connection",
			err:  amqp.ErrClosed,
			want: driver.KindTransient,
		},
		{
			name: "broker forced the connection closed",
			err: &amqp.Error{
				Code:   320,
				Server: true,
				Reason: "CONNECTION_FORCED - broker forced connection closure with reason 'shutdown'",
			},
			want: driver.KindTransient,
		},
		{
			name: "broker refused a frame",
			err: &amqp.Error{
				Code:   501,
				Server: true,
				Reason: "FRAME_ERROR - type 2, all octets = <<>>: {frame_too_large,200027,131064}",
			},
			want:       driver.KindTransient,
			wantReason: "frame_too_large,200027,131064",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := classifyPublishClose(test.err)
			kind, ok := driver.Classify(err)
			if !ok || kind != test.want {
				t.Fatalf("Classify(%v) = %v, %t, want %v, true", err, kind, ok, test.want)
			}
			if test.wantReason != "" && !strings.Contains(err.Error(), test.wantReason) {
				t.Fatalf("error = %v, want the broker's reason %q to survive", err, test.wantReason)
			}
		})
	}
}

// TestConfirmOrCloseServesAQueuedConfirmation pins the interleaving a refused
// publish leaves behind: the client closes the return stream before the confirm
// stream, so a confirmation it had already delivered can be sitting behind the
// close. That confirmation is the broker saying it published the message the
// window is waiting for, and reporting the close instead would report a message
// the broker published as one the close covers.
//
// The second half is the same channel once those confirmations are gone: no
// confirmation is left for the message being waited for, so the close is what
// the window reads. The close is classified too large here, which is what the
// limit it carries makes it; which message of the window that covers is decided
// by the window, not by this path.
func TestConfirmOrCloseServesAQueuedConfirmation(t *testing.T) {
	closedReturns := make(chan amqp.Return)
	close(closedReturns)
	channel := &publishChannel{
		confirms: make(chan amqp.Confirmation, 1),
		returns:  closedReturns,
		closes:   make(chan *amqp.Error, 1),
	}
	channel.closes <- &amqp.Error{
		Code:   406,
		Server: true,
		Reason: "PRECONDITION_FAILED - message size 16777217 is larger than configured max size 16777216",
	}
	channel.confirms <- amqp.Confirmation{DeliveryTag: 1, Ack: true}

	confirmation, err := channel.confirmOrClose(nil)
	if err != nil {
		t.Fatalf("confirmOrClose() error = %v, want the confirmation that was already delivered", err)
	}
	if !confirmation.Ack || confirmation.DeliveryTag != 1 {
		t.Fatalf("confirmOrClose() = %+v, want the confirmation that was already delivered", confirmation)
	}

	_, err = channel.confirmOrClose(nil)
	kind, classified := driver.Classify(err)
	if err == nil || !classified || kind != driver.KindTooLarge {
		t.Fatalf("confirmOrClose() error = %v, kind = %v, %t, want the close of a refused message", err, kind, classified)
	}
}

// TestPublishChannelCloseError covers what a channel's close is reported as
// once nothing is left of it but the client's notification: the reason the
// broker gave when there is one, and the closed publish channel this driver has
// always reported when there is not. Both shapes of "no reason" are pinned,
// because only one of them is the state a reader expects the notification to be
// in: the client closes the notification stream whether or not it had something
// to send on it.
func TestPublishChannelCloseError(t *testing.T) {
	for _, test := range []struct {
		name         string
		notification *amqp.Error
		closeStream  bool
		want         driver.Kind
		wantSentinel error
	}{
		{
			name: "the broker refused the message for its size",
			notification: &amqp.Error{
				Code:   406,
				Server: true,
				Reason: "PRECONDITION_FAILED - message size 16777217 is larger than configured max size 16777216",
			},
			closeStream: true,
			want:        driver.KindTooLarge,
		},
		{
			name:         "the client closed the notification without sending one",
			closeStream:  true,
			want:         driver.KindTransient,
			wantSentinel: amqp.ErrClosed,
		},
		{
			name:         "no notification has arrived yet",
			want:         driver.KindTransient,
			wantSentinel: amqp.ErrClosed,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			closes := make(chan *amqp.Error, 1)
			if test.notification != nil {
				closes <- test.notification
			}
			if test.closeStream {
				close(closes)
			}
			err := (&publishChannel{closes: closes}).closeError()
			if kind, ok := driver.Classify(err); !ok || kind != test.want {
				t.Fatalf("Classify(closeError()) = %v, %t, want %v, true", kind, ok, test.want)
			}
			if test.wantSentinel != nil && !errors.Is(err, test.wantSentinel) {
				t.Fatalf("closeError() = %v, want %v", err, test.wantSentinel)
			}
		})
	}
}
