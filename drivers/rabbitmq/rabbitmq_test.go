package rabbitmq

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
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
	if caps.NativePriority != driver.PriorityNone || caps.NativePriorityLevels != 0 {
		t.Fatalf("priority capabilities = %#v, want none: the driver applies no AMQP priority", caps)
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

// TestQuorumQueueKindReportsDurabilityUpgrade proves the upgrade a quorum
// queue kind forces on a non-durable destination is visible. Quorum queues are
// durable by definition, so the declare cannot honor a non-durable request, and
// the connection reports the difference instead of leaving the caller to
// discover it from the broker: once per destination, naming it.
func TestQuorumQueueKindReportsDurabilityUpgrade(t *testing.T) {
	if declared, _, _ := queueFlags(false, queueKindQuorum); !declared {
		t.Fatal("queueFlags(non-durable, quorum) is not durable, want the kind to force durability")
	}

	var logged bytes.Buffer
	c := &conn{queueKind: queueKindQuorum, logger: slog.New(slog.NewTextHandler(&logged, nil))}
	c.reportDurabilityUpgrade("orders")
	c.reportDurabilityUpgrade("orders")
	c.reportDurabilityUpgrade("payments")

	lines := strings.Split(strings.TrimSpace(logged.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("durability upgrade reported %d times, want one per destination: %q", len(lines), logged.String())
	}
	for index, destination := range []string{"orders", "payments"} {
		if !strings.Contains(lines[index], destination) {
			t.Fatalf("durability upgrade for %q = %q, want the destination named", destination, lines[index])
		}
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

func TestValidateEndpointRejectsHostlessEndpoint(t *testing.T) {
	err := validateEndpoint("amqp:///orders")
	if err == nil {
		t.Fatal("validateEndpoint() error = nil, want unsupported host-less endpoint error")
	}
	if !strings.Contains(err.Error(), "without a host is unsupported") {
		t.Fatalf("validateEndpoint() error = %v, want unsupported-spelling detail", err)
	}
	if !strings.Contains(err.Error(), "amqp://localhost/orders") {
		t.Fatalf("validateEndpoint() error = %v, want explicit-host example", err)
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
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			opened := make(chan struct{})
			go func() {
				defer close(opened)
				_, _ = (Driver{}).Open(ctx, driver.Config{Endpoints: []string{endpoint}, TLS: test.tls})
			}()

			// The dial is the proof; stop the handshake retries once it lands.
			select {
			case <-accepted:
			case <-opened:
				if !receivedOpenAttempt(accepted) {
					t.Fatal("Open() refused the amqps endpoint before dialing")
				}
			}
			cancel()
			<-opened
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

// TestCloseBlame covers which messages a channel's close is the broker
// refusing, with no broker and no channel in the test. The size rows are the
// shapes the refusal race produces: the message in front of the refused one is
// the one a quorum queue can leave unconfirmed, a segment can hold more than one
// message over the limit, and a message exactly at the limit is publishable.
// The missing-exchange rows use the reason the broker was measured to send,
// including a name that carries quotes of its own, and prove a message routed
// through the default exchange by queue name is never blamed.
func TestCloseBlame(t *testing.T) {
	const limit = 1 << 10
	sizeClose := classifyPublishClose(&amqp.Error{
		Code:   406,
		Server: true,
		Reason: fmt.Sprintf("PRECONDITION_FAILED - message size %d is larger than configured max size %d", limit+1, limit),
	})
	missingClose := func(exchange string) error {
		return classifyPublishClose(&amqp.Error{Code: 404, Server: true, Reason: "NOT_FOUND - no exchange '" + exchange + "' in vhost '/'"})
	}
	body := func(length int) outboundMessage {
		return outboundMessage{routingKey: "queue", publishing: amqp.Publishing{Body: make([]byte, length)}}
	}
	to := func(exchange string) outboundMessage { return outboundMessage{exchange: exchange} }
	for _, test := range []struct {
		name     string
		close    error
		messages []outboundMessage
		want     []bool
		named    bool
	}{
		{
			name:     "size: the message in front of the refusal is unconfirmed",
			close:    sizeClose,
			messages: []outboundMessage{body(limit / 2), body(limit + 1)},
			want:     []bool{false, true},
			named:    true,
		},
		{
			name:     "size: two oversized messages",
			close:    sizeClose,
			messages: []outboundMessage{body(limit + 1), body(limit / 2), body(limit + 1)},
			want:     []bool{true, false, true},
			named:    true,
		},
		{
			name:     "size: a body exactly at the limit",
			close:    sizeClose,
			messages: []outboundMessage{body(limit)},
			want:     []bool{false},
			named:    true,
		},
		{
			name:     "missing exchange: only messages to it",
			close:    missingClose("orders"),
			messages: []outboundMessage{to("orders"), to("orders.v2"), to("order"), to("")},
			want:     []bool{true, false, false, false},
			named:    true,
		},
		{
			name:     "missing exchange: a name with quotes",
			close:    missingClose("it's 'quoted'"),
			messages: []outboundMessage{to("it's 'quoted'"), to("it")},
			want:     []bool{true, false},
			named:    true,
		},
		{
			name:     "a close that names no message",
			close:    classifyPublishClose(&amqp.Error{Code: 320, Server: true, Reason: "CONNECTION_FORCED - broker forced connection closure"}),
			messages: []outboundMessage{to("orders")},
		},
		{
			name:     "a close without a reason",
			close:    classify("publish", driver.KindTransient, amqp.ErrClosed),
			messages: []outboundMessage{body(limit + 1)},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			blamed, named := closeBlame(test.close)
			if named != test.named {
				t.Fatalf("closeBlame(%v) named = %t, want %t", test.close, named, test.named)
			}
			if !named {
				return
			}
			for index, message := range test.messages {
				if got := blamed(message); got != test.want[index] {
					t.Fatalf("message %d (exchange %q, %d bytes) blamed = %t, want %t", index, message.exchange, len(message.publishing.Body), got, test.want[index])
				}
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

func TestOpenRejectsMismatchedVhostBeforeDialing(t *testing.T) {
	for _, later := range []bool{false, true} {
		t.Run(fmt.Sprintf("later endpoint=%t", later), func(t *testing.T) {
			listener, accepted := listenerForOpenAttempt(t)
			address := listener.Addr().String()
			endpoints := []string{"amqp://redaction-user:redaction-secret@" + address + "/billing"}
			if later {
				endpoints = append([]string{"amqp://" + address + "/orders"}, endpoints...)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
			defer cancel()
			connection, err := (Driver{}).Open(ctx, driver.Config{
				Endpoints: endpoints, ConnectTimeout: 200 * time.Millisecond,
				DriverOptions: map[string]string{"rabbitmq.vhost": "orders"},
			})
			assertFatalOpenRefusal(t, connection, err, accepted)
			for _, detail := range []string{"orders", "billing"} {
				if !strings.Contains(err.Error(), detail) {
					t.Errorf("vhost refusal does not name %s", detail)
				}
			}
		})
	}
}

func TestOpenRejectsSASLCredentialsWithoutMechanism(t *testing.T) {
	for _, settings := range []driver.SASLConfig{
		{Username: "redaction-user"},
		{Password: "redaction-secret"},
		{Username: "redaction-user", Password: "redaction-secret"},
	} {
		listener, accepted := listenerForOpenAttempt(t)
		ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
		connection, err := (Driver{}).Open(ctx, driver.Config{
			Endpoints: []string{"amqp://" + listener.Addr().String() + "/"},
			SASL:      &settings, ConnectTimeout: 200 * time.Millisecond,
		})
		cancel()
		assertFatalOpenRefusal(t, connection, err, accepted)
		if !strings.Contains(err.Error(), "mechanism") {
			t.Error("credential refusal does not ask for a mechanism")
		}
	}
}

func assertFatalOpenRefusal(t *testing.T, connection driver.Conn, err error, accepted <-chan struct{}) {
	t.Helper()
	var classified *driver.Error
	if connection != nil || !errors.As(err, &classified) || classified.Driver != "rabbitmq" || classified.Op != "open" || classified.K != driver.KindFatal {
		t.Fatalf("Open did not return a fatal rabbitmq/open configuration refusal: %v", err)
	}
	if receivedOpenAttempt(accepted) {
		t.Error("Open dialed before refusing the configuration")
	}
	for _, secret := range []string{"redaction-user", "redaction-secret", "amqp://"} {
		if strings.Contains(err.Error(), secret) {
			t.Error("Open refusal disclosed endpoint credentials")
		}
	}
}

// assertOpenSASL observes connection.start-ok on the socket Open actually dials.
// Stopping after authentication avoids inventing a broker's topology behavior.
func assertOpenSASL(t *testing.T, settings *driver.SASLConfig) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	deadline, _ := ctx.Deadline()
	if err := listener.(*net.TCPListener).SetDeadline(deadline); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		_, openErr := (Driver{}).Open(ctx, driver.Config{
			Endpoints: []string{"amqp://uri-user:uri-secret@" + listener.Addr().String() + "/"},
			SASL:      settings, ConnectTimeout: time.Second,
		})
		result <- openErr
	}()
	socket, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer socket.Close()
	if err := socket.SetDeadline(deadline); err != nil {
		t.Fatal(err)
	}
	header := make([]byte, 8)
	if _, err := io.ReadFull(socket, header); err != nil {
		t.Fatal(err)
	}
	if _, err := socket.Write(connectionStartFrame()); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(socket, header[:7]); err != nil {
		t.Fatal(err)
	}
	payload := make([]byte, int(binary.BigEndian.Uint32(header[3:7]))+1)
	if _, err := io.ReadFull(socket, payload); err != nil {
		t.Fatal(err)
	}
	cancel()
	<-result
	// start-ok is class/method, client-properties table, mechanism shortstr,
	// response longstr, then locale shortstr.
	offset := 8 + int(binary.BigEndian.Uint32(payload[4:8]))
	mechanismLength := int(payload[offset])
	mechanism := string(payload[offset+1 : offset+1+mechanismLength])
	offset += 1 + mechanismLength
	responseLength := int(binary.BigEndian.Uint32(payload[offset : offset+4]))
	response := string(payload[offset+4 : offset+4+responseLength])
	var expected amqp.Authentication
	switch strings.ToLower(settings.Mechanism) {
	case "plain":
		expected = &amqp.PlainAuth{Username: settings.Username, Password: settings.Password}
	case "amqplain":
		// AMQPLAIN is a field table; map iteration makes its byte order vary.
		fields := bytes.NewReader([]byte(response))
		identity := make(map[string]string, 2)
		for fields.Len() > 0 {
			keyLength, err := fields.ReadByte()
			if err != nil {
				t.Fatal("invalid AMQPLAIN field name")
			}
			key := make([]byte, keyLength)
			if _, err := io.ReadFull(fields, key); err != nil {
				t.Fatal("invalid AMQPLAIN field name")
			}
			fieldType, err := fields.ReadByte()
			if err != nil || fieldType != 'S' {
				t.Fatal("AMQPLAIN credential is not a long string")
			}
			var valueLength uint32
			if err := binary.Read(fields, binary.BigEndian, &valueLength); err != nil || int64(valueLength) > int64(fields.Len()) {
				t.Fatal("invalid AMQPLAIN field length")
			}
			value := make([]byte, valueLength)
			if _, err := io.ReadFull(fields, value); err != nil {
				t.Fatal("invalid AMQPLAIN field value")
			}
			identity[string(key)] = string(value)
		}
		if mechanism != "AMQPLAIN" || len(identity) != 2 || identity["LOGIN"] != settings.Username || identity["PASSWORD"] != settings.Password {
			t.Error("Open AMQPLAIN authentication does not use the configured identity")
		}
		return
	case "external":
		expected = &amqp.ExternalAuth{}
	}
	if mechanism != expected.Mechanism() || response != expected.Response() {
		t.Error("Open AMQP authentication does not use the configured SASL mechanism and identity")
	}
}
