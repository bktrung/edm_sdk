package rabbitmq

import (
	"context"
	"crypto/tls"
	"net"
	"runtime"
	"strings"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

// dialGoroutines counts goroutines still running a closure of dial.
func dialGoroutines() int {
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	return strings.Count(string(buf[:n]), "rabbitmq.dial.func")
}

func TestCanceledDialThatFailsLaterLeavesNoGoroutine(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr == nil {
			accepted <- conn
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	dialed := make(chan error, 1)
	go func() {
		_, dialErr := dial(ctx, "amqp://guest:guest@"+listener.Addr().String()+"/", amqp.Config{}, 5*time.Second)
		dialed <- dialErr
	}()
	var server net.Conn
	select {
	case server = <-accepted:
	case <-clock.NewReal().Timer(2 * time.Second).C:
		t.Fatal("the dial never reached the listener")
	}
	cancel()
	if err := <-dialed; err == nil {
		t.Fatal("dial() error = nil, want the cancellation")
	}
	// The dial fails only now, after the caller gave up on it.
	_ = server.Close()

	deadline := clock.NewReal().Timer(2 * time.Second)
	defer deadline.Stop()
	poll := clock.NewReal().Ticker(5 * time.Millisecond)
	defer poll.Stop()
	for dialGoroutines() > 0 {
		select {
		case <-deadline.C:
			t.Fatalf("%d dial goroutine(s) still running after the dial failed", dialGoroutines())
		case <-poll.C:
		}
	}
}

func TestOpenRefusesUnreadableTLSMaterialAsFatal(t *testing.T) {
	cfg := driver.Config{
		Endpoints:      []string{"amqps://127.0.0.1:1/"},
		ConnectTimeout: 2 * time.Second,
		TLS:            &driver.TLSConfig{Enabled: true, CAFile: t.TempDir() + "/missing-ca.pem"},
	}
	_, err := Driver{}.Open(context.Background(), cfg)
	kind, classified := driver.Classify(err)
	if !classified || kind != driver.KindFatal {
		t.Fatalf("Open() error = %v (kind %v, classified %v), want a fatal configuration error", err, kind, classified)
	}
}

// TestDialLeavesTheSharedTLSConfigUntouched dials a TLS endpoint that drops
// the connection. amqp091 fills an empty ServerName with the host it dials,
// and Open shares one TLS config across its endpoints, so a dial that wrote
// into it would check every later endpoint against the first one's name.
func TestDialLeavesTheSharedTLSConfigUntouched(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		for {
			conn, acceptErr := listener.Accept()
			if acceptErr != nil {
				return
			}
			_ = conn.Close()
		}
	}()
	shared := &tls.Config{MinVersion: tls.VersionTLS12}
	_, err = dial(context.Background(), "amqps://"+listener.Addr().String()+"/", amqp.Config{TLSClientConfig: shared}, time.Second)
	if err == nil {
		t.Fatal("dial() error = nil, want a failed handshake")
	}
	if shared.ServerName != "" {
		t.Fatalf("shared TLS config ServerName = %q after one dial, want it left empty", shared.ServerName)
	}
}
