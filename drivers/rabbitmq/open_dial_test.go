package rabbitmq

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
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

func TestCanceledSilentDialClosesSocketAndLeavesNoGoroutine(t *testing.T) {
	for _, scheme := range []string{"amqp", "amqps"} {
		t.Run(scheme, func(t *testing.T) {
			server, cancel, dialed := startSilentDial(t, scheme, 5*time.Second)
			cancel()
			wait := clock.NewReal().Timer(time.Second)
			defer wait.Stop()
			select {
			case err := <-dialed:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("dial() error = %v, want context.Canceled", err)
				}
			case <-wait.C:
				t.Fatal("dial did not return after cancellation")
			}
			assertSilentDialClosed(t, server)
		})
	}
}

func TestSilentDialHonorsConnectTimeout(t *testing.T) {
	for _, scheme := range []string{"amqp", "amqps"} {
		t.Run(scheme, func(t *testing.T) {
			server, _, dialed := startSilentDial(t, scheme, 200*time.Millisecond)
			wait := clock.NewReal().Timer(time.Second)
			defer wait.Stop()
			select {
			case err := <-dialed:
				if err == nil {
					t.Fatal("dial() error = nil, want a handshake timeout")
				}
			case <-wait.C:
				t.Fatal("dial did not return within 1s for a 200ms connect timeout")
			}
			assertSilentDialClosed(t, server)
		})
	}
}

func startSilentDial(t *testing.T, scheme string, timeout time.Duration) (net.Conn, context.CancelFunc, <-chan error) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr == nil {
			accepted <- conn
		}
		close(accepted)
	}()
	ctx, cancel := context.WithCancel(context.Background())
	dialed := make(chan error, 1)
	go func() {
		defer close(dialed)
		conn, dialErr := dial(ctx, scheme+"://guest:guest@"+listener.Addr().String()+"/",
			amqp.Config{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12}}, timeout)
		if conn != nil {
			_ = conn.CloseDeadline(clock.NewReal().Now().Add(time.Second))
		}
		dialed <- dialErr
	}()
	var server net.Conn
	t.Cleanup(func() {
		_ = listener.Close()
		cancel()
		if server != nil {
			_ = server.Close()
		}
		// A failing-before run must release the stalled handshake too.
		for conn := range accepted {
			_ = conn.Close()
		}
		wait := clock.NewReal().Timer(2 * time.Second)
		defer wait.Stop()
		select {
		case <-dialed:
		case <-wait.C:
			t.Error("dial did not exit during test cleanup")
		}
		waitForDialGoroutines(t)
	})
	wait := clock.NewReal().Timer(2 * time.Second)
	defer wait.Stop()
	select {
	case server = <-accepted:
		if server == nil {
			t.Fatal("listener closed before accepting the dial")
		}
	case <-wait.C:
		t.Fatal("the dial never reached the silent listener")
	}
	return server, cancel, dialed
}

func assertSilentDialClosed(t *testing.T, server net.Conn) {
	t.Helper()
	if err := server.SetReadDeadline(clock.NewReal().Now().Add(500 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	// Drain the AMQP header or TLS ClientHello before looking for peer closure.
	_, err := io.Copy(io.Discard, server)
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		t.Fatal("silent peer socket remained open after dial returned")
	}
	waitForDialGoroutines(t)
}

func waitForDialGoroutines(t *testing.T) {
	t.Helper()
	wait := clock.NewReal().Timer(time.Second)
	defer wait.Stop()
	poll := clock.NewReal().Ticker(5 * time.Millisecond)
	defer poll.Stop()
	for dialGoroutines() > 0 {
		select {
		case <-wait.C:
			t.Errorf("%d dial goroutine(s) still running", dialGoroutines())
			return
		case <-poll.C:
		}
	}
}
