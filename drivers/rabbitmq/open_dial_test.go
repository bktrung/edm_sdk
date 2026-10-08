package rabbitmq

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"runtime"
	"strings"
	"sync/atomic"
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

// credentialRefusingEndpoint starts a broker stand-in that answers the handshake
// up to the client's connection.start-ok and then drops the socket. amqp091
// reports a socket that dies at that point as ErrCredentials, the error a broker
// whose credentials were rejected produces, so every dial to this endpoint is
// refused the same way. The returned function reports the dials it answered.
func credentialRefusingEndpoint(t *testing.T) (string, func() int) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var dials atomic.Int64
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			socket, acceptErr := listener.Accept()
			if acceptErr != nil {
				return
			}
			dials.Add(1)
			go refuseAfterStartOk(socket)
		}
	}()
	return "amqp://guest:guest@" + listener.Addr().String() + "/", func() int { return int(dials.Load()) }
}

// refuseAfterStartOk answers the client's protocol header with connection.start,
// reads the connection.start-ok that carries the credentials, and closes the
// socket in place of connection.tune.
func refuseAfterStartOk(socket net.Conn) {
	defer socket.Close()
	if err := socket.SetDeadline(clock.NewReal().Now().Add(time.Second)); err != nil {
		return
	}
	header := make([]byte, 8)
	if _, err := io.ReadFull(socket, header); err != nil {
		return
	}
	if _, err := socket.Write(connectionStartFrame()); err != nil {
		return
	}
	if _, err := io.ReadFull(socket, header[:7]); err != nil {
		return
	}
	if _, err := io.ReadFull(socket, make([]byte, int(binary.BigEndian.Uint32(header[3:7]))+1)); err != nil {
		return
	}
}

// unreachableLoopbackEndpoint reserves and releases a loopback port, so a dial
// to it is refused at once and the failure is the transient one a retry recovers
// from.
func unreachableLoopbackEndpoint(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return "amqp://guest:guest@" + address + "/"
}

// connectionStartFrame is the connection.start frame amqp091's handshake waits
// for before it sends its credentials in connection.start-ok. A broker stand-in
// has to produce this one frame before it can refuse or accept a connection.
func connectionStartFrame() []byte {
	const mechanisms = "PLAIN AMQPLAIN EXTERNAL"
	const locale = "en_US"
	const length = uint32(10 + 4 + len(mechanisms) + 4 + len(locale))
	payload := []byte{0, 10, 0, 10, 0, 9, 0, 0, 0, 0}
	payload = binary.BigEndian.AppendUint32(payload, uint32(len(mechanisms)))
	payload = append(payload, mechanisms...)
	payload = binary.BigEndian.AppendUint32(payload, uint32(len(locale)))
	payload = append(payload, locale...)
	frame := binary.BigEndian.AppendUint32([]byte{1, 0, 0}, length)
	frame = append(frame, payload...)
	return append(frame, 0xce)
}

// TestOpenGivesUpOnlyAfterTheRefusalRepeats covers a broker that rejects the
// credentials of every endpoint on every attempt. amqp091 answers a socket that
// dies during the credential exchange with the same refusal, so one refused pass
// could be a dropped handshake, and Open has to be refused openRefusalPasses
// passes in a row before it reports the refusal.
func TestOpenGivesUpOnlyAfterTheRefusalRepeats(t *testing.T) {
	retryWait := openRetryWait
	openRetryWait = func(context.Context) error { return nil }
	t.Cleanup(func() { openRetryWait = retryWait })
	endpoint, dials := credentialRefusingEndpoint(t)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_, err := (Driver{}).Open(ctx, driver.Config{Endpoints: []string{endpoint}})
	kind, classified := driver.Classify(err)
	if !classified || kind != driver.KindPermission {
		t.Fatalf("Open() error = %v (kind %v, classified %t), want a permission refusal", err, kind, classified)
	}
	if !errors.Is(err, amqp.ErrCredentials) {
		t.Fatalf("Open() error = %v, want amqp.ErrCredentials in the chain", err)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("Open() waited for the caller's deadline instead of giving up on the refusal")
	}
	// The budget is 20 passes and this dials once per pass. The test names the
	// number instead of the constant so that it compiles against a revision
	// without the budget and fails on the count rather than on a build.
	const wantDials = 20
	if got := dials(); got != wantDials {
		t.Fatalf("Open dialed %d time(s), want %d: the refusal must repeat before it is reported", got, wantDials)
	}
	for _, secret := range []string{"guest:guest", "amqp://"} {
		if strings.Contains(err.Error(), secret) {
			t.Errorf("Open() refusal disclosed the endpoint: %v", err)
		}
	}
}

// TestOpenJoinsTheBrokerRefusalIntoATimeout covers a pass in which one endpoint
// refuses the credentials and another fails transiently. Open keeps trying for
// the endpoint that may recover and ends on the connect timeout, and the error
// it ends with has to name both that timeout and the refusal, which no retry
// repairs.
func TestOpenJoinsTheBrokerRefusalIntoATimeout(t *testing.T) {
	retryWait := openRetryWait
	openRetryWait = func(context.Context) error { return context.DeadlineExceeded }
	t.Cleanup(func() { openRetryWait = retryWait })
	refusing, _ := credentialRefusingEndpoint(t)
	_, err := (Driver{}).Open(context.Background(), driver.Config{
		Endpoints:      []string{refusing, unreachableLoopbackEndpoint(t)},
		ConnectTimeout: 300 * time.Millisecond,
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Open() error = %v, want the connect timeout in the chain", err)
	}
	if !errors.Is(err, amqp.ErrCredentials) {
		t.Fatalf("Open() error = %v, want the refused credentials in the chain", err)
	}
	if kind, _ := driver.Classify(err); kind != driver.KindTransient {
		t.Fatalf("Open() error = %v (kind %v), want transient while an endpoint may still recover", err, kind)
	}
}
