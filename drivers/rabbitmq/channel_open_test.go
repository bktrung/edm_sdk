package rabbitmq

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

const channelOpenTestLimit = 100 * time.Millisecond

type channelOpenFrame struct {
	channel uint16
	class   uint16
	method  uint16
}

// channelOpenPeer completes the real client's handshake but answers channel.open
// only when the test releases it. Channel closes are answered and recorded, so a
// late open must visibly release its broker channel, not just abandon a pointer.
type channelOpenPeer struct {
	socket      net.Conn
	writeMu     sync.Mutex
	frames      chan channelOpenFrame
	done        chan struct{}
	ignoreClose atomic.Bool
}

func newChannelOpenPeer(t *testing.T, wrap ...func(net.Conn) net.Conn) (*conn, *channelOpenPeer) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	peer := &channelOpenPeer{frames: make(chan channelOpenFrame, 32), done: make(chan struct{})}
	accepted := make(chan error, 1)
	go func() {
		socket, acceptErr := listener.Accept()
		if acceptErr != nil {
			accepted <- acceptErr
			close(peer.done)
			return
		}
		peer.socket = socket
		accepted <- nil
		defer close(peer.done)
		if peer.handshake() != nil {
			_ = socket.Close()
			return
		}
		for {
			frame, readErr := readChannelOpenFrame(socket)
			if readErr != nil {
				return
			}
			peer.frames <- frame
			switch {
			case frame.class == 20 && frame.method == 40:
				if peer.reply(frame.channel, 20, 41, nil) != nil {
					return
				}
			case frame.class == 10 && frame.method == 50 && !peer.ignoreClose.Load():
				if peer.reply(0, 10, 51, nil) != nil {
					return
				}
			case frame.class == 85 && frame.method == 10:
				if peer.reply(frame.channel, 85, 11, nil) != nil {
					return
				}
			}
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), channelOpenTestLimit)
	defer cancel()
	config := amqp.Config{Dial: func(network, address string) (net.Conn, error) {
		socket, dialErr := (&net.Dialer{}).DialContext(ctx, network, address)
		if dialErr != nil {
			return nil, dialErr
		}
		deadline, _ := ctx.Deadline()
		if err := socket.SetDeadline(deadline); err != nil {
			_ = socket.Close()
			return nil, err
		}
		if len(wrap) != 0 {
			return wrap[0](socket), nil
		}
		return socket, nil
	}}
	// A final handshake frame can enqueue a heartbeat deadline reset. If
	// CloseDeadline sets its bound before the heartbeater applies that reset,
	// the longer read deadline overwrites the close bound. URI zero is explicit;
	// Config's zero alone enables the client's default heartbeat negotiation.
	raw, dialErr := amqp.DialConfig("amqp://guest:guest@"+listener.Addr().String()+"/?heartbeat=0", config)
	acceptErr := <-accepted
	t.Cleanup(func() {
		_ = listener.Close()
		if acceptErr == nil {
			_ = peer.socket.Close()
		}
		wait := clock.NewReal().Timer(channelOpenTestLimit)
		defer wait.Stop()
		select {
		case <-peer.done:
		case <-wait.C:
			t.Error("AMQP peer did not exit during cleanup")
		}
		if raw != nil {
			_ = raw.CloseDeadline(clock.NewReal().Now().Add(channelOpenTestLimit))
		}
	})
	if dialErr != nil {
		t.Fatalf("dial channel-open peer: %v", dialErr)
	}
	if acceptErr != nil {
		t.Fatal(acceptErr)
	}
	c := &conn{
		amqp:      raw,
		queueKind: queueKindQuorum,
		active:    make(map[*consumer]struct{}),
		producers: make(map[*producer]struct{}),
		deferred:  make(map[string]time.Duration),
	}
	return c, peer
}

func (p *channelOpenPeer) handshake() error {
	header := make([]byte, 8)
	if _, err := io.ReadFull(p.socket, header); err != nil {
		return err
	}
	if _, err := p.socket.Write(connectionStartFrame()); err != nil {
		return err
	}
	if _, err := readChannelOpenFrame(p.socket); err != nil {
		return err
	}
	tune := binary.BigEndian.AppendUint16(nil, 64)
	tune = binary.BigEndian.AppendUint32(tune, 131072)
	tune = binary.BigEndian.AppendUint16(tune, 0)
	if err := p.reply(0, 10, 30, tune); err != nil {
		return err
	}
	for range 2 {
		if _, err := readChannelOpenFrame(p.socket); err != nil {
			return err
		}
	}
	return p.reply(0, 10, 41, []byte{0})
}

func readChannelOpenFrame(socket net.Conn) (channelOpenFrame, error) {
	var header [7]byte
	if _, err := io.ReadFull(socket, header[:]); err != nil {
		return channelOpenFrame{}, err
	}
	length := binary.BigEndian.Uint32(header[3:])
	if header[0] != 1 || length < 4 || length > 131072 {
		return channelOpenFrame{}, errors.New("unexpected AMQP method frame")
	}
	payload := make([]byte, int(length)+1)
	if _, err := io.ReadFull(socket, payload); err != nil {
		return channelOpenFrame{}, err
	}
	if payload[len(payload)-1] != 0xce {
		return channelOpenFrame{}, errors.New("invalid AMQP frame terminator")
	}
	return channelOpenFrame{
		channel: binary.BigEndian.Uint16(header[1:3]),
		class:   binary.BigEndian.Uint16(payload[:2]),
		method:  binary.BigEndian.Uint16(payload[2:4]),
	}, nil
}

func (p *channelOpenPeer) reply(channel, class, method uint16, body []byte) error {
	payload := binary.BigEndian.AppendUint16(nil, class)
	payload = binary.BigEndian.AppendUint16(payload, method)
	payload = append(payload, body...)
	length := len(payload)
	if length > 131072 {
		return errors.New("AMQP reply exceeds negotiated frame size")
	}
	frame := binary.BigEndian.AppendUint16([]byte{1}, channel)
	frame = binary.BigEndian.AppendUint32(frame, uint32(length))
	frame = append(frame, payload...)
	frame = append(frame, 0xce)
	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	_, err := p.socket.Write(frame)
	return err
}

func (p *channelOpenPeer) next(t *testing.T, class, method uint16) channelOpenFrame {
	t.Helper()
	wait := clock.NewReal().Timer(channelOpenTestLimit)
	defer wait.Stop()
	select {
	case frame := <-p.frames:
		if frame.class != class || frame.method != method {
			t.Fatalf("AMQP frame = %d,%d, want %d,%d", frame.class, frame.method, class, method)
		}
		return frame
	case <-wait.C:
		t.Fatalf("peer never received AMQP method %d,%d", class, method)
		return channelOpenFrame{}
	}
}

func channelOpenOutcome(t *testing.T, result <-chan error) error {
	t.Helper()
	wait := clock.NewReal().Timer(channelOpenTestLimit)
	defer wait.Stop()
	select {
	case err := <-result:
		return err
	case <-wait.C:
		t.Fatal("caller remained blocked on channel.open or shutdown join")
		return nil
	}
}

func assertChannelOpenTransient(t *testing.T, err, cause error) {
	t.Helper()
	kind, classified := driver.Classify(err)
	if !classified || kind != driver.KindTransient || !errors.Is(err, cause) {
		t.Fatalf("error = %v (kind %v, classified %t), want transient %v", err, kind, classified, cause)
	}
}

func TestPingDeadlineBoundsChannelOpen(t *testing.T) {
	c, peer := newChannelOpenPeer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- c.Ping(ctx) }()
	peer.next(t, 20, 10)
	assertChannelOpenTransient(t, channelOpenOutcome(t, result), context.DeadlineExceeded)
}

func TestEnsureTopologyDeadlineBoundsChannelOpen(t *testing.T) {
	c, peer := newChannelOpenPeer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := c.Admin().EnsureTopology(ctx, driver.TopologySpec{
			Policy:    driver.TopologyDeclare,
			Exchanges: []driver.ExchangeSpec{{Name: "channel-open-test", Kind: "fanout", Durable: true}},
		})
		result <- err
	}()
	peer.next(t, 20, 10)
	assertChannelOpenTransient(t, channelOpenOutcome(t, result), context.DeadlineExceeded)
}

func TestProducerDeadlineBoundsChannelOpen(t *testing.T) {
	t.Run("construction", func(t *testing.T) {
		c, peer := newChannelOpenPeer(t)
		ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
		defer cancel()
		result := make(chan error, 1)
		go func() {
			_, err := c.Producer(ctx, driver.ProducerConfig{})
			result <- err
		}()
		peer.next(t, 20, 10)
		assertChannelOpenTransient(t, channelOpenOutcome(t, result), context.DeadlineExceeded)
		c.mu.RLock()
		defer c.mu.RUnlock()
		if len(c.producers) != 0 {
			t.Fatal("cancelled construction installed a producer")
		}
	})
	t.Run("lazy_slot", func(t *testing.T) {
		c, peer := newChannelOpenPeer(t)
		p := &producer{conn: c}
		for slot := range p.opening {
			p.opening[slot] = make(chan struct{}, 1)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
		defer cancel()
		result := make(chan error, 1)
		go func() {
			result <- p.Publish(ctx, driver.OutboundMessage{Destination: "channel-open-test", Body: []byte("body")})
		}()
		peer.next(t, 20, 10)
		err := channelOpenOutcome(t, result)
		var publishErr *driver.PublishError
		if !errors.As(err, &publishErr) {
			t.Fatalf("Publish error = %v, want PublishError", err)
		}
		assertChannelOpenTransient(t, publishErr.Failed[0], context.DeadlineExceeded)
	})
}

func TestCloseDeadlineBoundsStalledChannelOpen(t *testing.T) {
	c, peer := newChannelOpenPeer(t)
	peer.ignoreClose.Store(true)
	pingCtx, cancelPing := context.WithTimeout(context.Background(), channelOpenTestLimit)
	defer cancelPing()
	ping := make(chan error, 1)
	go func() { ping <- c.Ping(pingCtx) }()
	peer.next(t, 20, 10)
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- c.Close(ctx) }()
	err := channelOpenOutcome(t, result)
	kind, classified := driver.Classify(err)
	var timeout net.Error
	var amqpErr *amqp.Error
	isTimeout := errors.Is(err, context.DeadlineExceeded) ||
		(errors.As(err, &timeout) && timeout.Timeout()) ||
		(errors.As(err, &amqpErr) && amqpErr.Code == amqp.FrameError)
	if !classified || kind != driver.KindTransient || !isTimeout {
		t.Fatalf("Close = %v, want classified transient timeout", err)
	}
	peer.next(t, 10, 50)
	retryCtx, cancelRetry := context.WithTimeout(context.Background(), channelOpenTestLimit)
	defer cancelRetry()
	if err := c.Close(retryCtx); err != nil {
		t.Fatalf("Close did not join the released open worker: %v", err)
	}
	if !c.amqp.IsClosed() {
		t.Fatal("Close deadline did not shut down transport")
	}
	if channelOpenOutcome(t, ping) == nil {
		t.Fatal("stalled Ping succeeded after shutdown")
	}
}

func TestChannelOpenedAfterCancellationIsClosed(t *testing.T) {
	c, peer := newChannelOpenPeer(t)
	ctx, cancel := context.WithTimeout(context.Background(), channelOpenTestLimit)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- c.Ping(ctx) }()
	opened := peer.next(t, 20, 10)
	cancel()
	assertChannelOpenTransient(t, channelOpenOutcome(t, result), context.Canceled)
	if err := peer.reply(opened.channel, 20, 11, []byte{0, 0, 0, 0}); err != nil {
		t.Fatal(err)
	}
	closed := peer.next(t, 20, 40)
	if closed.channel != opened.channel {
		t.Fatalf("closed channel %d, want late channel %d", closed.channel, opened.channel)
	}
	probeCtx, cancelProbe := context.WithTimeout(context.Background(), channelOpenTestLimit)
	defer cancelProbe()
	go func() { result <- c.Ping(probeCtx) }()
	probe := peer.next(t, 20, 10)
	if err := peer.reply(probe.channel, 20, 11, []byte{0, 0, 0, 0}); err != nil {
		t.Fatal(err)
	}
	peer.next(t, 20, 40)
	if err := channelOpenOutcome(t, result); err != nil {
		t.Fatalf("probe after late cleanup: %v", err)
	}
}

func TestCloseBoundsDetachedChannelJoin(t *testing.T) {
	for _, name := range []string{"deadline", "successful_join"} {
		t.Run(name, func(t *testing.T) {
			c, peer := newChannelOpenPeer(t)
			release := make(chan struct{})
			var once sync.Once
			t.Cleanup(func() { once.Do(func() { close(release) }) })
			finished := make(chan struct{})
			c.startChannelCloseWithFinish(func() error {
				<-release
				return nil
			}, func(error) { close(finished) })
			ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
			defer cancel()
			result := make(chan error, 1)
			go func() { result <- c.Close(ctx) }()
			peer.next(t, 10, 50)
			if name == "deadline" {
				assertChannelOpenTransient(t, channelOpenOutcome(t, result), context.DeadlineExceeded)
				once.Do(func() { close(release) })
				retryCtx, cancelRetry := context.WithTimeout(context.Background(), channelOpenTestLimit)
				defer cancelRetry()
				if err := c.Close(retryCtx); err != nil {
					t.Fatalf("Close retry after detached completion: %v", err)
				}
			} else {
				select {
				case err := <-result:
					t.Fatalf("Close escaped its detached worker: %v", err)
				default:
				}
				once.Do(func() { close(release) })
				if err := channelOpenOutcome(t, result); err != nil {
					t.Fatalf("successful Close: %v", err)
				}
			}
			select {
			case <-finished:
			default:
				t.Fatal("successful Close did not join detached cleanup")
			}
		})
	}
}

func TestChannelOpenRejectsClosingAdmission(t *testing.T) {
	t.Run("close_after_open_started", func(t *testing.T) {
		c, peer := newChannelOpenPeer(t)
		ctx, cancel := context.WithTimeout(context.Background(), channelOpenTestLimit)
		defer cancel()
		result := make(chan error, 1)
		go func() { result <- c.Ping(ctx) }()
		opened := peer.next(t, 20, 10)
		marked := make(chan error, 1)
		go func() {
			c.mu.Lock()
			c.closing = true
			c.mu.Unlock()
			marked <- nil
		}()
		if err := channelOpenOutcome(t, marked); err != nil {
			t.Fatal(err)
		}
		if err := peer.reply(opened.channel, 20, 11, []byte{0, 0, 0, 0}); err != nil {
			t.Fatal(err)
		}
		assertChannelOpenTransient(t, channelOpenOutcome(t, result), amqp.ErrClosed)
		peer.next(t, 20, 40)
	})
	t.Run("close_before_lazy_open", func(t *testing.T) {
		c, peer := newChannelOpenPeer(t)
		c.closing = true
		p := &producer{conn: c}
		ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
		defer cancel()
		result := make(chan error, 1)
		go func() {
			_, err := p.openChannel(ctx)
			result <- err
		}()
		assertChannelOpenTransient(t, channelOpenOutcome(t, result), amqp.ErrClosed)
		select {
		case frame := <-peer.frames:
			t.Fatalf("closing admission issued AMQP method %d,%d", frame.class, frame.method)
		default:
		}
	})
}
