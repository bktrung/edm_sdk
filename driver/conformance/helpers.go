package conformance

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

func assertCancelled(t *testing.T, operation string, err error) {
	t.Helper()
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("%s() error=%v, want context.Canceled", operation, err)
	}
	if kind, ok := driver.Classify(err); !ok || kind != driver.KindTransient {
		t.Fatalf("%s() classification=(%v,%t), want transient,true", operation, kind, ok)
	}
}

func (g *groupContext) maintenance(t *testing.T) driver.Maintenance {
	t.Helper()
	maintenance, ok := g.conn.Admin().(driver.Maintenance)
	if !ok {
		g.Skip(t, g.currentCheck, "driver.Maintenance is not implemented")
	}
	return &profileMaintenance{group: g, maintenance: maintenance}
}

const (
	waitTimeout  = 5 * time.Second
	waitInterval = 5 * time.Millisecond

	// The stability window only needs to outlast the next dispatch, which is the mechanism
	// that can violate the current empty-channel and upper-bound assertions.
	stabilityWindow = 250 * time.Millisecond
)

func profileDestination(group *groupContext, destination string) string {
	if destination == "" {
		return ""
	}
	profile := group.profile.String()
	runID := group.runID
	parts := strings.Split(destination, ".")
	for i := 0; i+1 < len(parts); i++ {
		if parts[i] == runID && parts[i+1] == profile {
			return destination
		}
	}
	if len(parts) == 1 {
		return destination + "." + runID + "." + profile
	}
	return parts[0] + "." + runID + "." + profile + "." + strings.Join(parts[1:], ".")
}

func unprofileDestination(group *groupContext, destination string) string {
	parts := strings.Split(destination, ".")
	runID := group.runID
	profile := group.profile.String()
	for i := 0; i+1 < len(parts); i++ {
		if parts[i] == runID && parts[i+1] == profile {
			parts = append(parts[:i], parts[i+2:]...)
			return strings.Join(parts, ".")
		}
	}
	return destination
}

func profileScope(group *groupContext, scope string) string {
	if strings.HasSuffix(scope, ".") {
		base := strings.TrimSuffix(scope, ".")
		return profileDestination(group, base) + "."
	}
	return profileDestination(group, scope)
}

func profileTopologySpec(group *groupContext, spec driver.TopologySpec) driver.TopologySpec {
	if spec.Exchanges != nil {
		spec.Exchanges = append([]driver.ExchangeSpec(nil), spec.Exchanges...)
		for i := range spec.Exchanges {
			spec.Exchanges[i].Name = profileDestination(group, spec.Exchanges[i].Name)
		}
	}
	if spec.Destinations != nil {
		spec.Destinations = append([]driver.DestinationSpec(nil), spec.Destinations...)
		for i := range spec.Destinations {
			destination := &spec.Destinations[i]
			destination.Name = profileDestination(group, destination.Name)
			if destination.DeadLetter != nil {
				route := *destination.DeadLetter
				route.Exchange = profileDestination(group, route.Exchange)
				destination.DeadLetter = &route
			}
		}
	}
	if spec.Bindings != nil {
		spec.Bindings = append([]driver.BindingSpec(nil), spec.Bindings...)
		for i := range spec.Bindings {
			spec.Bindings[i].Source = profileDestination(group, spec.Bindings[i].Source)
			spec.Bindings[i].Destination = profileDestination(group, spec.Bindings[i].Destination)
		}
	}
	if spec.Scope != nil {
		spec.Scope = append([]string(nil), spec.Scope...)
		for i := range spec.Scope {
			spec.Scope[i] = profileScope(group, spec.Scope[i])
		}
	}
	return spec
}

// warmTopology moves Kafka destination creation and metadata propagation out of timed checks.
// The first publish or fetch otherwise pays both costs.
func warmTopology(t *testing.T, group *groupContext, spec driver.TopologySpec) {
	t.Helper()
	if _, err := group.conn.Admin().EnsureTopology(group.ctx, profileTopologySpec(group, spec)); err != nil {
		t.Fatalf("EnsureTopology warm-up: %v", err)
	}
}

func profileDestinations(group *groupContext, destinations []string) ([]string, map[string]string) {
	scoped := make([]string, len(destinations))
	logical := make(map[string]string, len(destinations))
	for i, destination := range destinations {
		scopedDestination := profileDestination(group, destination)
		scoped[i] = scopedDestination
		logical[scopedDestination] = unprofileDestination(group, scopedDestination)
	}
	return scoped, logical
}

func profileConsumerConfig(group *groupContext, cfg driver.ConsumerConfig) (driver.ConsumerConfig, map[string]string) {
	scoped, logical := profileDestinations(group, cfg.Destinations)
	cfg.Destinations = scoped
	if cfg.PerDestination != nil {
		perDestination := make(map[string]int, len(cfg.PerDestination))
		for destination, share := range cfg.PerDestination {
			perDestination[profileDestination(group, destination)] = share
		}
		cfg.PerDestination = perDestination
	}
	return cfg, logical
}

type profileProducer struct {
	group    *groupContext
	producer driver.Producer
	scoped   bool
}

func (p *profileProducer) Publish(ctx context.Context, messages ...driver.OutboundMessage) error {
	scoped := make([]driver.OutboundMessage, len(messages))
	for i, message := range messages {
		if p.scoped {
			message.Destination = profileDestination(p.group, message.Destination)
		}
		scoped[i] = message
	}
	err := p.producer.Publish(ctx, scoped...)
	return profileError(p.group, err)
}

func (p *profileProducer) Close(ctx context.Context) error {
	return p.producer.Close(ctx)
}

type profileConsumer struct {
	group    *groupContext
	consumer driver.Consumer
	logical  map[string]string
	messages <-chan driver.InboundMessage
	done     chan struct{}
	stopOnce sync.Once
}

func newProfileConsumer(group *groupContext, consumer driver.Consumer, logical map[string]string) driver.Consumer {
	messages := make(chan driver.InboundMessage)
	done := make(chan struct{})
	go func() {
		defer close(messages)
		for message := range consumer.Messages() {
			if destination, ok := logical[message.Destination]; ok {
				message.Destination = destination
			}
			select {
			case messages <- message:
			case <-done:
				return
			}
		}
	}()
	return &profileConsumer{group: group, consumer: consumer, logical: logical, messages: messages, done: done}
}

func (c *profileConsumer) Messages() <-chan driver.InboundMessage {
	return c.messages
}

func (c *profileConsumer) Errors() <-chan error {
	return c.consumer.Errors()
}

func (c *profileConsumer) Pause(destinations ...string) error {
	return c.consumer.Pause(profileDestinationsForCall(c.group, destinations)...)
}

func (c *profileConsumer) Resume(destinations ...string) error {
	return c.consumer.Resume(profileDestinationsForCall(c.group, destinations)...)
}

func (c *profileConsumer) Drain(ctx context.Context) error {
	return c.consumer.Drain(ctx)
}

func (c *profileConsumer) Stop(ctx context.Context) error {
	err := c.consumer.Stop(ctx)
	if err == nil {
		c.stopOnce.Do(func() { close(c.done) })
	}
	return err
}

func (c *profileConsumer) Release(ctx context.Context) error {
	err := c.consumer.Release(ctx)
	if err == nil {
		c.stopOnce.Do(func() { close(c.done) })
	}
	return err
}

func (c *profileConsumer) Lag(ctx context.Context) (map[string]int64, error) {
	lag, err := c.consumer.Lag(ctx)
	if err != nil {
		return nil, err
	}
	logical := make(map[string]int64, len(lag))
	for destination, value := range lag {
		if name, ok := c.logical[destination]; ok {
			destination = name
		}
		logical[destination] = value
	}
	return logical, nil
}

type profileMaintenance struct {
	group       *groupContext
	maintenance driver.Maintenance
}

func (m *profileMaintenance) Purge(ctx context.Context, destination string) (int64, error) {
	count, err := m.maintenance.Purge(ctx, profileDestination(m.group, destination))
	return count, profileError(m.group, err)
}

func (m *profileMaintenance) Prune(ctx context.Context, names []string) ([]driver.PruneResult, error) {
	scoped, logical := profileDestinations(m.group, names)
	results, err := m.maintenance.Prune(ctx, scoped)
	for i := range results {
		if name, ok := logical[results[i].Name]; ok {
			results[i].Name = name
		}
	}
	return results, profileError(m.group, err)
}

type profileAdmin struct {
	group *groupContext
	admin driver.Admin
}

func newProfileAdmin(group *groupContext, admin driver.Admin) driver.Admin {
	return &profileAdmin{group: group, admin: admin}
}

type profileWrappedError struct {
	message string
	err     error
}

func (e *profileWrappedError) Error() string {
	return e.message
}

func (e *profileWrappedError) Unwrap() error {
	return e.err
}

func profileError(group *groupContext, err error) error {
	if err == nil {
		return nil
	}
	message := err.Error()
	runID := group.runID
	profile := group.profile.String()
	parts := strings.Split(message, " ")
	for i, part := range parts {
		parts[i] = strings.ReplaceAll(part, "."+runID+"."+profile, "")
	}
	return &profileWrappedError{message: strings.Join(parts, " "), err: err}
}

func (a *profileAdmin) EnsureTopology(ctx context.Context, spec driver.TopologySpec) (driver.TopologyDiff, error) {
	scoped := profileTopologySpec(a.group, spec)
	diff, err := a.admin.EnsureTopology(ctx, scoped)
	if err != nil {
		return driver.TopologyDiff{}, profileError(a.group, err)
	}
	return unprofileTopologyDiff(a.group, diff), nil
}

func (a *profileAdmin) DescribeTopology(ctx context.Context, names []string) (driver.TopologyState, error) {
	scoped, logical := profileDestinations(a.group, names)
	state, err := a.admin.DescribeTopology(ctx, scoped)
	if err != nil {
		return driver.TopologyState{}, profileError(a.group, err)
	}
	depth := make(map[string]int64, len(state.Depth))
	for destination, value := range state.Depth {
		if name, ok := logical[destination]; ok {
			destination = name
		}
		depth[destination] = value
	}
	state.Depth = depth
	return state, nil
}

func unprofileTopologyDiff(group *groupContext, diff driver.TopologyDiff) driver.TopologyDiff {
	logical := func(destination string) string {
		return unprofileDestination(group, destination)
	}
	mapNames := func(names []string) {
		for i := range names {
			names[i] = logical(names[i])
		}
	}
	mapNames(diff.CreatedExchanges)
	mapNames(diff.CreatedDestinations)
	mapNames(diff.ExistingExchanges)
	mapNames(diff.ExistingDestinations)
	for i := range diff.Orphaned {
		diff.Orphaned[i].Name = logical(diff.Orphaned[i].Name)
	}
	for i := range diff.Drifted {
		diff.Drifted[i].Name = logical(diff.Drifted[i].Name)
	}
	for i := range diff.CreatedBindings {
		diff.CreatedBindings[i].Source = logical(diff.CreatedBindings[i].Source)
		diff.CreatedBindings[i].Destination = logical(diff.CreatedBindings[i].Destination)
	}
	for i := range diff.ExistingBindings {
		diff.ExistingBindings[i].Source = logical(diff.ExistingBindings[i].Source)
		diff.ExistingBindings[i].Destination = logical(diff.ExistingBindings[i].Destination)
	}
	return diff
}

func profileDestinationsForCall(group *groupContext, destinations []string) []string {
	scoped := make([]string, len(destinations))
	for i, destination := range destinations {
		scoped[i] = profileDestination(group, destination)
	}
	return scoped
}

func newProducer(t *testing.T, group *groupContext, destination string, config driver.ProducerConfig) driver.Producer {
	t.Helper()
	logicalDestination := unprofileDestination(group, destination)
	if _, err := group.conn.Admin().EnsureTopology(group.ctx, driver.TopologySpec{
		Destinations: []driver.DestinationSpec{{Name: destination}},
		Effective:    group.effective,
	}); err != nil {
		t.Fatalf("EnsureTopology(%q): %v", logicalDestination, err)
	}
	producer, err := group.conn.Producer(group.ctx, config)
	if err != nil {
		t.Fatalf("Producer(%q): %v", logicalDestination, err)
	}
	t.Cleanup(func() {
		if err := producer.Close(group.ctx); err != nil {
			t.Errorf("close producer %q: %v", logicalDestination, err)
		}
	})
	t.Cleanup(func() {
		if err := purgeIfSupported(group.ctx, group.conn, destination); err != nil {
			t.Errorf("purge destination %q: %v", logicalDestination, err)
		}
	})
	return &profileProducer{group: group, producer: producer, scoped: destination != unprofileDestination(group, destination)}
}

func newConsumer(t *testing.T, group *groupContext, destination string, prefetch int) driver.Consumer {
	t.Helper()
	logicalDestination := unprofileDestination(group, destination)
	cfg := driver.ConsumerConfig{
		Destinations: []string{destination}, Prefetch: prefetch, Effective: group.effective,
	}
	logical := map[string]string{destination: logicalDestination}
	consumer, err := group.conn.Consumer(group.ctx, cfg)
	if err != nil {
		t.Fatalf("Consumer(%q): %v", logicalDestination, err)
	}
	wrapped := newProfileConsumer(group, consumer, logical)
	t.Cleanup(func() {
		if err := wrapped.Stop(group.ctx); err != nil {
			t.Errorf("stop consumer %q: %v", logicalDestination, err)
		}
	})
	return wrapped
}

func headerByKey(t *testing.T, headers []driver.Header, key string) driver.Header {
	t.Helper()
	for _, header := range headers {
		if header.Key == key {
			return header
		}
	}
	t.Fatalf("header %q not found in %#v", key, headers)
	return driver.Header{}
}

func inspectDestination(t *testing.T, group *groupContext, destination string) BrokerView {
	t.Helper()
	view, err := group.inspect(group.ctx, profileDestination(group, destination))
	if err != nil {
		t.Fatalf("Inspect(%q): %v", destination, err)
	}
	return view
}

func receiveMessage(t *testing.T, group *groupContext, consumer driver.Consumer) driver.InboundMessage {
	t.Helper()
	var received driver.InboundMessage
	waitFor(t, group, "published message", func() (bool, string) {
		select {
		case message, ok := <-consumer.Messages():
			if !ok {
				return false, "Messages channel closed"
			}
			received = message
			return true, fmt.Sprintf("received destination=%q", message.Destination)
		default:
			return false, "no message"
		}
	})
	return received
}

// waitFor retries condition until it holds or the deadline passes, and reports
// the last observed value on failure.
func waitFor(t *testing.T, group *groupContext, what string, condition func() (bool, string)) {
	t.Helper()
	waitForUntil(t, group, what, condition, false, waitTimeout)
}

func waitForStable(t *testing.T, group *groupContext, what string, condition func() (bool, string)) {
	t.Helper()
	waitForUntil(t, group, what, condition, true, stabilityWindow)
}

// waitForStableFor is waitForStable over a caller-chosen window.
func waitForStableFor(t *testing.T, group *groupContext, what string, window time.Duration, condition func() (bool, string)) {
	t.Helper()
	waitForUntil(t, group, what, condition, true, window)
}

func waitForUntil(t *testing.T, group *groupContext, what string, condition func() (bool, string), stable bool, timeout time.Duration) {
	t.Helper()
	deadline, cancel := context.WithTimeout(group.ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(waitInterval) //nolint:forbidigo // conformance is stdlib-only and cannot import internal/clock
	defer ticker.Stop()
	var lastObservation string
	for {
		holds, observation := condition()
		lastObservation = observation
		if !holds {
			if stable {
				t.Fatalf("%s violated; last observation: %s", what, lastObservation)
			}
		} else if !stable {
			return
		}

		select {
		case <-deadline.Done():
			if stable {
				return
			}
			t.Fatalf("%s timed out after %s; last observation: %s", what, timeout, lastObservation)
		case <-ticker.C:
		}
	}
}

func ackMessage(t *testing.T, group *groupContext, message driver.InboundMessage) {
	t.Helper()
	if message.Settle == nil {
		t.Fatal("published message has nil settler")
	}
	if err := message.Settle.Ack(group.ctx); err != nil {
		t.Fatalf("Ack() error = %v", err)
	}
}
