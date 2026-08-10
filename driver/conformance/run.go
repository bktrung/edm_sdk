package conformance

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

// Run opens one connection, executes both profiles, and compares their vectors.
func Run(t *testing.T, suite Suite) Report {
	t.Helper()
	if suite.Driver == nil {
		t.Fatalf("conformance: Driver is required")
	}
	if suite.NewInspector == nil {
		t.Fatalf("conformance: NewInspector is required")
	}
	if err := validateManifestState(groupManifest, pendingGroups, groupRunners); err != nil {
		t.Fatalf("conformance: invalid group manifest: %v", err)
	}

	ctx := context.Background()
	conn, err := suite.Driver.Open(ctx, suite.Config)
	if err != nil {
		t.Fatalf("conformance: open driver: %v", err)
	}
	defer func() {
		if err := conn.Close(ctx); err != nil {
			t.Errorf("conformance: close driver: %v", err)
		}
	}()

	inspect, err := suite.NewInspector(conn)
	if validationErr := validateInspectorFactory(inspect, err); validationErr != nil {
		t.Fatalf("conformance: NewInspector: %v", validationErr)
	}

	report := Report{
		Driver:  suite.Driver.Name(),
		Pending: append([]string(nil), pendingGroups...),
	}
	profileReports := make([]ProfileReport, 0, 2)
	for _, profile := range []Profile{ProfileFull, ProfileStrictPortability} {
		profile := profile
		var result ProfileReport
		ok := t.Run(profile.String(), func(profileTest *testing.T) {
			result = runProfile(profileTest, ctx, conn, inspect, profile)
		})
		if !ok {
			t.Fatalf("conformance: %s profile failed", profile)
		}
		profileReports = append(profileReports, result)
	}
	report.Profiles = profileReports
	if diff := report.Profiles[0].Vector.Diff(report.Profiles[1].Vector); diff != "" {
		t.Fatalf("conformance: full and strict behavior vectors differ: %s", diff)
	}
	for _, pending := range report.Pending {
		t.Logf("conformance: pending group=%s", pending)
	}
	if data, err := json.Marshal(report); err == nil {
		t.Logf("conformance report: %s", data)
	} else {
		t.Errorf("conformance: marshal report: %v", err)
	}
	return report
}

func runProfile(
	t *testing.T,
	ctx context.Context,
	conn driver.Conn,
	inspect Inspect,
	profile Profile,
) ProfileReport {
	effective := effectiveCapabilities(conn.Capabilities(), profile)
	destination := "conformance.inspect." + profile.String() + ".probe"
	_, err := conn.Admin().EnsureTopology(ctx, driver.TopologySpec{
		Destinations: []driver.DestinationSpec{{Name: destination}},
		Scope:        []string{"conformance.inspect." + profile.String() + "."},
		Effective:    effective,
	})
	if err != nil {
		t.Fatalf("ensure topology: %v", err)
	}
	producer, err := conn.Producer(ctx, driver.ProducerConfig{
		RequireDurableAck: true,
		Effective:         effective,
	})
	if err != nil {
		t.Fatalf("create producer: %v", err)
	}
	var consumer driver.Consumer
	messages := make([]driver.InboundMessage, 0, 2)
	t.Cleanup(func() {
		cleanupProfile(t, ctx, producer, consumer, messages...)
	})
	before, err := inspect(ctx, destination)
	if err != nil {
		t.Fatalf("inspect baseline: %v", err)
	}
	const n = 2
	for i := 0; i < n; i++ {
		if err := producer.Publish(ctx, driver.OutboundMessage{
			Destination: destination,
			Body:        []byte{byte(i)},
		}); err != nil {
			t.Fatalf("publish inspector probe %d: %v", i, err)
		}
	}
	afterPublish, err := inspect(ctx, destination)
	if err != nil {
		t.Fatalf("inspect after publish: %v", err)
	}

	consumer, err = conn.Consumer(ctx, driver.ConsumerConfig{
		Destinations: []string{destination},
		Prefetch:     n,
		Effective:    effective,
	})
	if err != nil {
		t.Fatalf("create consumer: %v", err)
	}
	for i := 0; i < n; i++ {
		receiveCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		select {
		case message := <-consumer.Messages():
			cancel()
			if message.Settle == nil {
				t.Fatalf("inspector probe message %d has nil settler", i)
			}
			messages = append(messages, message)
		case <-receiveCtx.Done():
			cancel()
			t.Fatalf("receive inspector probe %d: %v", i, receiveCtx.Err())
		}
	}
	afterReceive, err := inspect(ctx, destination)
	if err != nil {
		t.Fatalf("inspect after receive: %v", err)
	}
	for i := range messages {
		if err := messages[i].Settle.Ack(ctx); err != nil {
			t.Fatalf("settle inspector probe %d: %v", i, err)
		}
	}
	messages = messages[:0]
	afterSettle, err := inspect(ctx, destination)
	if err != nil {
		t.Fatalf("inspect after settle: %v", err)
	}
	if validationErr := validateInspectorDelta(before, afterPublish, afterReceive, afterSettle, n); validationErr != nil {
		t.Fatalf("Inspect self-check: %v", validationErr)
	}

	result := ProfileReport{Profile: profile, Vector: BehaviorVector{}, Groups: []GroupResult{}}
	for _, entry := range groupManifest {
		runner, runnerExists := groupRunners[entry.name]
		if !runnerExists {
			continue
		}
		var groupResult *groupContext
		groupOK := t.Run(entry.name, func(groupTest *testing.T) {
			groupResult = &groupContext{
				t: groupTest, ctx: ctx, conn: conn, inspect: inspect,
				profile: profile,
			}
			runner(groupResult)
		})
		if !groupOK {
			t.Fatalf("conformance group %s failed", entry.name)
		}
		observed := groupResult.checks
		if err := validateGroupCount(entry.name, entry.declared, observed); err != nil {
			t.Fatalf("%v", err)
		}
		result.Vector = append(result.Vector, groupResult.vector...)
		result.Groups = append(result.Groups, GroupResult{
			Name: entry.name, Declared: entry.declared, Observed: observed, Status: "passed",
		})
	}
	return result
}

func cleanupProfile(t *testing.T, ctx context.Context, producer driver.Producer, consumer driver.Consumer, messages ...driver.InboundMessage) {
	t.Helper()
	for _, message := range messages {
		if message.Settle != nil {
			_ = message.Settle.Nack(ctx, driver.NackOptions{})
		}
	}
	if consumer != nil {
		if err := consumer.Stop(ctx); err != nil {
			t.Errorf("stop profile consumer: %v", err)
		}
	}
	if err := producer.Close(ctx); err != nil {
		t.Errorf("close profile producer: %v", err)
	}
}

// WriteJSON writes an indented JSON report.
func (r Report) WriteJSON(w io.Writer) error {
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	return encoder.Encode(r)
}

// WriteMarkdown writes a Markdown report.
func (r Report) WriteMarkdown(w io.Writer) error {
	if _, err := fmt.Fprintf(w, "# Conformance report: %s\n\n", r.Driver); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w, "| Profile | Capability | Declared | Status | Evidence |\n|---|---|---|---|---|"); err != nil {
		return err
	}
	for _, capability := range r.Capabilities {
		if _, err := fmt.Fprintf(w, "| %s | %s | %s | %s | %s |\n",
			capability.Profile, capability.Capability, capability.Declared,
			capability.Status, capability.Evidence); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintln(w, "\n## Pending groups"); err != nil {
		return err
	}
	for _, pending := range r.Pending {
		if _, err := fmt.Fprintf(w, "- %s\n", pending); err != nil {
			return err
		}
	}
	return nil
}
