package conformance

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

func init() {
	registerGroup("topology", runTopology)
}

// farFuture is a DelayUntil value that never comes due during a test run, so
// a message published with it stays in a destination's park rather than its
// ready queue.
var farFuture = time.Date(2100, time.January, 1, 0, 0, 0, 0, time.UTC)

func runTopology(group *groupContext) {
	runTopologyPolicyChecks(group)
	group.Check("EnsureTopology creates missing destinations", func(t *testing.T) {
		admin := group.conn.Admin()
		// Suffixed by profile: Run shares one Conn across both profile passes,
		// so a fixed name would already exist as Existing on the second pass.
		name := "topology.create." + group.profile.String()
		diff, err := admin.EnsureTopology(group.ctx, driver.TopologySpec{
			Destinations: []driver.DestinationSpec{{Name: name}},
			Effective:    group.effective,
		})
		if err != nil {
			t.Fatalf("EnsureTopology(%q): %v", name, err)
		}
		if !containsName(diff.CreatedDestinations, name) {
			t.Fatalf("CreatedDestinations=%v, want %q", diff.CreatedDestinations, name)
		}
		if containsName(diff.ExistingDestinations, name) {
			t.Fatalf("ExistingDestinations=%v, want %q absent on first creation", diff.ExistingDestinations, name)
		}
		cleanupTopologyDestinations(t, admin, group.ctx, name)
		// FinalDestination is the profile-independent label: the two profile
		// passes must record identical vectors, but the actual destination name
		// is suffixed per profile to stay unique across the shared Conn.
		group.vector.Add(BehaviorEvent{ID: "topology-create", Outcome: "created", FinalDestination: "topology.create"})
	})

	group.Check("EnsureTopology is idempotent on a repeat call", func(t *testing.T) {
		admin := group.conn.Admin()
		name := "topology.idempotent"
		spec := driver.TopologySpec{
			Destinations: []driver.DestinationSpec{{Name: name}},
			Effective:    group.effective,
		}
		if _, err := admin.EnsureTopology(group.ctx, spec); err != nil {
			t.Fatalf("first EnsureTopology(%q): %v", name, err)
		}
		t.Cleanup(func() {
			if _, err := admin.Purge(group.ctx, name); err != nil {
				t.Errorf("purge destination %q: %v", name, err)
			}
		})
		diff, err := admin.EnsureTopology(group.ctx, spec)
		if err != nil {
			t.Fatalf("second EnsureTopology(%q): %v", name, err)
		}
		if containsName(diff.CreatedDestinations, name) {
			t.Fatalf("CreatedDestinations=%v on repeat, want %q reported existing only", diff.CreatedDestinations, name)
		}
		if !containsName(diff.ExistingDestinations, name) {
			t.Fatalf("ExistingDestinations=%v, want %q", diff.ExistingDestinations, name)
		}
		// Positive control: the destination still functions after the repeat
		// call, proving idempotency did not tear anything down.
		producer := newProducer(t, group, name, driver.ProducerConfig{Effective: group.effective})
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: name}); err != nil {
			t.Fatalf("Publish(%q) after repeat EnsureTopology: %v", name, err)
		}
		waitFor(t, group, "destination to accept a publish after the repeat call", func() (bool, string) {
			view := inspectDestination(t, group, name)
			return view.Ready == 1, fmt.Sprintf("view=%+v", view)
		})
		group.vector.Add(BehaviorEvent{ID: "topology-idempotent", Outcome: "existing", FinalDestination: name})
	})

	group.Check("EnsureTopology reports created and existing destinations together in one call", func(t *testing.T) {
		admin := group.conn.Admin()
		existing := "topology.mixed.existing"
		// Suffixed by profile: Run shares one Conn across both profile passes,
		// so a fixed name would already exist as Existing on the second pass.
		created := "topology.mixed.created." + group.profile.String()
		if _, err := admin.EnsureTopology(group.ctx, driver.TopologySpec{
			Destinations: []driver.DestinationSpec{{Name: existing}},
			Effective:    group.effective,
		}); err != nil {
			t.Fatalf("EnsureTopology(seed): %v", err)
		}
		cleanupTopologyDestinations(t, admin, group.ctx, existing, created)
		diff, err := admin.EnsureTopology(group.ctx, driver.TopologySpec{
			Destinations: []driver.DestinationSpec{{Name: existing}, {Name: created}},
			Effective:    group.effective,
		})
		if err != nil {
			t.Fatalf("EnsureTopology(mixed): %v", err)
		}
		if !containsName(diff.ExistingDestinations, existing) {
			t.Fatalf("ExistingDestinations=%v, want %q", diff.ExistingDestinations, existing)
		}
		if !containsName(diff.CreatedDestinations, created) {
			t.Fatalf("CreatedDestinations=%v, want %q", diff.CreatedDestinations, created)
		}
		group.vector.Add(BehaviorEvent{ID: "topology-mixed-diff", Outcome: "mixed", FinalDestination: "topology.mixed.created"})
	})

	group.Check("EnsureTopology reports a destination dropped from the spec but still in scope", func(t *testing.T) {
		admin := group.conn.Admin()
		kept := "topology.orphan.under.high"
		dropped := "topology.orphan.under.low"
		scope := []string{"topology.orphan.under."}
		if _, err := admin.EnsureTopology(group.ctx, driver.TopologySpec{
			Destinations: []driver.DestinationSpec{{Name: kept}, {Name: dropped}},
			Scope:        scope,
			Effective:    group.effective,
		}); err != nil {
			t.Fatalf("EnsureTopology(seed): %v", err)
		}
		t.Cleanup(func() {
			for _, name := range []string{kept, dropped} {
				if _, err := admin.Purge(group.ctx, name); err != nil {
					t.Errorf("purge destination %q: %v", name, err)
				}
			}
		})
		diff, err := admin.EnsureTopology(group.ctx, driver.TopologySpec{
			Destinations: []driver.DestinationSpec{{Name: kept}},
			Scope:        scope,
			Effective:    group.effective,
		})
		if err != nil {
			t.Fatalf("EnsureTopology(dropped): %v", err)
		}
		if diff.OrphanScanError != "" {
			t.Fatalf("OrphanScanError=%q, want empty", diff.OrphanScanError)
		}
		if _, ok := findOrphan(diff.Orphaned, dropped); !ok {
			t.Fatalf("Orphaned=%v, want %q", diff.Orphaned, dropped)
		}
		group.vector.Add(BehaviorEvent{ID: "topology-orphan-under-report", Outcome: "orphaned", FinalDestination: dropped})
	})

	group.Check("EnsureTopology does not report a destination outside the requested scope", func(t *testing.T) {
		admin := group.conn.Admin()
		inSpec := "topology.orphan.over.sub1.main"
		droppedInScope := "topology.orphan.over.sub1.retry"
		siblingOutsideScope := "topology.orphan.over.sub2.main"
		if _, err := admin.EnsureTopology(group.ctx, driver.TopologySpec{
			Destinations: []driver.DestinationSpec{{Name: inSpec}, {Name: droppedInScope}, {Name: siblingOutsideScope}},
			Effective:    group.effective,
		}); err != nil {
			t.Fatalf("EnsureTopology(seed): %v", err)
		}
		t.Cleanup(func() {
			for _, name := range []string{inSpec, droppedInScope, siblingOutsideScope} {
				if _, err := admin.Purge(group.ctx, name); err != nil {
					t.Errorf("purge destination %q: %v", name, err)
				}
			}
		})
		diff, err := admin.EnsureTopology(group.ctx, driver.TopologySpec{
			Destinations: []driver.DestinationSpec{{Name: inSpec}},
			Scope:        []string{"topology.orphan.over.sub1."},
			Effective:    group.effective,
		})
		if err != nil {
			t.Fatalf("EnsureTopology(scoped): %v", err)
		}
		if diff.OrphanScanError != "" {
			t.Fatalf("OrphanScanError=%q, want empty", diff.OrphanScanError)
		}
		// Positive control: a destination genuinely in scope and dropped from
		// the spec must be reported, proving the scan ran rather than
		// returning an empty result unconditionally.
		if _, ok := findOrphan(diff.Orphaned, droppedInScope); !ok {
			t.Fatalf("Orphaned=%v, want %q (in scope, dropped)", diff.Orphaned, droppedInScope)
		}
		if _, ok := findOrphan(diff.Orphaned, siblingOutsideScope); ok {
			t.Fatalf("Orphaned=%v, sibling %q outside scope must not be reported", diff.Orphaned, siblingOutsideScope)
		}
		group.vector.Add(BehaviorEvent{ID: "topology-orphan-over-report", Outcome: "excluded", FinalDestination: siblingOutsideScope})
	})

	group.Check("EnsureTopology scope prefix matches only at a component boundary", func(t *testing.T) {
		admin := group.conn.Admin()
		inSpec := "topology.orphan.boundary.worker.main"
		continuation := "topology.orphan.boundary.worker-v2.main"
		droppedInScope := "topology.orphan.boundary.worker.retry"
		if _, err := admin.EnsureTopology(group.ctx, driver.TopologySpec{
			Destinations: []driver.DestinationSpec{{Name: inSpec}, {Name: continuation}, {Name: droppedInScope}},
			Effective:    group.effective,
		}); err != nil {
			t.Fatalf("EnsureTopology(seed): %v", err)
		}
		t.Cleanup(func() {
			for _, name := range []string{inSpec, continuation, droppedInScope} {
				if _, err := admin.Purge(group.ctx, name); err != nil {
					t.Errorf("purge destination %q: %v", name, err)
				}
			}
		})
		diff, err := admin.EnsureTopology(group.ctx, driver.TopologySpec{
			Destinations: []driver.DestinationSpec{{Name: inSpec}},
			Scope:        []string{"topology.orphan.boundary.worker."},
			Effective:    group.effective,
		})
		if err != nil {
			t.Fatalf("EnsureTopology(scoped): %v", err)
		}
		if diff.OrphanScanError != "" {
			t.Fatalf("OrphanScanError=%q, want empty", diff.OrphanScanError)
		}
		// Positive control: proves the scan ran before trusting the negative
		// claim below.
		if _, ok := findOrphan(diff.Orphaned, droppedInScope); !ok {
			t.Fatalf("Orphaned=%v, want %q (proves the scan ran)", diff.Orphaned, droppedInScope)
		}
		if _, ok := findOrphan(diff.Orphaned, continuation); ok {
			t.Fatalf("Orphaned=%v, %q must not match the scope by same-token continuation", diff.Orphaned, continuation)
		}
		// Sharper case: the same prefix without a trailing separator must catch
		// worker-v2.main too, proving the exclusion above comes from matching the
		// plain string rather than from a driver that parses dot-separated
		// components itself. Boundary safety is the caller's, carried by the
		// trailing separator in the scope entry.
		unbounded, err := admin.EnsureTopology(group.ctx, driver.TopologySpec{
			Destinations: []driver.DestinationSpec{{Name: inSpec}},
			Scope:        []string{"topology.orphan.boundary.worker"},
			Effective:    group.effective,
		})
		if err != nil {
			t.Fatalf("EnsureTopology(unbounded scope): %v", err)
		}
		if _, ok := findOrphan(unbounded.Orphaned, continuation); !ok {
			t.Fatalf("Orphaned=%v, want %q reported under a scope without a trailing separator", unbounded.Orphaned, continuation)
		}
		group.vector.Add(BehaviorEvent{ID: "topology-orphan-boundary", Outcome: "excluded", FinalDestination: continuation})
	})

	group.Check("EnsureTopology with an empty scope disables orphan scanning", func(t *testing.T) {
		admin := group.conn.Admin()
		kept := "topology.orphan.noscan.kept"
		dropped := "topology.orphan.noscan.dropped"
		scope := []string{"topology.orphan.noscan."}
		if _, err := admin.EnsureTopology(group.ctx, driver.TopologySpec{
			Destinations: []driver.DestinationSpec{{Name: kept}, {Name: dropped}},
			Scope:        scope,
			Effective:    group.effective,
		}); err != nil {
			t.Fatalf("EnsureTopology(seed): %v", err)
		}
		t.Cleanup(func() {
			for _, name := range []string{kept, dropped} {
				if _, err := admin.Purge(group.ctx, name); err != nil {
					t.Errorf("purge destination %q: %v", name, err)
				}
			}
		})
		// Positive control: with a real scope, dropping "dropped" from the spec
		// is reported, proving there is something a disabled scan could miss.
		control, err := admin.EnsureTopology(group.ctx, driver.TopologySpec{
			Destinations: []driver.DestinationSpec{{Name: kept}},
			Scope:        scope,
			Effective:    group.effective,
		})
		if err != nil {
			t.Fatalf("EnsureTopology(scoped control): %v", err)
		}
		if _, ok := findOrphan(control.Orphaned, dropped); !ok {
			t.Fatalf("Orphaned=%v, want %q (control: scope enabled)", control.Orphaned, dropped)
		}
		// Same call, empty Scope this time: the only thing that changed is
		// whether scanning is enabled.
		diff, err := admin.EnsureTopology(group.ctx, driver.TopologySpec{
			Destinations: []driver.DestinationSpec{{Name: kept}},
			Effective:    group.effective,
		})
		if err != nil {
			t.Fatalf("EnsureTopology(no scope): %v", err)
		}
		if diff.OrphanScanError == "" {
			t.Fatal("OrphanScanError empty, want a reason for not scanning")
		}
		if len(diff.Orphaned) != 0 {
			t.Fatalf("Orphaned=%v, want none reported when scanning is disabled", diff.Orphaned)
		}
		group.vector.Add(BehaviorEvent{ID: "topology-orphan-scan-disabled", Outcome: "skipped", FinalDestination: dropped})
	})

	group.Check("EnsureTopology folds deferred messages into an orphaned destination's message count", func(t *testing.T) {
		admin := group.conn.Admin()
		kept := "topology.orphan.aux.kept"
		dropped := "topology.orphan.aux.dropped"
		scope := []string{"topology.orphan.aux."}
		if _, err := admin.EnsureTopology(group.ctx, driver.TopologySpec{
			Destinations: []driver.DestinationSpec{{Name: kept}, {Name: dropped}},
			Scope:        scope,
			Effective:    group.effective,
		}); err != nil {
			t.Fatalf("EnsureTopology(seed): %v", err)
		}
		t.Cleanup(func() {
			for _, name := range []string{kept, dropped} {
				if _, err := admin.Purge(group.ctx, name); err != nil {
					t.Errorf("purge destination %q: %v", name, err)
				}
			}
		})
		producer := newDeferredProducer(t, group, dropped, deferredDelay)
		if err := producer.Publish(group.ctx,
			driver.OutboundMessage{Destination: dropped},
			driver.OutboundMessage{Destination: dropped, DelayUntil: farFuture},
		); err != nil {
			t.Fatalf("Publish(%q): %v", dropped, err)
		}
		waitFor(t, group, "seeded messages to land on the dropped destination", func() (bool, string) {
			view := inspectDestination(t, group, dropped)
			return view.Ready == 1 && view.Auxiliary == 1, fmt.Sprintf("view=%+v", view)
		})
		diff, err := admin.EnsureTopology(group.ctx, driver.TopologySpec{
			Destinations: []driver.DestinationSpec{{Name: kept}},
			Scope:        scope,
			Effective:    group.effective,
		})
		if err != nil {
			t.Fatalf("EnsureTopology(dropped): %v", err)
		}
		if diff.OrphanScanError != "" {
			t.Fatalf("OrphanScanError=%q, want empty", diff.OrphanScanError)
		}
		orphan, ok := findOrphan(diff.Orphaned, dropped)
		if !ok {
			t.Fatalf("Orphaned=%v, want %q", diff.Orphaned, dropped)
		}
		if orphan.Messages != 2 {
			t.Fatalf("Orphaned %q Messages=%d, want 2 (ready + deferred)", dropped, orphan.Messages)
		}
		group.vector.Add(BehaviorEvent{ID: "topology-orphan-auxiliary", Outcome: "counted", FinalDestination: dropped})
	})

	group.Check("DescribeTopology reports depth including deferred messages", func(t *testing.T) {
		admin := group.conn.Admin()
		name := "topology.describe.depth"
		producer := newDeferredProducer(t, group, name, deferredDelay)
		if err := producer.Publish(group.ctx,
			driver.OutboundMessage{Destination: name},
			driver.OutboundMessage{Destination: name, DelayUntil: farFuture},
			driver.OutboundMessage{Destination: name, DelayUntil: farFuture},
		); err != nil {
			t.Fatalf("Publish(%q): %v", name, err)
		}
		waitFor(t, group, "seeded messages to land on the destination", func() (bool, string) {
			view := inspectDestination(t, group, name)
			return view.Ready == 1 && view.Auxiliary == 2, fmt.Sprintf("view=%+v", view)
		})
		state, err := admin.DescribeTopology(group.ctx, []string{name})
		if err != nil {
			t.Fatalf("DescribeTopology(%q): %v", name, err)
		}
		depth, ok := state.Depth[name]
		if !ok || depth != 3 {
			t.Fatalf("Depth[%q]=(%d,%t), want (3,true) (ready + deferred)", name, depth, ok)
		}
		group.vector.Add(BehaviorEvent{ID: "topology-describe-depth", Outcome: "counted", FinalDestination: name})
	})

	group.Check("Prune refuses a destination that still holds ready messages", func(t *testing.T) {
		admin := group.conn.Admin()
		nonEmpty := "topology.prune.ready"
		eligible := "topology.prune.ready.eligible"
		if _, err := admin.EnsureTopology(group.ctx, driver.TopologySpec{
			Destinations: []driver.DestinationSpec{{Name: eligible}},
			Effective:    group.effective,
		}); err != nil {
			t.Fatalf("EnsureTopology(eligible): %v", err)
		}
		producer := newProducer(t, group, nonEmpty, driver.ProducerConfig{Effective: group.effective})
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: nonEmpty}); err != nil {
			t.Fatalf("Publish(%q): %v", nonEmpty, err)
		}
		waitFor(t, group, "ready message to land", func() (bool, string) {
			view := inspectDestination(t, group, nonEmpty)
			return view.Ready == 1, fmt.Sprintf("view=%+v", view)
		})
		results, err := admin.Prune(group.ctx, []string{nonEmpty, eligible})
		if err != nil {
			t.Fatalf("Prune(): %v", err)
		}
		refused, deleted := findPruneResult(results, nonEmpty), findPruneResult(results, eligible)
		if refused.Deleted || refused.Reason == "" {
			t.Fatalf("Prune(%q)=%+v, want refused with a reason", nonEmpty, refused)
		}
		// Positive control: the eligible destination deletes in the same call,
		// corroborating that the refusal above is genuine and not a Prune
		// that silently did nothing.
		if !deleted.Deleted {
			t.Fatalf("Prune(%q)=%+v, want deleted", eligible, deleted)
		}
		// newProducer already registered a Purge cleanup for nonEmpty.
		group.vector.Add(BehaviorEvent{ID: "topology-prune-ready-guard", Outcome: "refused", FinalDestination: nonEmpty})
	})

	group.Check("Prune refuses an empty destination whose park still holds messages", func(t *testing.T) {
		admin := group.conn.Admin()
		parked := "topology.prune.park"
		eligible := "topology.prune.park.eligible"
		if _, err := admin.EnsureTopology(group.ctx, driver.TopologySpec{
			Destinations: []driver.DestinationSpec{{Name: eligible}},
			Effective:    group.effective,
		}); err != nil {
			t.Fatalf("EnsureTopology(eligible): %v", err)
		}
		producer := newDeferredProducer(t, group, parked, deferredDelay)
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: parked, DelayUntil: farFuture}); err != nil {
			t.Fatalf("Publish(%q): %v", parked, err)
		}
		// The sharpest case: the ready queue is empty while the park is not, so
		// a driver that checks the ready queue alone would delete this one.
		waitFor(t, group, "deferred message to land in the park", func() (bool, string) {
			view := inspectDestination(t, group, parked)
			return view.Ready == 0 && view.Auxiliary == 1, fmt.Sprintf("view=%+v", view)
		})
		results, err := admin.Prune(group.ctx, []string{parked, eligible})
		if err != nil {
			t.Fatalf("Prune(): %v", err)
		}
		refused, deleted := findPruneResult(results, parked), findPruneResult(results, eligible)
		if refused.Deleted || refused.Reason == "" {
			t.Fatalf("Prune(%q)=%+v, want refused with a reason", parked, refused)
		}
		if !deleted.Deleted {
			t.Fatalf("Prune(%q)=%+v, want deleted", eligible, deleted)
		}
		// newProducer already registered a Purge cleanup for parked.
		group.vector.Add(BehaviorEvent{ID: "topology-prune-park-guard", Outcome: "refused", FinalDestination: parked})
	})

	group.Check("Prune refuses a destination with an attached consumer", func(t *testing.T) {
		admin := group.conn.Admin()
		attached := "topology.prune.consumer"
		eligible := "topology.prune.consumer.eligible"
		if _, err := admin.EnsureTopology(group.ctx, driver.TopologySpec{
			Destinations: []driver.DestinationSpec{{Name: attached}, {Name: eligible}},
			Effective:    group.effective,
		}); err != nil {
			t.Fatalf("EnsureTopology(seed): %v", err)
		}
		_ = newConsumer(t, group, attached, 1)
		results, err := admin.Prune(group.ctx, []string{attached, eligible})
		if err != nil {
			t.Fatalf("Prune(): %v", err)
		}
		refused, deleted := findPruneResult(results, attached), findPruneResult(results, eligible)
		if refused.Deleted || refused.Reason == "" {
			t.Fatalf("Prune(%q)=%+v, want refused with a reason", attached, refused)
		}
		if !deleted.Deleted {
			t.Fatalf("Prune(%q)=%+v, want deleted", eligible, deleted)
		}
		group.vector.Add(BehaviorEvent{ID: "topology-prune-consumer-guard", Outcome: "refused", FinalDestination: attached})
	})

	group.Check("Prune on an unknown destination reports not-deleted without erroring", func(t *testing.T) {
		admin := group.conn.Admin()
		name := "topology.prune.missing"
		results, err := admin.Prune(group.ctx, []string{name})
		if err != nil {
			t.Fatalf("Prune(%q): %v", name, err)
		}
		result := findPruneResult(results, name)
		if result.Deleted || result.Reason == "" {
			t.Fatalf("Prune(%q)=%+v, want not deleted with a reason", name, result)
		}
		group.vector.Add(BehaviorEvent{ID: "topology-prune-missing", Outcome: "refused", FinalDestination: name})
	})

	group.Check("Purge empties a destination and keeps it", func(t *testing.T) {
		admin := group.conn.Admin()
		name := "topology.purge"
		producer := newProducer(t, group, name, driver.ProducerConfig{Effective: group.effective})
		if err := producer.Publish(group.ctx,
			driver.OutboundMessage{Destination: name},
			driver.OutboundMessage{Destination: name},
		); err != nil {
			t.Fatalf("Publish(%q): %v", name, err)
		}
		waitFor(t, group, "seeded messages to land", func() (bool, string) {
			view := inspectDestination(t, group, name)
			return view.Ready == 2, fmt.Sprintf("view=%+v", view)
		})
		purged, err := admin.Purge(group.ctx, name)
		if err != nil {
			t.Fatalf("Purge(%q): %v", name, err)
		}
		if purged != 2 {
			t.Fatalf("Purge(%q)=%d, want 2", name, purged)
		}
		// The destination itself survives, empty: DescribeTopology still finds
		// it (present in the map) rather than reporting it missing (absent).
		state, err := admin.DescribeTopology(group.ctx, []string{name})
		if err != nil {
			t.Fatalf("DescribeTopology(%q) after purge: %v", name, err)
		}
		depth, ok := state.Depth[name]
		if !ok {
			t.Fatalf("Depth[%q] absent after purge, want present with 0 (Purge keeps the destination)", name)
		}
		if depth != 0 {
			t.Fatalf("Depth[%q]=%d after purge, want 0", name, depth)
		}
		// Positive control: the destination still accepts new publishes.
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: name}); err != nil {
			t.Fatalf("Publish(%q) after purge: %v", name, err)
		}
		waitFor(t, group, "purged destination to accept a new publish", func() (bool, string) {
			view := inspectDestination(t, group, name)
			return view.Ready == 1, fmt.Sprintf("view=%+v", view)
		})
		group.vector.Add(BehaviorEvent{ID: "topology-purge", Outcome: "emptied", FinalDestination: name})
	})

	group.Check("cancelled context prevents topology admin calls without a partial effect", func(t *testing.T) {
		admin := group.conn.Admin()
		name := "topology.cancel"
		newName := "topology.cancel.new"
		if _, err := admin.EnsureTopology(group.ctx, driver.TopologySpec{
			Destinations: []driver.DestinationSpec{{Name: name}},
			Effective:    group.effective,
		}); err != nil {
			t.Fatalf("EnsureTopology(seed): %v", err)
		}
		t.Cleanup(func() {
			if _, err := admin.Purge(group.ctx, name); err != nil {
				t.Errorf("purge destination %q: %v", name, err)
			}
		})
		t.Cleanup(func() {
			// Best effort: newName should never exist if the driver is correct,
			// so a missing-destination error here is expected, not a failure.
			_, _ = admin.Purge(group.ctx, newName)
		})
		// Seed a message so the cancelled Purge below has real state to leave
		// behind: an already-empty destination cannot distinguish "refused"
		// from "did nothing because there was nothing to do."
		producer := newProducer(t, group, name, driver.ProducerConfig{Effective: group.effective})
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: name}); err != nil {
			t.Fatalf("Publish(%q): %v", name, err)
		}
		waitFor(t, group, "seeded message to land before the cancelled calls", func() (bool, string) {
			view := inspectDestination(t, group, name)
			return view.Ready == 1, fmt.Sprintf("view=%+v", view)
		})

		ctx, cancel := context.WithCancel(group.ctx)
		cancel()
		// newName is unseen by any earlier call: a driver that creates it and
		// only afterward checks ctx.Err() would still leave it behind despite
		// returning context.Canceled here.
		if _, err := admin.EnsureTopology(ctx, driver.TopologySpec{
			Destinations: []driver.DestinationSpec{{Name: newName}},
			Effective:    group.effective,
		}); !errors.Is(err, context.Canceled) {
			t.Fatalf("EnsureTopology() with cancelled ctx error=%v, want context.Canceled", err)
		}
		if _, err := admin.DescribeTopology(ctx, []string{name}); !errors.Is(err, context.Canceled) {
			t.Fatalf("DescribeTopology() with cancelled ctx error=%v, want context.Canceled", err)
		}
		if _, err := admin.Purge(ctx, name); !errors.Is(err, context.Canceled) {
			t.Fatalf("Purge() with cancelled ctx error=%v, want context.Canceled", err)
		}
		if _, err := admin.Prune(ctx, []string{name}); !errors.Is(err, context.Canceled) {
			t.Fatalf("Prune() with cancelled ctx error=%v, want context.Canceled", err)
		}

		// Positive control: newName must never have been created. This is also
		// the group's coverage for DescribeTopology's classified not-found
		// error, since observing "never created" needs exactly this lookup.
		_, err := admin.DescribeTopology(group.ctx, []string{newName})
		if !errors.Is(err, driver.ErrDestinationMissing) {
			t.Fatalf("DescribeTopology(%q) error=%v, want ErrDestinationMissing", newName, err)
		}
		if kind, ok := driver.Classify(err); !ok || kind != driver.KindNotFound {
			t.Fatalf("DescribeTopology(%q) classification=(%v,%t), want not_found", newName, kind, ok)
		}
		// Positive control: name is unaffected by the cancelled Purge/Prune,
		// proving neither ran to completion despite returning context.Canceled.
		waitForStable(t, group, "name to stay unaffected by the cancelled calls", func() (bool, string) {
			state, describeErr := admin.DescribeTopology(group.ctx, []string{name})
			if describeErr != nil {
				return false, fmt.Sprintf("DescribeTopology(%q): %v", name, describeErr)
			}
			depth, ok := state.Depth[name]
			return ok && depth == 1, fmt.Sprintf("Depth[%q]=(%d,%t), want (1,true)", name, depth, ok)
		})
		group.vector.Add(BehaviorEvent{ID: "topology-cancel", Outcome: "cancelled", FinalDestination: name})
	})
}

func containsName(names []string, want string) bool {
	for _, name := range names {
		if name == want {
			return true
		}
	}
	return false
}

func cleanupTopologyDestinations(t *testing.T, admin driver.Admin, ctx context.Context, names ...string) {
	t.Helper()
	t.Cleanup(func() {
		results, err := admin.Prune(ctx, names)
		if err != nil {
			t.Errorf("prune destinations %v: %v", names, err)
			return
		}
		for _, name := range names {
			result := findPruneResult(results, name)
			if !result.Deleted {
				t.Errorf("Prune(%q)=%+v, want deleted", name, result)
				continue
			}
			_, describeErr := admin.DescribeTopology(ctx, []string{name})
			if describeErr == nil {
				t.Errorf("DescribeTopology(%q) succeeded after cleanup, want destination missing", name)
				continue
			}
			if !errors.Is(describeErr, driver.ErrDestinationMissing) {
				t.Errorf("DescribeTopology(%q) after cleanup: %v", name, describeErr)
			}
		}
	})
}

func findOrphan(orphans []driver.OrphanedDestination, name string) (driver.OrphanedDestination, bool) {
	for _, orphan := range orphans {
		if orphan.Name == name {
			return orphan, true
		}
	}
	return driver.OrphanedDestination{}, false
}

func findPruneResult(results []driver.PruneResult, name string) driver.PruneResult {
	for _, result := range results {
		if result.Name == name {
			return result
		}
	}
	return driver.PruneResult{}
}

func runTopologyPolicyChecks(group *groupContext) {
	group.Check("TopologyVerify reports the first missing destination without creating", func(t *testing.T) {
		admin := group.conn.Admin()
		existing := "topology.verify.existing." + group.profile.String()
		missing := "topology.verify.missing." + group.profile.String()
		second := "topology.verify.second." + group.profile.String()
		if _, err := admin.EnsureTopology(group.ctx, driver.TopologySpec{
			Destinations: []driver.DestinationSpec{{Name: existing}},
			Effective:    group.effective,
		}); err != nil {
			t.Fatalf("seed topology: %v", err)
		}
		t.Cleanup(func() {
			if _, err := admin.Purge(group.ctx, existing); err != nil {
				t.Errorf("purge %q: %v", existing, err)
			}
		})
		_, err := admin.EnsureTopology(group.ctx, driver.TopologySpec{
			Destinations: []driver.DestinationSpec{{Name: existing}, {Name: missing}, {Name: second}},
			Policy:       driver.TopologyVerify,
			Effective:    group.effective,
		})
		if err == nil {
			t.Fatal("TopologyVerify returned nil for a missing destination")
		}
		if kind, ok := driver.Classify(err); !ok || kind != driver.KindNotFound {
			t.Fatalf("TopologyVerify classification = %v,%t, want not_found,true", kind, ok)
		}
		if !strings.Contains(err.Error(), missing) || strings.Contains(err.Error(), second) {
			t.Fatalf("TopologyVerify error = %q, want first missing %q only", err, missing)
		}
		if _, inspectErr := group.inspect(group.ctx, missing); inspectErr == nil {
			t.Fatalf("TopologyVerify created %q", missing)
		}
		if _, inspectErr := group.inspect(group.ctx, second); inspectErr == nil {
			t.Fatalf("TopologyVerify created %q", second)
		}
		group.vector.Add(BehaviorEvent{ID: "topology-verify-missing", Outcome: "not-found", FinalDestination: "topology.verify.missing"})
	})

	group.Check("FanoutAtConsume ignores bindings", func(t *testing.T) {
		if group.effective.Fanout != driver.FanoutAtConsume {
			group.Skip(t, "FanoutAtConsume ignores bindings", fmt.Sprintf("effective fanout mode %d is not FanoutAtConsume", group.effective.Fanout))
			return
		}
		effective := group.effective
		prefix := "topology.consume-fanout." + group.profile.String()
		first := prefix + ".first"
		second := prefix + ".second"
		diff, err := group.conn.Admin().EnsureTopology(group.ctx, driver.TopologySpec{
			Destinations: []driver.DestinationSpec{{Name: first}, {Name: second}},
			Bindings:     []driver.BindingSpec{{Source: prefix + ".exchange", Destination: first}},
			Effective:    effective,
		})
		if err != nil {
			t.Fatalf("FanoutAtConsume EnsureTopology: %v", err)
		}
		t.Cleanup(func() {
			for _, name := range []string{first, second} {
				if _, err := group.conn.Admin().Purge(group.ctx, name); err != nil {
					t.Errorf("purge FanoutAtConsume destination %q: %v", name, err)
				}
			}
		})
		if !containsName(diff.CreatedDestinations, first) || !containsName(diff.CreatedDestinations, second) {
			t.Fatalf("FanoutAtConsume CreatedDestinations=%v, want %q and %q", diff.CreatedDestinations, first, second)
		}
		producer, err := group.conn.Producer(group.ctx, driver.ProducerConfig{Effective: effective})
		if err != nil {
			t.Fatalf("FanoutAtConsume producer: %v", err)
		}
		t.Cleanup(func() {
			if err := producer.Close(group.ctx); err != nil {
				t.Errorf("close FanoutAtConsume producer: %v", err)
			}
		})
		consumer, err := group.conn.Consumer(group.ctx, driver.ConsumerConfig{
			Destinations: []string{first}, Prefetch: 1, Effective: effective,
		})
		if err != nil {
			t.Fatalf("FanoutAtConsume consumer: %v", err)
		}
		t.Cleanup(func() {
			if err := consumer.Stop(group.ctx); err != nil {
				t.Errorf("stop FanoutAtConsume consumer: %v", err)
			}
		})
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: first, Body: []byte("consume-fanout")}); err != nil {
			t.Fatalf("FanoutAtConsume publish: %v", err)
		}
		message := receiveMessage(t, group, consumer)
		if string(message.Body) != "consume-fanout" {
			t.Fatalf("FanoutAtConsume body=%q, want consume-fanout", message.Body)
		}
		ackMessage(t, group, message)
		group.vector.Add(BehaviorEvent{ID: "topology-fanout-consume", Outcome: "ignored", FinalDestination: "topology.consume-fanout"})
	})

	group.Check("TopologyNone leaves missing topology untouched", func(t *testing.T) {
		admin := group.conn.Admin()
		name := "topology.none." + group.profile.String()
		diff, err := admin.EnsureTopology(group.ctx, driver.TopologySpec{
			Destinations: []driver.DestinationSpec{{Name: name}},
			Policy:       driver.TopologyNone,
			Effective:    group.effective,
		})
		if err != nil {
			t.Fatalf("TopologyNone: %v", err)
		}
		if len(diff.CreatedExchanges) != 0 || len(diff.CreatedDestinations) != 0 || len(diff.CreatedBindings) != 0 || len(diff.ExistingExchanges) != 0 || len(diff.ExistingDestinations) != 0 || len(diff.ExistingBindings) != 0 || len(diff.Orphaned) != 0 {
			t.Fatalf("TopologyNone diff = %+v, want empty", diff)
		}
		producer, err := group.conn.Producer(group.ctx, driver.ProducerConfig{Effective: group.effective})
		if err != nil {
			t.Fatalf("producer: %v", err)
		}
		t.Cleanup(func() {
			if err := producer.Close(group.ctx); err != nil {
				t.Errorf("close producer: %v", err)
			}
		})
		if err := producer.Publish(group.ctx, driver.OutboundMessage{Destination: name}); err == nil {
			t.Fatalf("Publish(%q) succeeded after TopologyNone", name)
		} else if kind, ok := driver.Classify(err); !ok || kind != driver.KindNotFound {
			t.Fatalf("Publish(%q) classification = %v,%t, want not_found,true", name, kind, ok)
		}
		group.vector.Add(BehaviorEvent{ID: "topology-none", Outcome: "untouched", FinalDestination: "topology.none"})
	})
}
