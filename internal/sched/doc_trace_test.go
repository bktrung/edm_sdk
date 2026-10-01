package sched

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

var updateDocTrace = flag.Bool("update-doc-trace", false, "update the scheduler documentation trace")

type docTrace struct {
	Scenarios []docScenario `json:"scenarios"`
}

type docScenario struct {
	ID     string    `json:"id"`
	Title  string    `json:"title"`
	Groups []string  `json:"groups"`
	Lanes  []docLane `json:"lanes"`
	Steps  []docStep `json:"steps"`
}

type docLane struct {
	ID    string `json:"id"`
	Group string `json:"group"`
}

type docStep struct {
	Number            int        `json:"number"`
	Action            string     `json:"action"`
	ClockOffsetMillis int64      `json:"clockOffsetMillis"`
	Winner            string     `json:"winner"`
	Promoted          bool       `json:"promoted"`
	Scores            []docScore `json:"scores"`
	Depths            []docDepth `json:"depths"`
	Caption           string     `json:"caption"`
}

type docScore struct {
	Group string `json:"group"`
	Score int    `json:"score"`
}

type docDepth struct {
	Lane  string `json:"lane"`
	Depth int    `json:"depth"`
}

func TestSchedulerDocTrace(t *testing.T) {
	start := time.Unix(0, 0)
	trace := docTrace{Scenarios: []docScenario{
		buildWeightedTrace(t, start),
		buildPromotionTrace(t, start),
	}}

	path := filepath.Join("..", "..", "docs", ".vitepress", "data", "scheduler-trace.json")
	data, err := json.MarshalIndent(trace, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	data = append(data, '\n')
	if *updateDocTrace {
		if err := os.WriteFile(path, data, 0o644); err != nil { //nolint:gosec // a committed docs fixture, readable like every other repository file
			t.Fatalf("write %s: %v", path, err)
		}
		return
	}

	want, err := os.ReadFile(path) //nolint:gosec // path is the fixed docs fixture this test owns
	if err != nil {
		t.Fatalf("read %s: %v; run go test ./internal/sched -run TestSchedulerDocTrace -update-doc-trace", path, err)
	}
	if !bytes.Equal(want, data) {
		t.Fatalf("scheduler doc trace is out of date; run go test ./internal/sched -run TestSchedulerDocTrace -update-doc-trace")
	}
}

func buildWeightedTrace(t *testing.T, start time.Time) docScenario {
	t.Helper()
	specs := []LaneSpec{
		{ID: "high", Group: "high", Weight: 8, Capacity: 13},
		{ID: "medium", Group: "medium", Weight: 4, Capacity: 13},
		{ID: "low", Group: "low", Weight: 1, Capacity: 13},
	}
	fake := clock.NewFake(start)
	scheduler, err := New(specs, fake, false)
	if err != nil {
		t.Fatal(err)
	}
	scenario := newDocScenario("weighted", "Weighted pick with weights 8, 4 and 1", specs)
	for _, spec := range specs {
		for range 13 {
			if err := scheduler.Enqueue(spec.ID, Item{Value: spec.ID}); err != nil {
				t.Fatal(err)
			}
		}
	}
	appendDocEnqueueStep(&scenario, fake, scheduler)

	wantWinners := []string{
		"high", "medium", "high", "high", "medium", "high", "low",
		"high", "medium", "high", "high", "medium", "high",
	}
	for _, want := range wantWinners {
		beforeScores, beforeDepths := schedulerDocSnapshot(scheduler)
		item, promotion, ok := scheduler.Next()
		if !ok {
			t.Fatal("weighted scheduler unexpectedly empty")
		}
		if item.Value != want {
			t.Fatalf("weighted winner = %v, want %s", item.Value, want)
		}
		if promotion != (Promotion{}) {
			t.Fatalf("weighted pick reported promotion %+v", promotion)
		}
		appendWeightedPickStep(t, &scenario, fake, scheduler, specs, beforeScores, beforeDepths, want)
	}
	return scenario
}

func buildPromotionTrace(t *testing.T, start time.Time) docScenario {
	t.Helper()
	specs := []LaneSpec{
		{ID: "high", Group: "high", Weight: 8, Budget: time.Hour, Capacity: 2},
		{ID: "low", Group: "low", Weight: 1, Budget: time.Second, Capacity: 1},
	}
	fake := clock.NewFake(start)
	scheduler, err := New(specs, fake, true)
	if err != nil {
		t.Fatal(err)
	}
	scenario := newDocScenario("promotion", "Jumping the queue when overdue", specs)
	for range 2 {
		if err := scheduler.Enqueue("high", Item{Value: "high"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := scheduler.Enqueue("low", Item{Value: "low"}); err != nil {
		t.Fatal(err)
	}
	appendDocEnqueueStep(&scenario, fake, scheduler)

	beforeScores, beforeDepths := schedulerDocSnapshot(scheduler)
	item, promotion, ok := scheduler.Next()
	if !ok || item.Value != "high" || promotion != (Promotion{}) {
		t.Fatalf("normal pick = %#v, %+v, %v; want high without promotion", item.Value, promotion, ok)
	}
	appendWeightedPickStep(t, &scenario, fake, scheduler, specs, beforeScores, beforeDepths, "high")

	fake.Advance(time.Second)
	appendDocAdvanceStep(t, &scenario, fake, scheduler, "low")

	beforePromotion := schedulerDocScores(scheduler)
	wait, budget := schedulerDocLaneTiming(scheduler, "low")
	item, promotion, ok = scheduler.Next()
	if !ok || item.Value != "low" || promotion.LaneID != "low" {
		t.Fatalf("promoted pick = %#v, %+v, %v; want low promotion", item.Value, promotion, ok)
	}
	appendPromotedPickStep(&scenario, fake, scheduler, "low", wait, budget)
	if after := schedulerDocScores(scheduler); !reflect.DeepEqual(beforePromotion, after) {
		t.Fatalf("promoted pick changed scores: before=%v after=%v", beforePromotion, after)
	}

	beforeScores, beforeDepths = schedulerDocSnapshot(scheduler)
	item, promotion, ok = scheduler.Next()
	if !ok || item.Value != "high" || promotion != (Promotion{}) {
		t.Fatalf("next normal pick = %#v, %+v, %v; want high without promotion", item.Value, promotion, ok)
	}
	appendWeightedPickStep(t, &scenario, fake, scheduler, specs, beforeScores, beforeDepths, "high")
	return scenario
}

func newDocScenario(id, title string, specs []LaneSpec) docScenario {
	scenario := docScenario{ID: id, Title: title}
	for _, spec := range specs {
		group := spec.Group
		if group == "" {
			group = spec.ID
		}
		if len(scenario.Groups) == 0 || scenario.Groups[len(scenario.Groups)-1] != group {
			scenario.Groups = append(scenario.Groups, group)
		}
		scenario.Lanes = append(scenario.Lanes, docLane{ID: spec.ID, Group: group})
	}
	return scenario
}

func newDocStep(scenario *docScenario, fake *clock.Fake, scheduler *Scheduler, action, winner string, promoted bool) docStep {
	scores, depths := schedulerDocSnapshot(scheduler)
	return docStep{
		Number:            len(scenario.Steps) + 1,
		Action:            action,
		ClockOffsetMillis: fake.Now().Sub(time.Unix(0, 0)).Milliseconds(),
		Winner:            winner,
		Promoted:          promoted,
		Scores:            scores,
		Depths:            depths,
	}
}

func appendDocEnqueueStep(scenario *docScenario, fake *clock.Fake, scheduler *Scheduler) {
	step := newDocStep(scenario, fake, scheduler, "enqueue", "", false)
	step.Caption = fmt.Sprintf("Items enter the lanes at %d ms.", step.ClockOffsetMillis)
	scenario.Steps = append(scenario.Steps, step)
}

func appendDocAdvanceStep(t *testing.T, scenario *docScenario, fake *clock.Fake, scheduler *Scheduler, laneID string) {
	t.Helper()
	lane := scheduler.byID[laneID]
	if lane == nil || len(lane.items) == 0 {
		t.Fatalf("advance caption lane %q has no queued item", laneID)
	}
	step := newDocStep(scenario, fake, scheduler, "advance", "", false)
	wait := fake.Now().Sub(lane.items[0].EnqueuedAt)
	step.Caption = fmt.Sprintf(
		"The oldest %s item has waited %d ms against its %d ms wait limit.",
		laneID,
		wait.Milliseconds(),
		lane.spec.Budget.Milliseconds(),
	)
	scenario.Steps = append(scenario.Steps, step)
}

func appendWeightedPickStep(
	t *testing.T,
	scenario *docScenario,
	fake *clock.Fake,
	scheduler *Scheduler,
	specs []LaneSpec,
	beforeScores []docScore,
	beforeDepths []docDepth,
	winner string,
) {
	t.Helper()
	step := newDocStep(scenario, fake, scheduler, "pick", winner, false)
	activeWeight := 0
	winnerActive := false
	for _, score := range beforeScores {
		if docGroupDepth(score.Group, specs, beforeDepths) == 0 {
			continue
		}
		activeWeight += docGroupWeight(score.Group, specs)
		if score.Group == winner {
			winnerActive = true
		}
	}
	if !winnerActive {
		t.Fatalf("weighted winner %q was not active before the pick", winner)
	}
	winnerAfterGiveBack := docScoreFor(winner, step.Scores)
	winnerBeforeGiveBack := winnerAfterGiveBack + activeWeight
	for _, score := range step.Scores {
		if score.Group == winner || docGroupDepth(score.Group, specs, beforeDepths) == 0 {
			continue
		}
		if score.Score > winnerBeforeGiveBack {
			t.Fatalf("weighted winner score = %d, but recorded %s score = %d", winnerBeforeGiveBack, score.Group, score.Score)
		}
	}
	step.Caption = fmt.Sprintf(
		"%s has the highest score, %d, so it wins and gives back the total active weight, %d, leaving %d.",
		winner,
		winnerBeforeGiveBack,
		activeWeight,
		winnerAfterGiveBack,
	)
	scenario.Steps = append(scenario.Steps, step)
}

func appendPromotedPickStep(scenario *docScenario, fake *clock.Fake, scheduler *Scheduler, winner string, wait, budget time.Duration) {
	budgetStatus := "passed"
	if wait == budget {
		budgetStatus = "reached"
	}
	step := newDocStep(scenario, fake, scheduler, "pick", winner, true)
	step.Caption = fmt.Sprintf(
		"%s waited %d ms and %s its %d ms wait limit, so it jumps the queue and the scores do not move.",
		winner,
		wait.Milliseconds(),
		budgetStatus,
		budget.Milliseconds(),
	)
	scenario.Steps = append(scenario.Steps, step)
}

func schedulerDocSnapshot(scheduler *Scheduler) ([]docScore, []docDepth) {
	scores := make([]docScore, 0, len(scheduler.slots))
	depths := make([]docDepth, 0, len(scheduler.byID))
	for _, group := range scheduler.slots {
		scores = append(scores, docScore{Group: group.id, Score: group.deficit})
		for _, lane := range group.lanes {
			depths = append(depths, docDepth{Lane: lane.spec.ID, Depth: len(lane.items)})
		}
	}
	return scores, depths
}

func schedulerDocScores(scheduler *Scheduler) []docScore {
	scores, _ := schedulerDocSnapshot(scheduler)
	return scores
}

func schedulerDocLaneTiming(scheduler *Scheduler, laneID string) (time.Duration, time.Duration) {
	lane := scheduler.byID[laneID]
	if lane == nil || len(lane.items) == 0 {
		return 0, 0
	}
	return scheduler.clock.Now().Sub(lane.items[0].EnqueuedAt), lane.spec.Budget
}

func docScoreFor(group string, scores []docScore) int {
	for _, score := range scores {
		if score.Group == group {
			return score.Score
		}
	}
	return 0
}

func docGroupDepth(group string, specs []LaneSpec, depths []docDepth) int {
	total := 0
	for _, spec := range specs {
		if docGroupName(spec) != group {
			continue
		}
		for _, depth := range depths {
			if depth.Lane == spec.ID {
				total += depth.Depth
				break
			}
		}
	}
	return total
}

func docGroupWeight(group string, specs []LaneSpec) int {
	for _, spec := range specs {
		if docGroupName(spec) == group {
			return spec.Weight
		}
	}
	return 0
}

func docGroupName(spec LaneSpec) string {
	if spec.Group != "" {
		return spec.Group
	}
	return spec.ID
}
