package f1_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"log/slog"
	"maps"
	"os"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/stretchr/testify/require"

	f1 "fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk"
)

type docAttribute struct {
	Attribute string   `json:"attribute"`
	Type      string   `json:"type"`
	SetBy     string   `json:"setBy"`
	Enum      []string `json:"enum,omitempty"`
}

type docAttributeFixture struct {
	Attributes []docAttribute `json:"attributes"`
}

func loadAttributeFixture(t *testing.T) []docAttribute {
	t.Helper()
	data, err := os.ReadFile("testdata/envelope-attributes.json")
	require.NoError(t, err)
	var fixture docAttributeFixture
	require.NoError(t, json.Unmarshal(data, &fixture))
	require.NotEmpty(t, fixture.Attributes)
	return fixture.Attributes
}

func fullEnvelope() f1.Envelope {
	due := time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC)
	death := time.Date(2026, 8, 5, 13, 0, 0, 0, time.UTC)
	expiry := time.Date(2026, 8, 6, 0, 0, 0, 0, time.UTC)
	return f1.Envelope{
		SpecVersion:    "1.0",
		ID:             "evt-1",
		Source:         "/prod/ingest-api",
		Type:           "com.za.order.created.v1",
		Time:           time.Date(2026, 8, 5, 11, 0, 0, 0, time.UTC),
		IdempotencyKey: "evt-1",
		Priority:       f1.PriorityHigh,
		Attempt:        1,
		MaxAttempts:    5,
		DueTime:        &due,
		OriginalDest:   "com.za.order.created",
		CorrelationID:  "corr-1",
		CausationID:    "cause-1",
		Producer:       "ingest-api/1.0/pod-7",
		PartitionKey:   "ORD-88213",
		Expiry:         &expiry,
		DeathError:     "boom",
		DeathReason:    f1.ReasonMaxAttempts,
		DeathTime:      &death,
		TraceParent:    "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		TraceState:     "congo=t61rcWkgMzE",
	}
}

func minimalEnvelope() f1.Envelope {
	return f1.Envelope{
		SpecVersion: "1.0",
		ID:          "i",
		Source:      "s",
		Type:        "t",
		Time:        time.Date(2026, 8, 5, 11, 0, 0, 0, time.UTC),
		Priority:    f1.PriorityMedium,
	}
}

func TestEnvelope_EncodeHeadersEnforcesCapAcrossTiers(t *testing.T) {
	t.Parallel()

	base := minimalEnvelope()
	protocol := base
	protocol.IdempotencyKey = strings.Repeat("p", 512)
	descriptive := base
	descriptive.Subject = strings.Repeat("s", 300)
	death := base
	death.DeathError = strings.Repeat("e", 2048)

	tests := []struct {
		name     string
		envelope f1.Envelope
		limit    int
		wantErr  bool
		shed     string
	}{
		{name: "mandatory", envelope: base, limit: 1, wantErr: true},
		{name: "protocol", envelope: protocol, limit: 200, wantErr: true},
		{name: "descriptive", envelope: descriptive, limit: 200, shed: "subject"},
		{name: "death", envelope: death, limit: 200},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			headers, err := tt.envelope.EncodeHeaders(tt.limit)
			if tt.wantErr {
				require.ErrorIs(t, err, f1.ErrEnvelopeTooLarge)
				require.Nil(t, headers)
				return
			}
			require.NoError(t, err)
			require.LessOrEqual(t, headerBytesOf(headers), tt.limit)
			if tt.shed != "" {
				require.NotContains(t, headers, tt.shed)
			}
		})
	}
}

func TestEnvelope_EncodeHeadersBackstopRejectsUnsheddableProtocolOverflow(t *testing.T) {
	t.Parallel()

	e := minimalEnvelope()
	e.Subject = strings.Repeat("s", 300)
	e.IdempotencyKey = strings.Repeat("p", 300)

	headers, err := e.EncodeHeaders(200)
	require.ErrorIs(t, err, f1.ErrEnvelopeTooLarge)
	require.Nil(t, headers)
}

func TestEnvelope_EncodeHeadersNeverShedsProtocolHeaders(t *testing.T) {
	t.Parallel()

	due := time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC)
	expiry := time.Date(2026, 8, 6, 0, 0, 0, 0, time.UTC)
	e := minimalEnvelope()
	e.Subject = strings.Repeat("s", 300)
	e.IdempotencyKey = "idempotency"
	e.Attempt = 7
	e.MaxAttempts = 9
	e.DueTime = &due
	e.OriginalDest = "orders.created"
	e.CorrelationID = "correlation"
	e.CausationID = "causation"
	e.Producer = "producer"
	e.PartitionKey = "partition"
	e.Expiry = &expiry

	withoutSubject := e
	withoutSubject.Subject = ""
	expected, err := withoutSubject.EncodeHeaders(0)
	require.NoError(t, err)

	headers, err := e.EncodeHeaders(headerBytesOf(expected))
	require.NoError(t, err)
	require.Equal(t, expected["f1idempotencykey"], headers["f1idempotencykey"])
	require.Equal(t, expected["f1priority"], headers["f1priority"])
	require.Equal(t, expected["f1attempt"], headers["f1attempt"])
	require.Equal(t, expected["f1maxattempts"], headers["f1maxattempts"])
	require.Equal(t, expected["f1duetime"], headers["f1duetime"])
	require.Equal(t, expected["f1originaldest"], headers["f1originaldest"])
	require.Equal(t, expected["f1correlationid"], headers["f1correlationid"])
	require.Equal(t, expected["f1causationid"], headers["f1causationid"])
	require.Equal(t, expected["f1producer"], headers["f1producer"])
	require.Equal(t, expected["f1partitionkey"], headers["f1partitionkey"])
	require.Equal(t, expected["f1expiry"], headers["f1expiry"])
	require.NotContains(t, headers, "subject")
	e.IdempotencyKey = strings.Repeat("p", 512)
	_, err = e.EncodeHeaders(headerBytesOf(expected))
	require.ErrorIs(t, err, f1.ErrEnvelopeTooLarge)
}

func TestEnvelope_EncodeHeadersShedsDescriptiveHeadersInOrder(t *testing.T) {
	t.Parallel()

	value := strings.Repeat("x", 100)
	e := minimalEnvelope()
	e.Forwarded = map[string]string{"f1future": value}
	e.TraceState = value
	e.DataSchema = value
	e.DataContentType = value
	e.Subject = value
	e.TraceParent = value

	all, err := e.EncodeHeaders(0)
	require.NoError(t, err)
	order := []string{"f1future", "tracestate", "dataschema", "datacontenttype", "subject", "traceparent"}
	for i := range order {
		after := cloneHeaders(all)
		for _, shed := range order[:i+1] {
			delete(after, shed)
		}

		headers, err := e.EncodeHeaders(headerBytesOf(after))
		require.NoError(t, err)
		require.LessOrEqual(t, headerBytesOf(headers), headerBytesOf(after))
		for j, candidate := range order {
			if j <= i {
				require.NotContains(t, headers, candidate)
			} else {
				require.Equal(t, value, headers[candidate])
			}
		}
	}
}

func TestEnvelope_EncodeHeadersShedTiersRetainMandatoryHeaders(t *testing.T) {
	t.Parallel()

	e := minimalEnvelope()
	e.Extensions = map[string]string{"x-extension": strings.Repeat("x", 100)}
	e.Subject = strings.Repeat("s", 100)
	e.DeathError = strings.Repeat("e", 2048)

	base, err := minimalEnvelope().EncodeHeaders(0)
	require.NoError(t, err)
	limit := headerBytesOf(base) + 20

	headers, err := e.EncodeHeaders(limit)
	require.NoError(t, err)
	require.LessOrEqual(t, headerBytesOf(headers), limit)
	require.Equal(t, "1.0", headers["specversion"])
	require.Equal(t, "i", headers["id"])
	require.Equal(t, "s", headers["source"])
	require.Equal(t, "t", headers["type"])
	require.NotEmpty(t, headers["time"])
	require.NotContains(t, headers, "x-extension")
	require.NotContains(t, headers, "subject")
	expectedDeathErrorBytes := limit - headerBytesOf(base) - len("f1deatherror")
	require.Equal(t, expectedDeathErrorBytes, len(headers["f1deatherror"]))
}

func TestEnvelope_EncodeHeadersKeepsForwardedWhenDeathDetailsShedFits(t *testing.T) {
	t.Parallel()

	e := minimalEnvelope()
	e.Forwarded = map[string]string{"f1future": strings.Repeat("f", 100)}
	e.DeathError = strings.Repeat("e", 900)
	e.DeathDetails = map[string]string{"tenant": strings.Repeat("d", 100)}

	atFloor := e
	atFloor.DeathError = strings.Repeat("e", 512)
	atFloor.DeathDetails = nil
	expected, err := atFloor.EncodeHeaders(0)
	require.NoError(t, err)

	headers, err := e.EncodeHeaders(headerBytesOf(expected))
	require.NoError(t, err)
	require.Equal(t, strings.Repeat("f", 100), headers["f1future"])
	require.NotContains(t, headers, "f1detailtenant")
	require.Equal(t, 512, len(headers["f1deatherror"]))
	require.LessOrEqual(t, headerBytesOf(headers), headerBytesOf(expected))
}

func TestEnvelope_EncodeHeadersPreservesDeathErrorFloorBeforeDescriptive(t *testing.T) {
	t.Parallel()

	e := minimalEnvelope()
	e.DeathError = strings.Repeat("e", 900)
	e.Subject = strings.Repeat("s", 300)

	atFloor := e
	atFloor.DeathError = strings.Repeat("e", 512)
	atFloor.Subject = ""
	expected, err := atFloor.EncodeHeaders(0)
	require.NoError(t, err)

	headers, err := e.EncodeHeaders(headerBytesOf(expected))
	require.NoError(t, err)
	require.Equal(t, 512, len(headers["f1deatherror"]))
	require.NotContains(t, headers, "subject")
	require.LessOrEqual(t, headerBytesOf(headers), headerBytesOf(expected))
}

func TestEnvelope_MatchesAttributeFixture(t *testing.T) {
	t.Parallel()

	attrs := loadAttributeFixture(t)
	headers, err := fullEnvelope().EncodeHeaders(0)
	require.NoError(t, err)

	knownEnums := map[string][]string{
		"f1priority":    {f1.PriorityHigh.String(), f1.PriorityMedium.String(), f1.PriorityLow.String()},
		"f1deathreason": deathReasonWireValues(),
	}

	for _, attr := range attrs {
		v, ok := headers[attr.Attribute]
		require.Truef(t, ok, "fixture declares %s but EncodeHeaders never wrote it", attr.Attribute)

		switch attr.Type {
		case "int":
			require.Regexpf(t, `^-?\d+$`, v, "%s: fixture types it as int", attr.Attribute)
		case "RFC3339":
			_, err := time.Parse(time.RFC3339Nano, v)
			require.NoErrorf(t, err, "%s: fixture types it as RFC3339", attr.Attribute)
		case "enum (string)":
			require.Containsf(t, attr.Enum, v, "%s: %q is not a declared fixture value", attr.Attribute, v)
			gotValues, ok := knownEnums[attr.Attribute]
			require.Truef(t, ok, "%s: test has no Go-side enum mapping to check the fixture against", attr.Attribute)
			require.ElementsMatchf(t, attr.Enum, gotValues, "%s: fixture values and Go constants differ", attr.Attribute)
		case "string":
		default:
			t.Fatalf("%s: unrecognised fixture type %q - test needs updating", attr.Attribute, attr.Type)
		}
	}
}

func deathReasonWireValues() []string {
	return []string{
		f1.ReasonMaxAttempts.String(), f1.ReasonTerminal.String(), f1.ReasonPanic.String(),
		f1.ReasonDecode.String(), f1.ReasonExpired.String(), f1.ReasonPoison.String(),
		f1.ReasonUnmatched.String(),
	}
}

func TestEnvelope_FixtureCoversEveryF1HeaderEmitted(t *testing.T) {
	t.Parallel()

	attrs := loadAttributeFixture(t)
	known := make(map[string]bool, len(attrs))
	for _, a := range attrs {
		known[a.Attribute] = true
	}

	headers, err := fullEnvelope().EncodeHeaders(0)
	require.NoError(t, err)
	for k := range headers {
		if !strings.HasPrefix(k, "f1") {
			continue // Core and optional attributes are outside the fixture.
		}
		require.Truef(t, known[k], "EncodeHeaders emitted %q but the fixture has no row for it", k)
	}
}

func TestEnvelope_HeaderRoundTripIsLossless(t *testing.T) {
	t.Parallel()

	e := fullEnvelope()
	e.DataContentType = "application/json"
	e.Subject = "ORD-88213"
	e.DataSchema = "https://schemas.example/order.v1.json"
	e.Expiry = timePtr(time.Date(2026, 8, 6, 0, 0, 0, 0, time.UTC))
	e.Extensions = map[string]string{"x-custom-ext": "value"}

	headers, err := e.EncodeHeaders(0)
	require.NoError(t, err)
	got, err := f1.DecodeHeaders(headers)
	require.NoError(t, err)
	require.Equal(t, e, got)
}

func TestEnvelope_DeathDetailsRoundTripAndRejectInvalidKeys(t *testing.T) {
	t.Parallel()

	e := fullEnvelope()
	e.DeathDetails = map[string]string{
		"tenant":          "acme",
		"step":            "charge",
		"with_underscore": "discard",
		"Capital":         "discard",
		"bad-key":         "discard",
	}
	headers, err := e.EncodeHeaders(0)
	require.NoError(t, err)
	require.Equal(t, "acme", headers["f1detailtenant"])
	require.Equal(t, "charge", headers["f1detailstep"])
	require.NotContains(t, headers, "f1detailwith_underscore")
	require.NotContains(t, headers, "f1detailCapital")
	require.NotContains(t, headers, "f1detailbad-key")

	got, err := f1.DecodeHeaders(headers)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"tenant": "acme", "step": "charge"}, got.DeathDetails)
	require.NotContains(t, got.Forwarded, "f1detailtenant")
	require.NotContains(t, got.Extensions, "f1detailtenant")

	e.Extensions = map[string]string{"f1detailtenant": "must reject"}
	_, err = e.EncodeHeaders(0)
	require.ErrorIs(t, err, f1.ErrReservedExtension)
}

func TestEnvelope_InvalidDeathDetailKeyIsSilent(t *testing.T) {
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })

	envelope := fullEnvelope()
	envelope.DeathDetails = map[string]string{
		"tenant":  "acme",
		"BAD_KEY": "secret",
	}
	headers, err := envelope.EncodeHeaders(0)
	require.NoError(t, err)
	require.Equal(t, "acme", headers["f1detailtenant"])
	require.NotContains(t, headers, "f1detailBAD_KEY")
	require.Empty(t, logs.String())
}

func timePtr(t time.Time) *time.Time { return &t }

func TestEnvelope_TracedMessageSurvivesDecodeEncodeRoundTrip(t *testing.T) {
	t.Parallel()

	e := fullEnvelope()
	require.NotEmpty(t, e.TraceParent)
	require.NotEmpty(t, e.TraceState)

	headers, err := e.EncodeHeaders(0)
	require.NoError(t, err)
	require.Equal(t, e.TraceParent, headers["traceparent"])
	require.Equal(t, e.TraceState, headers["tracestate"])

	got, err := f1.DecodeHeaders(headers)
	require.NoError(t, err)
	require.Equal(t, e, got)
	require.NotContains(t, got.Extensions, "traceparent")
	require.NotContains(t, got.Extensions, "tracestate")

	replayed, err := got.EncodeHeaders(0)
	require.NoError(t, err)
	require.Equal(t, headers, replayed)
}

func TestDLQ_UnknownReasonSurvivesReplay(t *testing.T) {
	t.Parallel()

	headers, err := fullEnvelope().EncodeHeaders(0)
	require.NoError(t, err)
	headers["f1deathreason"] = "a_reason_this_build_predates"

	e, err := f1.DecodeHeaders(headers)
	require.NoError(t, err, "an unrecognised death reason must not fail the decode")
	require.Equal(t, f1.DeathReason("a_reason_this_build_predates"), e.DeathReason)

	replayed, err := e.EncodeHeaders(0)
	require.NoError(t, err)
	require.Equal(t, "a_reason_this_build_predates", replayed["f1deathreason"])
}

func TestArchivedDependencyUnavailableReasonSurvivesReplay(t *testing.T) {
	t.Parallel()

	headers := map[string]string{
		"specversion":     "1.0",
		"id":              "archived-dependency-outage",
		"source":          "/legacy/orders",
		"type":            "orders.created.v1",
		"f1priority":      "medium",
		"f1attempt":       "2",
		"f1deathreason":   "dependency_unavailable",
		"f1correlationid": "archived-dependency-outage",
	}

	e, err := f1.DecodeHeaders(headers)
	require.NoError(t, err)
	require.Equal(t, "dependency_unavailable", e.DeathReason.String())
	replayed, err := e.EncodeHeaders(0)
	require.NoError(t, err)
	require.Equal(t, "dependency_unavailable", replayed["f1deathreason"])
}

func TestMaxAttempts_AbsentUnlessProducerSets(t *testing.T) {
	t.Parallel()

	e := f1.Envelope{ID: "evt-1", Source: "/prod/svc", Type: "com.za.x.v1"}
	h1, err := e.EncodeHeaders(0)
	require.NoError(t, err)
	_, ok := h1["f1maxattempts"]
	require.False(t, ok, "f1maxattempts must be absent when the producer set no cap")

	e.MaxAttempts = 3
	h2, err := e.EncodeHeaders(0)
	require.NoError(t, err)
	v, ok := h2["f1maxattempts"]
	require.True(t, ok)
	require.Equal(t, "3", v)
}

func TestEnvelope_DecodeHeadersRejectsNegativeMaxAttempts(t *testing.T) {
	t.Parallel()

	headers, err := fullEnvelope().EncodeHeaders(0)
	require.NoError(t, err)
	headers["f1maxattempts"] = "-1"

	_, err = f1.DecodeHeaders(headers)
	require.Error(t, err)
	require.Contains(t, err.Error(), "f1maxattempts")
}

func TestEnvelope_DecodeHeadersPreservesOverRangeMaxAttempts(t *testing.T) {
	t.Parallel()

	headers, err := fullEnvelope().EncodeHeaders(0)
	require.NoError(t, err)
	headers["f1maxattempts"] = "1000000"

	got, err := f1.DecodeHeaders(headers)
	require.NoError(t, err)
	require.Equal(t, 1000000, got.MaxAttempts)
}

func TestCorrelationID_RootDefault(t *testing.T) {
	t.Parallel()

	e := f1.Envelope{ID: "evt-root"}
	h1, err := e.EncodeHeaders(0)
	require.NoError(t, err)
	require.Equal(t, "evt-root", h1["f1correlationid"])

	e.CorrelationID = "saga-42"
	h2, err := e.EncodeHeaders(0)
	require.NoError(t, err)
	require.Equal(t, "saga-42", h2["f1correlationid"])
}

func TestEnvelope_HeaderSizeGuardShedsExtensionsAndTraceBeforeDeathError(t *testing.T) {
	t.Parallel()

	e := fullEnvelope()
	e.Extensions = map[string]string{"ext-a": "aaaaaaaaaa", "ext-b": "bbbbbbbbbb"}

	withoutExt := e
	withoutExt.Extensions = nil
	withoutExtHeaders, err := withoutExt.EncodeHeaders(0)
	require.NoError(t, err)
	baseline := headerBytesOf(withoutExtHeaders)

	withExtHeaders, err := e.EncodeHeaders(0)
	require.NoError(t, err)
	small := baseline + 5
	require.Greater(t, headerBytesOf(withExtHeaders), small, "test setup: the Extensions must add more bytes than the available headroom")

	h, err := e.EncodeHeaders(small)
	require.NoError(t, err)
	_, hasExtA := h["ext-a"]
	_, hasExtB := h["ext-b"]
	require.False(t, hasExtA)
	require.False(t, hasExtB, "Extensions must be shed first, before DeathError is touched")
	require.Equal(t, "boom", h["f1deatherror"], "DeathError must be untouched once dropping Extensions alone fits")

	// An unachievable cap reports that the mandatory and non-sheddable headers cannot fit.
	e.DeathError = "a terminal error longer than the headroom but shorter than the floor"
	_, err = e.EncodeHeaders(80)
	require.Error(t, err)
	require.True(t, errors.Is(err, f1.ErrEnvelopeTooLarge))

	e.DeathError = "a terminal error longer than the headroom but shorter than the floor"
	tight, err := e.EncodeHeaders(small)
	require.NoError(t, err)
	require.LessOrEqual(t, headerBytesOf(tight), small)
	require.Equal(t, e.DeathError, tight["f1deatherror"], "a DeathError under the floor must stay whole")
	require.NotContains(t, tight, "traceparent")
	require.NotContains(t, tight, "tracestate")
}

func TestEnvelope_SizeGuardRetainsDetailsAfterErrorFloor(t *testing.T) {
	t.Parallel()

	e := fullEnvelope()
	e.DeathError = strings.Repeat("💥", 400)
	e.DeathDetails = map[string]string{"tenant": "acme", "step": "charge"}

	atFloor := e
	atFloor.DeathError = strings.Repeat("💥", 128)
	floorHeaders, err := atFloor.EncodeHeaders(0)
	require.NoError(t, err)

	headers, err := e.EncodeHeaders(headerBytesOf(floorHeaders))
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(headers["f1deatherror"]), 512)
	require.True(t, utf8.ValidString(headers["f1deatherror"]))
	require.Equal(t, "acme", headers["f1detailtenant"])
	require.Equal(t, "charge", headers["f1detailstep"])
}

func TestEnvelope_SizeGuardFloorRuneBoundaryTerminates(t *testing.T) {
	t.Parallel()

	e := fullEnvelope()
	e.DeathError = "é" + strings.Repeat("💥", 250)
	headers, err := e.EncodeHeaders(1000)
	require.NoError(t, err)
	require.Greater(t, len(headers["f1deatherror"]), 0)
	require.True(t, utf8.ValidString(headers["f1deatherror"]))
	require.LessOrEqual(t, headerBytesOf(headers), 1000)
}

func TestEnvelope_SizeGuardShedsAllDetailsAtErrorFloor(t *testing.T) {
	t.Parallel()

	atFloor := fullEnvelope()
	atFloor.DeathError = strings.Repeat("e", 512)
	floorHeaders, err := atFloor.EncodeHeaders(0)
	require.NoError(t, err)
	limit := headerBytesOf(floorHeaders)

	e := atFloor
	e.DeathError = strings.Repeat("e", 2048)
	e.DeathDetails = map[string]string{"tenant": "acme", "step": "charge"}
	headers, err := e.EncodeHeaders(limit)
	require.NoError(t, err)
	require.Equal(t, 512, len(headers["f1deatherror"]))
	require.NotContains(t, headers, "f1detailtenant")
	require.NotContains(t, headers, "f1detailstep")
	require.LessOrEqual(t, headerBytesOf(headers), limit)
}

func TestEnvelope_SizeGuardTruncatesErrorBelowFloorWhenDetailsAbsent(t *testing.T) {
	t.Parallel()

	withoutError := fullEnvelope()
	withoutError.DeathError = ""
	baseHeaders, err := withoutError.EncodeHeaders(0)
	require.NoError(t, err)

	e := withoutError
	e.DeathError = strings.Repeat("e", 2048)
	headers, err := e.EncodeHeaders(headerBytesOf(baseHeaders))
	require.NoError(t, err)
	require.LessOrEqual(t, headerBytesOf(headers), headerBytesOf(baseHeaders))
	withoutTrace := cloneHeaders(baseHeaders)
	delete(withoutTrace, "traceparent")
	delete(withoutTrace, "tracestate")
	require.NotContains(t, headers, "traceparent")
	require.NotContains(t, headers, "tracestate")
	expectedDeathErrorBytes := headerBytesOf(baseHeaders) - headerBytesOf(withoutTrace) - len("f1deatherror")
	require.Equal(t, expectedDeathErrorBytes, len(headers["f1deatherror"]))
}

func headerBytesOf(h map[string]string) int {
	n := 0
	for k, v := range h {
		n += len(k) + len(v)
	}
	return n
}

func TestEnvelope_DecodeHeadersRejectsMalformedValues(t *testing.T) {
	t.Parallel()

	base, err := fullEnvelope().EncodeHeaders(0)
	require.NoError(t, err)

	cases := map[string]string{
		"time":          "not-a-time",
		"f1attempt":     "not-an-int",
		"f1maxattempts": "not-an-int",
		"f1duetime":     "not-a-time",
		"f1expiry":      "not-a-time",
		"f1deathtime":   "not-a-time",
		"f1priority":    "urgent",
	}
	for key, bad := range cases {
		h := map[string]string{}
		maps.Copy(h, base)
		h[key] = bad
		_, err := f1.DecodeHeaders(h)
		require.Errorf(t, err, "expected DecodeHeaders to reject %s=%q", key, bad)
	}
}

func TestParsePriorityRejectsUnrecognisedValues(t *testing.T) {
	t.Parallel()

	_, err := f1.ParsePriority("normal")
	var valueErr *f1.UnrecognisedValueError
	require.ErrorAs(t, err, &valueErr)
	require.Equal(t, "f1priority", valueErr.Attribute)
	require.Equal(t, "normal", valueErr.Value)
	require.EqualError(t, err, `f1: unrecognised f1priority value "normal"`)
}

func TestEnvelope_RejectsExtensionOverwritingCanonicalHeader(t *testing.T) {
	t.Parallel()

	e := fullEnvelope()
	e.Extensions = map[string]string{"f1priority": "urgent"}
	_, err := e.EncodeHeaders(0)
	require.ErrorIs(t, err, f1.ErrReservedExtension)

	e.Extensions = map[string]string{"id": "hijacked"}
	_, err = e.EncodeHeaders(0)
	require.ErrorIs(t, err, f1.ErrReservedExtension)

	for _, k := range []string{"traceparent", "tracestate"} {
		e.Extensions = map[string]string{k: "x"}
		_, err = e.EncodeHeaders(0)
		require.ErrorIsf(t, err, f1.ErrReservedExtension, "key %q must be rejected", k)
	}

	e.Extensions = map[string]string{"x-custom-ext": "value"}
	_, err = e.EncodeHeaders(0)
	require.NoError(t, err)
}

func TestEnvelope_RejectsReservedPrefixExtension(t *testing.T) {
	t.Parallel()

	for _, k := range []string{"f1bogus", "ce_type", "ce-type", "f1", "x-death", "x-first-death-queue"} {
		e := fullEnvelope()
		e.Extensions = map[string]string{k: "x"}
		_, err := e.EncodeHeaders(0)
		require.ErrorIsf(t, err, f1.ErrReservedExtension, "key %q must be rejected as a reserved prefix", k)
	}

	e := fullEnvelope()
	e.Extensions = map[string]string{"x-custom-ext": "value"}
	_, err := e.EncodeHeaders(0)
	require.NoError(t, err)
}

func TestEnvelope_UnknownReservedAttributeSurvivesRoundTrip(t *testing.T) {
	t.Parallel()

	h := map[string]string{
		"specversion":   "1.0",
		"id":            "evt-1",
		"source":        "/prod/ingest-api",
		"type":          "com.za.order.created.v1",
		"time":          "2026-08-05T11:00:00Z",
		"f1priority":    "high",
		"f1futurething": "future-value",
	}

	e, err := f1.DecodeHeaders(h)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"f1futurething": "future-value"}, e.Forwarded)
	require.Empty(t, e.Extensions)

	encoded, err := e.EncodeHeaders(0)
	require.NoError(t, err)
	require.Equal(t, "future-value", encoded["f1futurething"])
}

func TestEnvelope_ForwardedNeverOverwritesCanonicalAttribute(t *testing.T) {
	t.Parallel()

	e := fullEnvelope()
	e.Forwarded = map[string]string{"f1priority": "low"}

	h, err := e.EncodeHeaders(0)
	require.NoError(t, err)
	require.Equal(t, "high", h["f1priority"])
}

func TestEnvelope_CallerKeysBeginningCeAreNotReserved(t *testing.T) {
	t.Parallel()

	for _, k := range []string{"celery-task-id", "census-id", "cell", "certificate"} {
		e := fullEnvelope()
		e.Extensions = map[string]string{k: "x"}
		_, err := e.EncodeHeaders(0)
		require.NoErrorf(t, err, "key %q must remain available to callers", k)
	}

	for _, k := range []string{"ce_type", "ce-type"} {
		e := fullEnvelope()
		e.Extensions = map[string]string{k: "x"}
		_, err := e.EncodeHeaders(0)
		require.ErrorIsf(t, err, f1.ErrReservedExtension, "key %q must remain reserved", k)
	}
}

func TestPriority_UndeclaredValueDoesNotBecomeALane(t *testing.T) {
	t.Parallel()

	undeclared := f1.Priority(99)
	require.Equal(t, "invalid", undeclared.String())
	require.False(t, undeclared.Valid())

	require.True(t, f1.PriorityMedium.Valid())
	require.True(t, f1.PriorityHigh.Valid())
	require.True(t, f1.PriorityLow.Valid())

	e := fullEnvelope()
	e.Priority = undeclared
	_, err := e.EncodeHeaders(0)
	require.ErrorIs(t, err, f1.ErrInvalidPriority)
}

func TestEnvelope_DecodeHeadersRejectsMissingRequiredAttributes(t *testing.T) {
	t.Parallel()
	base := map[string]string{
		"specversion": "1.0",
		"id":          "evt-1",
		"source":      "/prod/orders",
		"type":        "orders.created.v1",
	}
	for _, attribute := range []string{"specversion", "id", "source", "type"} {
		t.Run("missing "+attribute, func(t *testing.T) {
			h := cloneHeaders(base)
			delete(h, attribute)
			_, err := f1.DecodeHeaders(h)
			require.Error(t, err)
			require.Contains(t, err.Error(), attribute)
		})
		t.Run("empty "+attribute, func(t *testing.T) {
			h := cloneHeaders(base)
			h[attribute] = ""
			_, err := f1.DecodeHeaders(h)
			require.Error(t, err)
			require.Contains(t, err.Error(), attribute)
		})
	}
	h := cloneHeaders(base)
	h["specversion"] = "0.3"
	_, err := f1.DecodeHeaders(h)
	require.EqualError(t, err, `f1: header specversion must be "1.0", got "0.3"`)
}

func cloneHeaders(input map[string]string) map[string]string {
	clone := make(map[string]string, len(input))
	maps.Copy(clone, input)
	return clone
}

func TestEnvelope_DeathErrorShrinkPreservesUTF8(t *testing.T) {
	t.Parallel()
	e := fullEnvelope()
	e.DeathError = strings.Repeat("\U0001F4A5", 200)
	withoutDeathError := e
	withoutDeathError.DeathError = ""
	baseHeaders, err := withoutDeathError.EncodeHeaders(0)
	require.NoError(t, err)
	headers, err := e.EncodeHeaders(headerBytesOf(baseHeaders))
	require.NoError(t, err)
	require.NotEmpty(t, headers["f1deatherror"])
	require.True(t, utf8.ValidString(headers["f1deatherror"]))
	require.LessOrEqual(t, headerBytesOf(headers), headerBytesOf(baseHeaders))
}
