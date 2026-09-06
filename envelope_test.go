package f1_test

import (
	"bytes"
	"encoding/json"
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

func loadDoc03Attributes(t *testing.T) []docAttribute {
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

func TestEnvelope_MatchesDoc03Table(t *testing.T) {
	t.Parallel()

	attrs := loadDoc03Attributes(t)
	headers, err := fullEnvelope().EncodeHeaders(0)
	require.NoError(t, err)

	knownEnums := map[string][]string{
		"f1priority":    {f1.PriorityHigh.String(), f1.PriorityNormal.String(), f1.PriorityLow.String()},
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

	attrs := loadDoc03Attributes(t)
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
	require.False(t, e.DeathReason.Valid())

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
		"f1priority":      "normal",
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

func TestEnvelope_HeaderSizeGuardShedsExtensionsThenTruncatesDeathError(t *testing.T) {
	t.Parallel()

	e := fullEnvelope()
	e.Extensions = map[string]string{"ext-a": "aaaaaaaaaa", "ext-b": "bbbbbbbbbb"}

	small := 200
	withoutExt := e
	withoutExt.Extensions = nil
	withoutExtHeaders, err := withoutExt.EncodeHeaders(0)
	require.NoError(t, err)
	baseline := headerBytesOf(withoutExtHeaders)
	require.Greater(t, baseline, small, "test setup: baseline must already exceed the limit before Extensions is even added")

	h, err := e.EncodeHeaders(small)
	require.NoError(t, err)
	_, hasExtA := h["ext-a"]
	_, hasExtB := h["ext-b"]
	require.False(t, hasExtA)
	require.False(t, hasExtB, "Extensions must be shed first, before DeathError is touched")

	// An unachievable cap still returns the best-effort headers.
	e.DeathError = "a very long terminal error string that will need truncating to fit under the cap"
	tight, err := e.EncodeHeaders(120)
	require.NoError(t, err)
	require.LessOrEqual(t, len(tight["f1deatherror"]), len("a very long terminal error string that will need truncating to fit under the cap"))
}

func TestEnvelope_HeaderSizeGuardExtensionsAloneIsEnough(t *testing.T) {
	t.Parallel()

	withoutExt := fullEnvelope()
	baseline, err := withoutExt.EncodeHeaders(0)
	require.NoError(t, err)
	baselineBytes := headerBytesOf(baseline)

	e := fullEnvelope()
	e.Extensions = map[string]string{"ext-a": "aaaaaaaaaa"}
	withExt, err := e.EncodeHeaders(0)
	require.NoError(t, err)
	require.Greater(t, headerBytesOf(withExt), baselineBytes, "test setup: the extension must add bytes")

	limit := baselineBytes + 5 // fits without Extensions, not with them
	h, err := e.EncodeHeaders(limit)
	require.NoError(t, err)
	_, hasExt := h["ext-a"]
	require.False(t, hasExt)
	require.Equal(t, "boom", h["f1deatherror"], "DeathError must be untouched once dropping Extensions alone fits")
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
	require.Empty(t, headers["f1deatherror"])
}

func headerBytesOf(h map[string]string) int {
	n := 0
	for k, v := range h {
		n += len(k) + len(v)
	}
	return n
}

func TestEnvelope_SizeGuardDriverLimitBelowCoreCap(t *testing.T) {
	t.Parallel()

	e := fullEnvelope()
	e.Extensions = map[string]string{"ext-a": "aaaaaaaaaa"}
	h, err := e.EncodeHeaders(50)
	require.NoError(t, err)
	_, hasExt := h["ext-a"]
	require.False(t, hasExt)
}

func TestEnvelope_SizeGuardDeathErrorEmptyStopsShrinking(t *testing.T) {
	t.Parallel()

	e := fullEnvelope()
	e.DeathError = ""
	require.NotPanics(t, func() { _, _ = e.EncodeHeaders(1) })
}

func TestDeathReason_ValidIsTrueForEachOfTheSevenReasons(t *testing.T) {
	t.Parallel()

	for _, r := range []f1.DeathReason{
		f1.ReasonMaxAttempts, f1.ReasonTerminal, f1.ReasonPanic, f1.ReasonDecode,
		f1.ReasonExpired, f1.ReasonPoison, f1.ReasonUnmatched,
	} {
		require.True(t, r.Valid(), r)
	}
	require.False(t, f1.ReasonUnspecified.Valid())
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

func TestUnrecognisedValueError_Error(t *testing.T) {
	t.Parallel()

	_, err := f1.ParsePriority("urgent")
	require.EqualError(t, err, `f1: unrecognised f1priority value "urgent"`)
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

	require.True(t, f1.PriorityNormal.Valid())
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
	e.DeathError = "\U0001F4A5\U0001F4A3\U0001F525\U0001F4A7"
	withoutDeathError := e
	withoutDeathError.DeathError = ""
	baseHeaders, err := withoutDeathError.EncodeHeaders(0)
	require.NoError(t, err)
	limit := headerBytesOf(baseHeaders) + len("f1deatherror") + 5
	headers, err := e.EncodeHeaders(limit)
	require.NoError(t, err)
	require.True(t, utf8.ValidString(headers["f1deatherror"]))
	require.LessOrEqual(t, headerBytesOf(headers), limit)
}
