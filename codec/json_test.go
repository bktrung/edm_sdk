package codec_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	f1 "fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/codec"
)

func TestJSON_RoundTripsThroughAnyAndTypedStruct(t *testing.T) {
	t.Parallel()

	var jsonCodec codec.JSON
	require.Equal(t, "json", jsonCodec.Name())
	require.Equal(t, "application/json", jsonCodec.ContentType())

	var decodedAny map[string]any
	body := []byte(`{"order_id":"ORD-1","amount":42}`)
	require.NoError(t, jsonCodec.Decode(body, &decodedAny))
	encoded, err := jsonCodec.Encode(decodedAny)
	require.NoError(t, err)
	require.JSONEq(t, string(body), string(encoded))

	type order struct {
		OrderID string `json:"order_id"`
		Amount  int    `json:"amount"`
	}

	var decodedTyped order
	require.NoError(t, jsonCodec.Decode(body, &decodedTyped))
	require.Equal(t, order{OrderID: "ORD-1", Amount: 42}, decodedTyped)
	encoded, err = jsonCodec.Encode(decodedTyped)
	require.NoError(t, err)
	require.JSONEq(t, string(body), string(encoded))
}

func TestJSON_DecodeIgnoresUnknownFields(t *testing.T) {
	t.Parallel()

	var decoded struct {
		ID string `json:"id"`
	}

	err := (codec.JSON{}).Decode([]byte("{\"id\":\"evt-1\",\"future\":true}"), &decoded)
	require.NoError(t, err)
	require.Equal(t, "evt-1", decoded.ID)
}

func TestJSON_DecodeErrorIsUnclassified(t *testing.T) {
	t.Parallel()

	var decoded map[string]any
	err := (codec.JSON{}).Decode([]byte(`{"malformed"`), &decoded)
	require.Error(t, err)
	require.False(t, f1.IsTerminal(err))
}
