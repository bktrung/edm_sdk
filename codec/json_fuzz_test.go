package codec_test

import (
	"testing"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/codec"
)

func FuzzJSONDecode(f *testing.F) {
	f.Add([]byte(`{"order_id":"ORD-1","amount":42}`))
	f.Add([]byte(`{"id":"evt-1","future":true}`))
	f.Add([]byte(`{"malformed"`))
	f.Add([]byte(`null`))
	f.Add([]byte(`[]`))

	f.Fuzz(func(t *testing.T, data []byte) {
		var decodedAny map[string]any
		_ = (codec.JSON{}).Decode(data, &decodedAny)

		var decodedStruct struct {
			ID      string `json:"id"`
			OrderID string `json:"order_id"`
			Amount  int    `json:"amount"`
		}
		_ = (codec.JSON{}).Decode(data, &decodedStruct)
	})
}
