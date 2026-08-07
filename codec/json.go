package codec

import "encoding/json"

// JSON is the v1 JSON payload codec.
type JSON struct{}

var _ Codec = JSON{}

// Name returns the codec identifier used for JSON payloads.
func (JSON) Name() string {
	return "json"
}

// ContentType returns JSON's media type.
func (JSON) ContentType() string {
	return "application/json"
}

// Encode serializes v as JSON.
func (JSON) Encode(v any) ([]byte, error) {
	return json.Marshal(v)
}

// Decode deserializes JSON into v. Unknown object fields are ignored.
func (JSON) Decode(data []byte, v any) error {
	return json.Unmarshal(data, v)
}
