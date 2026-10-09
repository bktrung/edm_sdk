package codec

// Codec encodes and decodes message payloads.
//
// Implementations return their codec identifier and media type so callers can
// select a codec and set the matching content type.
type Codec interface {
	// Name returns the identifier used to select this codec.
	Name() string
	// ContentType returns the media type emitted by the codec.
	ContentType() string
	// Encode serializes v into the codec's wire representation and returns an
	// error if v cannot be encoded.
	Encode(v any) ([]byte, error)
	// Decode deserializes data into v and returns an error if it cannot be
	// decoded.
	Decode(data []byte, v any) error
}
