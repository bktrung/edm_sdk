package codec

// Codec encodes and decodes message payloads.
type Codec interface {
	// Name returns the codec identifier used by datacontenttype selection.
	Name() string
	// ContentType returns the media type emitted by the codec.
	ContentType() string
	// Encode serializes v into the codec's wire representation.
	Encode(v any) ([]byte, error)
	// Decode deserializes data into v. Unknown object fields are ignored.
	Decode(data []byte, v any) error
}
