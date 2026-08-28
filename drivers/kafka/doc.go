// Package kafka is the Kafka adapter for the F1 driver port.
//
// The package reserves the adapter boundary and declares the capability
// ceiling the finished driver will report. Open returns driver.ErrUnsupported
// until the connection, producer, consumer, and topology surfaces exist.
package kafka
