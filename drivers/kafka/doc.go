// Package kafka is the Kafka adapter for the F1 driver port.
//
// A backlog sample performs the same three offset reads as Lag, plus one
// group-less Fetch per partition leader with positive lag. The probe never
// joins the consumer group, commits offsets, or uses the group's fetch session.
//
// The package provides the connected Kafka boundary and declares the
// capability ceiling the driver can report. Resource factories remain
// unsupported until their corresponding surfaces are implemented.
package kafka
