package f1

// PartitionKeyOf returns the ordering identity doc 03 §3.2 defines:
// f1partitionkey, falling back to subject, falling back to id. Kafka hashes
// this as the record key. It is a DIFFERENT projection of the envelope than
// RoutingKey below, and nothing else in this package derives it (doc 03
// §3.2, F-P1).
func PartitionKeyOf(e Envelope) string {
	switch {
	case e.PartitionKey != "":
		return e.PartitionKey
	case e.Subject != "":
		return e.Subject
	default:
		return e.ID
	}
}

// RoutingKey composes the AMQP lane-selection string doc 03 §3.2 defines:
// <priority>.<partition key>, with lane queues bound as <priority>.#. This
// is the ONLY function that builds it - a bare partition key as the routing
// key cannot work with doc 02 §6's queue-per-(subscription, priority)
// topology, since the only binding that matches every message is "#" and
// the priority lanes collapse into copies of one stream (F-P1). Priority
// marshals through its String() wire form here, never its int value:
// RoutingKey(...) produces "high.customer-42", never "1.customer-42".
func RoutingKey(e Envelope) string {
	return e.Priority.String() + "." + PartitionKeyOf(e)
}
