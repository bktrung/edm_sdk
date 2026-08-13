package f1

// PartitionKeyOf returns the partition key, falling back to Subject and then ID.
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
