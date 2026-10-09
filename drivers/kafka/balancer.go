package kafka

import (
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

// defaultBalancerName is the protocol a consumer joins with when the option is
// unset.
//
// Cooperative-sticky is the default because it moves only the partitions a
// membership change has to move. An eager protocol is handed an empty current
// assignment on every rebalance and clears every fetch position with it, so
// every partition is re-fetched from its committed offset whether or not its
// owner changed, which is a duplicate and a re-warm per partition per
// membership change.
const defaultBalancerName = "cooperative-sticky"

// resolveBalancer returns the group balancer the consumer clients join with.
//
// The accepted values are the stock balancers franz-go ships, and nothing else:
// every member of a classic group has to agree on the protocol, so a name no
// other client advertises cannot be joined by a mixed fleet. The custom protocol
// this driver used to advertise is gone rather than aliased, so a group running
// it is moved by setting the new value on every member as one roll. A value the
// option carries is resolved, so naming it with nothing in it is refused rather
// than taken as the default: an empty value is a config that meant to set the
// protocol and did not.
//
// A group cannot be moved back from a cooperative protocol to an eager one
// without every partition being revoked and re-consumed, so a deployment that
// selects "sticky" or "range" after starting cooperative pays one rebalance of
// duplicate deliveries.
func resolveBalancer(options map[string]string) (kgo.GroupBalancer, error) {
	value, set := options["kafka.balancer"]
	if !set {
		value = defaultBalancerName
	}
	switch value {
	case "cooperative-sticky":
		return kgo.CooperativeStickyBalancer(), nil
	case "sticky":
		return kgo.StickyBalancer(), nil
	case "range":
		return kgo.RangeBalancer(), nil
	default:
		return nil, fmt.Errorf("kafka: invalid kafka.balancer %q; supported values are cooperative-sticky, sticky, range", value)
	}
}
