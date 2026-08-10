package conformance

import "fmt"

func validateInspectorFactory(inspect Inspect, factoryErr error) error {
	if factoryErr != nil {
		return factoryErr
	}
	if inspect == nil {
		return fmt.Errorf("NewInspector returned a nil Inspect")
	}
	return nil
}

func validateInspectorDelta(before, afterPublish, afterReceive, afterSettle BrokerView, n int) error {
	if afterPublish.Ready-before.Ready != int64(n) {
		return fmt.Errorf("ready delta: before=%+v after_publish=%+v", before, afterPublish)
	}
	if afterReceive.Ready != before.Ready || afterReceive.Unsettled-before.Unsettled != int64(n) {
		return fmt.Errorf("receive delta: before=%+v after_receive=%+v", before, afterReceive)
	}
	if afterSettle != before {
		return fmt.Errorf("settle delta: before=%+v after_settle=%+v", before, afterSettle)
	}
	return nil
}
