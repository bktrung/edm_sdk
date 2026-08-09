package driver

import "testing"

func TestBrokerInfo_DisplayComposesKindAndVersion(t *testing.T) {
	t.Parallel()

	if got := (BrokerInfo{
		Kind: "rabbitmq", Version: "4.3.0",
	}).Display(); got != "rabbitmq 4.3.0" {
		t.Fatalf("Display() = %q", got)
	}
	if got := (BrokerInfo{Kind: "rabbitmq"}).Display(); got != "rabbitmq" {
		t.Fatalf("Display() = %q", got)
	}
	if got := (BrokerInfo{Version: "4.3.0"}).Display(); got != "4.3.0" {
		t.Fatalf("Display() = %q", got)
	}
}
