package obs

import "testing"

func TestNewMetricsWithoutProviderIsNoop(t *testing.T) {
	metrics, err := NewMetrics(nil)
	if err != nil {
		t.Fatal(err)
	}
	called := false
	unregister := metrics.Register("orders", func() map[string]int64 {
		called = true
		return map[string]int64{"orders.created": 1}
	}, func() int64 {
		called = true
		return 1
	})
	unregister()
	if called {
		t.Fatal("metric sources ran without a meter provider")
	}
	if err := metrics.Close(); err != nil {
		t.Fatal(err)
	}
}
