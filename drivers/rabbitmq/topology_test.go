package rabbitmq

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

// TestScanOrphansKeepsAParkingQueueInsideItsScope pins where a parking queue
// whose parent lies outside the caller's scope is reported: under its own name
// and count, never under the excluded parent's name. With the parent in scope
// the count is still folded into it.
func TestScanOrphansKeepsAParkingQueueInsideItsScope(t *testing.T) {
	for _, test := range []struct {
		name  string
		scope []string
		want  []driver.OrphanedDestination
	}{
		{
			name:  "parent out of scope",
			scope: []string{"orders.old.park"},
			want:  []driver.OrphanedDestination{{Name: "orders.old.park", Messages: 3}},
		},
		{
			name:  "parent in scope",
			scope: []string{"orders.old"},
			want:  []driver.OrphanedDestination{{Name: "orders.old", Messages: 5}},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.EscapedPath() != "/api/queues/%2F" {
					http.NotFound(w, r)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode([]managementQueue{
					{Name: "orders.old", Messages: 2, MessagesReady: 2},
					{Name: "orders.old.park", Messages: 3, MessagesReady: 3},
				})
			}))
			defer server.Close()
			client := &managementClient{baseURL: server.URL, vhost: "/", client: *server.Client()}
			admin := &adminOperations{conn: &conn{management: client}}

			var diff driver.TopologyDiff
			admin.scanOrphans(t.Context(), driver.TopologySpec{Scope: test.scope}, &diff)
			if diff.OrphanScanError != "" {
				t.Fatalf("OrphanScanError = %q, want empty", diff.OrphanScanError)
			}
			if len(diff.Orphaned) != len(test.want) {
				t.Fatalf("Orphaned = %+v, want %+v", diff.Orphaned, test.want)
			}
			for index, want := range test.want {
				if diff.Orphaned[index] != want {
					t.Fatalf("Orphaned[%d] = %+v, want %+v", index, diff.Orphaned[index], want)
				}
			}
		})
	}
}
