package f1

import (
	"strings"
	"testing"
)

func TestEndpointAddress(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		endpoint string
		host     string
		port     int
	}{
		{name: "host port", endpoint: "h:5672", host: "h", port: 5672},
		{name: "amqp with userinfo path query", endpoint: "amqp://user:pw@h:5672/vh?x=y", host: "h", port: 5672}, //nolint:gosec // test endpoint carries credentials
		{name: "amqps bare host", endpoint: "amqps://h", host: "h", port: 0},
		{name: "kafka host port", endpoint: "kafka-1:9092", host: "kafka-1", port: 9092},
		{name: "ipv6 port", endpoint: "[::1]:5672", host: "::1", port: 5672},
		{name: "bare host", endpoint: "h", host: "h", port: 0},
		{name: "empty", endpoint: "", host: "", port: 0},
		{name: "bad port", endpoint: "h:notaport", host: "", port: 0},
		{name: "empty host", endpoint: ":5672", host: "", port: 0},
		{name: "bad ipv6", endpoint: "http://[::1", host: "", port: 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			host, port := endpointAddress(tc.endpoint)
			if host != tc.host || port != tc.port {
				t.Fatalf("endpointAddress(%q) = %q,%d want %q,%d", tc.endpoint, host, port, tc.host, tc.port)
			}
			if strings.Contains(host, "user") || strings.Contains(host, "pw") {
				t.Fatalf("endpointAddress(%q) host %q contains credentials", tc.endpoint, host)
			}
		})
	}
}
