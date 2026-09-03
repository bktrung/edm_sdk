package kafka

import (
	"context"
	"strings"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

func TestInsecureTLSRefusedOffLoopback(t *testing.T) {
	cases := []struct {
		name      string
		endpoints []string
		reject    bool
	}{
		{name: "single non-loopback endpoint", endpoints: []string{"broker.example.com:9092"}, reject: true},
		{name: "mixed loopback and non-loopback endpoints", endpoints: []string{"localhost:9092", "broker.example.com:9092"}, reject: true},
		{name: "hostname containing localhost", endpoints: []string{"localhost.attacker.example:9092"}, reject: true},
		{name: "hostname containing loopback IP", endpoints: []string{"127.0.0.1.attacker.com:9092"}, reject: true},
		{name: "empty endpoint list", endpoints: []string{}, reject: true},
		{name: "every endpoint exact loopback literal", endpoints: []string{"localhost:9092", "127.0.0.1:9092", "[::1]:9092"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			settings := &driver.TLSConfig{Enabled: true, InsecureSkipVerify: true}
			err := validateTLSConfig(tc.endpoints, settings)
			if tc.reject {
				if err == nil {
					t.Fatal("validateTLSConfig() error = nil, want insecure TLS rejection")
				}
				if !strings.Contains(err.Error(), "insecure TLS is only allowed for a test endpoint") {
					t.Fatalf("validateTLSConfig() error = %v, want insecure TLS rejection", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("validateTLSConfig() error = %v, want nil for loopback endpoints", err)
			}
		})
	}
}

func TestOpenRefusesInsecureTLSOffLoopback(t *testing.T) {
	_, err := (Driver{}).Open(context.Background(), driver.Config{
		Endpoints:      []string{"broker.example.com:9092"},
		ConnectTimeout: 100 * time.Millisecond,
		TLS:            &driver.TLSConfig{Enabled: true, InsecureSkipVerify: true},
	})
	assertFatalOpenError(t, err, "insecure TLS is only allowed for a test endpoint")
}
