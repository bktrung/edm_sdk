package rabbitmq

import (
	"strings"
	"testing"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

// TestInsecureTLSEndpointGate exercises makeAMQPConfig, the seam dial uses to
// decide whether InsecureSkipVerify is honored. Each rejection case must
// produce the same "insecure TLS is only allowed for a test endpoint" error
// that a real connection attempt would see, and each permitted case must
// build a TLS config with verification off rather than being rejected.
func TestInsecureTLSEndpointGate(t *testing.T) {
	cases := []struct {
		name      string
		endpoints []string
		reject    bool
	}{
		{name: "empty endpoint list", endpoints: []string{}, reject: true},
		{name: "single non-loopback endpoint", endpoints: []string{"amqps://prod.example.com:5671"}, reject: true},
		{name: "loopback mixed with non-loopback", endpoints: []string{"amqp://localhost:5672", "amqps://prod.example.com:5671"}, reject: true},
		{name: "hostname containing localhost as substring", endpoints: []string{"amqps://evil-localhost.attacker.com:5671"}, reject: true},
		{name: "hostname containing loopback IP as substring", endpoints: []string{"amqps://127.0.0.1.attacker.com:5671"}, reject: true},
		{name: "loopback hostname", endpoints: []string{"amqp://localhost:5672"}, reject: false},
		{name: "loopback IPv4", endpoints: []string{"amqp://127.0.0.1:5672"}, reject: false},
		{name: "loopback IPv6", endpoints: []string{"amqp://[::1]:5672"}, reject: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := driver.Config{
				Endpoints: tc.endpoints,
				TLS:       &driver.TLSConfig{Enabled: true, InsecureSkipVerify: true},
			}
			_, err := makeAMQPConfig(cfg)
			if tc.reject {
				if err == nil {
					t.Fatal("makeAMQPConfig() error = nil, want insecure TLS rejection")
				}
				if !strings.Contains(err.Error(), "insecure TLS is only allowed for a test endpoint") {
					t.Fatalf("makeAMQPConfig() error = %v, want insecure TLS rejection message", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("makeAMQPConfig() error = %v, want nil for a loopback endpoint", err)
			}
		})
	}
}

func TestTLSConfigUsesConfiguredServerName(t *testing.T) {
	for _, test := range []struct {
		name       string
		serverName string
	}{
		{name: "configured", serverName: "broker.alias.example"},
		{name: "empty"},
	} {
		t.Run(test.name, func(t *testing.T) {
			config, err := tlsConfig(&driver.TLSConfig{ServerName: test.serverName})
			if err != nil {
				t.Fatalf("tlsConfig() error = %v", err)
			}
			if config.ServerName != test.serverName {
				t.Fatalf("tls.Config.ServerName = %q, want %q", config.ServerName, test.serverName)
			}
		})
	}
}
