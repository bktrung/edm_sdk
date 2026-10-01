package kafka

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	f1 "fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk"
)

func TestConfigKafkaTimeoutFallbacksMatchDriverDefaults(t *testing.T) {
	t.Parallel()

	for _, timeout := range []struct {
		name          string
		key           string
		bound         time.Duration
		sessionOption string
	}{
		{name: "session", key: "kafka.sessionTimeout", bound: defaultKafkaSessionTimeout * 3 / 5},
		{name: "rebalance", key: "kafka.rebalanceTimeout", bound: defaultKafkaRebalanceTimeout * 3 / 5, sessionOption: "      sessionTimeout: 60s\n"},
	} {
		t.Run(timeout.name, func(t *testing.T) {
			for _, tc := range []struct {
				name    string
				drain   time.Duration
				wantErr bool
			}{
				{name: "at bound", drain: timeout.bound},
				{name: "above bound", drain: timeout.bound + time.Nanosecond, wantErr: true},
			} {
				t.Run(tc.name, func(t *testing.T) {
					path := filepath.Join(t.TempDir(), "config.yaml")
					content := fmt.Sprintf(`f1:
  env: test
  service: orders
  broker:
    driver: kafka
    endpoints: [kafka://broker:9092]
    kafka:
%s  lifecycle:
    rebalanceDrainTimeout: %s
`, timeout.sessionOption, tc.drain)
					if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
						t.Fatal(err)
					}

					_, err := f1.LoadConfig(path)
					if tc.wantErr {
						if err == nil || !strings.Contains(err.Error(), timeout.key) {
							t.Fatalf("LoadConfig() error = %v, want %s validation", err, timeout.key)
						}
						return
					}
					if err != nil {
						t.Fatalf("LoadConfig() error = %v, want the driver fallback bound to validate", err)
					}
				})
			}
		})
	}
}

func TestParseKafkaSessionTimeoutConfigRejectsDurationOverflow(t *testing.T) {
	t.Parallel()

	if _, err := parseKafkaSessionTimeoutConfig("group.min.session.timeout.ms", "9223372036854775807"); err == nil {
		t.Fatal("parseKafkaSessionTimeoutConfig() error = nil, want duration overflow error")
	}
}

func TestValidateKafkaSessionTimeoutHonorsAvailableBrokerBounds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		timeout  time.Duration
		settings brokerConfig
		wantErr  bool
	}{
		{
			name:    "below minimum",
			timeout: 5 * time.Second,
			settings: brokerConfig{
				groupMinSessionTimeout:    6 * time.Second,
				hasGroupMinSessionTimeout: true,
			},
			wantErr: true,
		},
		{
			name:    "above maximum",
			timeout: 6 * time.Second,
			settings: brokerConfig{
				groupMaxSessionTimeout:    5 * time.Second,
				hasGroupMaxSessionTimeout: true,
			},
			wantErr: true,
		},
		{
			name:     "missing bounds",
			timeout:  5 * time.Second,
			settings: brokerConfig{},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateKafkaSessionTimeout(test.timeout, test.settings)
			if (err != nil) != test.wantErr {
				t.Fatalf("validateKafkaSessionTimeout() error = %v, wantErr %t", err, test.wantErr)
			}
		})
	}
}

func TestValidateKafkaDrainTimeoutChecksBothGroupTimeouts(t *testing.T) {
	t.Parallel()

	if err := validateKafkaDrainTimeout(nil, 40*time.Second); err == nil || !strings.Contains(err.Error(), "kafka.sessionTimeout") {
		t.Fatalf("validateKafkaDrainTimeout(defaults, 40s) error = %v, want session timeout bound", err)
	}
	if err := validateKafkaDrainTimeout(map[string]string{
		"kafka.sessionTimeout":   "60s",
		"kafka.rebalanceTimeout": "50s",
	}, 31*time.Second); err == nil || !strings.Contains(err.Error(), "kafka.rebalanceTimeout") {
		t.Fatalf("validateKafkaDrainTimeout(custom, 31s) error = %v, want rebalance timeout bound", err)
	}
}

// TestKafkaSoftwareVersionFitsTheBrokerPattern pins the version sent in
// ApiVersions to Kafka's accepted pattern: a broker refuses the whole request
// for "(devel)", which a test binary and a working-tree build report.
func TestKafkaSoftwareVersionFitsTheBrokerPattern(t *testing.T) {
	for _, test := range []struct {
		version string
		want    string
	}{
		{version: "(devel)", want: "devel"},
		{version: "v0.1.0", want: "v0.1.0"},
		{version: "v0.0.0-20260923120000-abcdef123456", want: "v0.0.0-20260923120000-abcdef123456"},
		{version: "v1.2.3+dirty", want: "v1.2.3-dirty"},
		{version: "", want: "unknown"},
	} {
		if got := kafkaSoftwareVersion(test.version); got != test.want {
			t.Fatalf("kafkaSoftwareVersion(%q) = %q, want %q", test.version, got, test.want)
		}
	}
}
