package rabbitmq

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

func TestManagementClientHasExplicitTimeout(t *testing.T) {
	cfg := driver.Config{ConnectTimeout: 7 * time.Second}
	client, err := newManagementClient(defaultEndpoint, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if client.client.Timeout != 7*time.Second {
		t.Fatalf("management HTTP timeout = %s, want 7s", client.client.Timeout)
	}
}

func TestManagementClientRejectsInvalidEndpoint(t *testing.T) {
	client, err := newManagementClient("://invalid", driver.Config{})
	if err == nil || client != nil {
		t.Fatalf("newManagementClient() = client %v, error %v; want parse error", client, err)
	}
	if !strings.Contains(err.Error(), "invalid management endpoint") {
		t.Fatalf("newManagementClient() error = %v, want invalid endpoint detail", err)
	}
}

func TestManagementClientUsesResolvedVhostCredentialsAndTLS(t *testing.T) {
	cfg := driver.Config{
		ConnectTimeout: 7 * time.Second,
		DriverOptions:  map[string]string{"rabbitmq.vhost": "configured-vhost"},
		TLS:            &driver.TLSConfig{Enabled: true, InsecureSkipVerify: true},
		SASL:           &driver.SASLConfig{Mechanism: "plain", Username: "configured-user", Password: "configured-pass"},
	}
	client, err := newManagementClient("amqps://url-user:url-pass@broker.example:5671/from-url", cfg)
	if err != nil {
		t.Fatal(err)
	}
	if client.baseURL != "https://broker.example:15671" {
		t.Fatalf("management base URL = %q, want configured TLS management endpoint", client.baseURL)
	}
	if client.vhost != "configured-vhost" {
		t.Fatalf("management vhost = %q, want configured-vhost", client.vhost)
	}
	if client.username != "configured-user" || client.password != "configured-pass" {
		t.Fatalf("management credentials = %q/%q, want configured credentials", client.username, client.password)
	}
	transport, ok := client.client.Transport.(*http.Transport)
	if !ok || transport.TLSClientConfig == nil || !transport.TLSClientConfig.InsecureSkipVerify {
		t.Fatalf("management TLS transport = %#v, want configured TLS client", client.client.Transport)
	}
}

func TestManagementClientUsesConfiguredPort(t *testing.T) {
	cfg := driver.Config{DriverOptions: map[string]string{managementPortOption: "18080"}}
	client, err := newManagementClient("amqp://broker.example:5673/", cfg)
	if err != nil {
		t.Fatal(err)
	}
	if client.baseURL != "http://broker.example:18080" {
		t.Fatalf("management base URL = %q, want configured port", client.baseURL)
	}
}

func TestManagementClientRejectsInvalidConfiguredPort(t *testing.T) {
	for _, configured := range []string{"not-a-port", "0", "65536"} {
		t.Run(configured, func(t *testing.T) {
			cfg := driver.Config{DriverOptions: map[string]string{managementPortOption: configured}}
			client, err := newManagementClient(defaultEndpoint, cfg)
			if err == nil || client != nil {
				t.Fatalf("newManagementClient() = client %v, error %v; want invalid port error", client, err)
			}
		})
	}
}
