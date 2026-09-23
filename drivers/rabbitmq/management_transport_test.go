package rabbitmq

import (
	"crypto/tls"
	"net/http"
	"testing"
)

type replacedTransport struct{}

func (replacedTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, http.ErrNotSupported
}

func TestManagementTransportToleratesAReplacedDefaultTransport(t *testing.T) {
	original := http.DefaultTransport
	http.DefaultTransport = replacedTransport{}
	t.Cleanup(func() { http.DefaultTransport = original })

	tlsClientConfig := &tls.Config{ServerName: "broker.internal", MinVersion: tls.VersionTLS12}
	transport := managementTransport(tlsClientConfig)
	if transport.TLSClientConfig != tlsClientConfig {
		t.Fatalf("TLSClientConfig = %v, want the management TLS configuration", transport.TLSClientConfig)
	}
	if transport.Proxy == nil {
		t.Fatal("Proxy = nil, want the environment proxy the standard transport uses")
	}
}
