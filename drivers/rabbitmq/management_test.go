package rabbitmq

import (
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

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

func TestManagementClientDerivesStandardPort(t *testing.T) {
	client, err := newManagementClient("amqp://localhost:5672/", driver.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if client.baseURL != "http://localhost:15672" {
		t.Fatalf("management base URL = %q, want standard management endpoint", client.baseURL)
	}
}

func TestManagementClientDerivesAlternatePort(t *testing.T) {
	client, err := newManagementClient("amqp://localhost:25672/", driver.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if client.baseURL != "http://localhost:35672" {
		t.Fatalf("management base URL = %q, want alternate management endpoint", client.baseURL)
	}
}

// managementVhostCase is one endpoint spelling and the vhost the management
// client has to derive from it.
type managementVhostCase struct {
	name     string
	endpoint string
	options  map[string]string
	want     string // derived vhost; empty when the endpoint is refused
	wantErr  string // substring the refusal names; empty when a vhost is expected
}

// managementVhostCases is the corpus both vhost tests read. The expectations
// are written out rather than computed, so a change in the AMQP library's
// reading fails here and names the spelling that moved.
//
// An endpoint with no authority is the one spelling whose vhost the management
// client never derives: the AMQP library reads "orders" from amqp://///orders
// through its triple-slash branch and would connect to the default host, but
// the constructor refuses any endpoint without a host long before that, so the
// row pins the refusal instead.
func managementVhostCases() []managementVhostCase {
	return []managementVhostCase{
		{
			name:     "trailing slash is the default vhost",
			endpoint: "amqp://localhost:5672/",
			want:     "/",
		},
		{
			name:     "named vhost",
			endpoint: "amqp://localhost:5672/orders",
			want:     "orders",
		},
		{
			name:     "percent-encoded leading slash",
			endpoint: "amqp://localhost:5672/%2Forders",
			want:     "/orders",
		},
		{
			name:     "empty path is the default vhost",
			endpoint: "amqp://localhost:5672",
			want:     "/",
		},
		{
			name:     "percent-encoded inner slash",
			endpoint: "amqps://localhost:5671/orders%2Fsub",
			want:     "orders/sub",
		},
		{
			name:     "configured option wins over the endpoint",
			endpoint: "amqp://localhost:5672/orders",
			options:  map[string]string{"rabbitmq.vhost": "configured-vhost"},
			want:     "configured-vhost",
		},
		{
			name:     "authority-less endpoint is refused before any derivation",
			endpoint: "amqp://///orders",
			wantErr:  "missing host",
		},
	}
}

// TestManagementClientDerivesVhostFromEndpoint spells out what each endpoint
// spelling means to the management client. It is the layer a reader consults
// instead of re-deriving the AMQP library's parse from its source, and it is
// the layer that goes red if an upgrade changes that parse under us.
func TestManagementClientDerivesVhostFromEndpoint(t *testing.T) {
	for _, test := range managementVhostCases() {
		t.Run(test.name, func(t *testing.T) {
			client, err := newManagementClient(test.endpoint, driver.Config{DriverOptions: test.options})
			if test.wantErr != "" {
				if err == nil || client != nil {
					t.Fatalf("newManagementClient(%q) = client %v, error %v; want refusal", test.endpoint, client, err)
				}
				if !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("newManagementClient(%q) error = %v, want %q", test.endpoint, err, test.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("newManagementClient(%q): %v", test.endpoint, err)
			}
			if client.vhost != test.want {
				t.Fatalf("derived vhost for %q = %q, want %q", test.endpoint, client.vhost, test.want)
			}
		})
	}
}

// TestManagementClientVhostMatchesAMQPParse is the property the row exists for:
// with no configured vhost, the management client addresses the vhost the AMQP
// connection's own parse reads from the same endpoint. It is what makes a
// hand-written reimplementation of that parse impossible to land quietly.
func TestManagementClientVhostMatchesAMQPParse(t *testing.T) {
	for _, test := range managementVhostCases() {
		if test.wantErr != "" || test.options["rabbitmq.vhost"] != "" {
			continue // the property holds only where the endpoint decides the vhost
		}
		t.Run(test.name, func(t *testing.T) {
			client, err := newManagementClient(test.endpoint, driver.Config{})
			if err != nil {
				t.Fatalf("newManagementClient(%q): %v", test.endpoint, err)
			}
			amqpURI, err := amqp.ParseURI(test.endpoint)
			if err != nil {
				t.Fatalf("amqp.ParseURI(%q): %v", test.endpoint, err)
			}
			if client.vhost != amqpURI.Vhost {
				t.Fatalf("management vhost for %q = %q, AMQP parse reads %q", test.endpoint, client.vhost, amqpURI.Vhost)
			}
		})
	}
}

// TestManagementClientRejectsEndpointTheAMQPParserRefuses pins what happens when
// the two readers disagree: net/url accepts an endpoint that the AMQP library's
// parse refuses, which is reachable for an endpoint carrying whitespace. The
// constructor refuses it rather than build a client addressing a vhost the
// connection never reaches, and the refusal does not repeat the endpoint, which
// here carries credentials.
func TestManagementClientRejectsEndpointTheAMQPParserRefuses(t *testing.T) {
	const username = "fake-whitespace-user"
	const password = "fake-whitespace-pass"

	client, err := newManagementClient("amqp://"+username+":"+password+"@localhost:5672/order s", driver.Config{})
	if err == nil || client != nil {
		t.Fatalf("newManagementClient() = client %v, error %v; want refusal", client, err)
	}
	if !strings.Contains(err.Error(), "invalid management endpoint") {
		t.Fatalf("newManagementClient() error = %v, want invalid endpoint detail", err)
	}
	if strings.Contains(err.Error(), username) || strings.Contains(err.Error(), password) {
		t.Fatalf("newManagementClient() error = %v, contains endpoint credentials", err)
	}
}

func TestManagementClientRejectsInvalidDerivedPort(t *testing.T) {
	client, err := newManagementClient("amqp://localhost:55536/", driver.Config{})
	if err == nil || client != nil {
		t.Fatalf("newManagementClient() = client %v, error %v; want invalid derived port error", client, err)
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

func TestManagementEndpointErrorRedactsCredentials(t *testing.T) {
	const username = "fake-management-user"
	const password = "fake-management-pass"

	for _, test := range []struct {
		name     string
		endpoint string
	}{
		{
			name:     "missing host",
			endpoint: "amqp://" + username + ":" + password + "@",
		},
		{
			name:     "opaque endpoint",
			endpoint: "amqp:" + username + ":" + password + "@",
		},
		{
			name:     "parse failure",
			endpoint: "amqp://" + username + ":" + password + "@%zz",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			client, err := newManagementClient(test.endpoint, driver.Config{})
			if err == nil || client != nil {
				t.Fatalf("newManagementClient() = client %v, error %v; want validation error", client, err)
			}
			if !strings.Contains(err.Error(), "invalid management endpoint") {
				t.Fatalf("newManagementClient() error = %v, want invalid endpoint detail", err)
			}
			if strings.Contains(err.Error(), username) || strings.Contains(err.Error(), password) {
				t.Fatalf("newManagementClient() error = %v, contains endpoint credentials", err)
			}
		})
	}
}

func TestManagementClientRefusesPlaintextForRemoteHost(t *testing.T) {
	for _, test := range []struct {
		name string
		cfg  driver.Config
	}{
		{
			name: "without TLS",
			cfg:  driver.Config{},
		},
		{
			name: "with TLS enabled",
			cfg:  driver.Config{TLS: &driver.TLSConfig{Enabled: true}},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			client, err := newManagementClient("amqp://broker.example:5672/", test.cfg)
			if err == nil || client != nil {
				t.Fatalf("newManagementClient() = client %v, error %v; want remote plaintext refusal", client, err)
			}
			if !strings.Contains(err.Error(), "plaintext") || !strings.Contains(err.Error(), "amqps://") {
				t.Fatalf("newManagementClient() error = %v, want plaintext and amqps requirement", err)
			}
		})
	}
}

func TestManagementClientRefusesForgedLoopbackHost(t *testing.T) {
	const username = "fake-loopback-user"
	const value = "fake-loopback-value"

	for _, endpoint := range []string{
		"amqp://" + username + ":" + value + "@evil-localhost.example.com:5672/",
		"amqp://" + username + ":" + value + "@127.0.0.1.example.com:5672/",
	} {
		t.Run(endpoint, func(t *testing.T) {
			client, err := newManagementClient(endpoint, driver.Config{})
			if err == nil || client != nil {
				t.Fatalf("newManagementClient() = client %v, error %v; want forged loopback refusal", client, err)
			}
			if !strings.Contains(err.Error(), "plaintext") || !strings.Contains(err.Error(), "amqps://") {
				t.Fatalf("newManagementClient() error = %v, want plaintext and amqps requirement", err)
			}
			if strings.Contains(err.Error(), username) || strings.Contains(err.Error(), value) {
				t.Fatalf("newManagementClient() error = %v, contains endpoint credentials", err)
			}
		})
	}
}

func TestManagementClientAllowsPlaintextOnLoopback(t *testing.T) {
	for _, endpoint := range []string{
		"amqp://localhost:5672/",
		"amqp://127.0.0.1:5672/",
	} {
		t.Run(endpoint, func(t *testing.T) {
			client, err := newManagementClient(endpoint, driver.Config{})
			if err != nil {
				t.Fatalf("newManagementClient() error = %v, want loopback plaintext to work", err)
			}
			if client == nil {
				t.Fatal("newManagementClient() returned nil client")
			}
		})
	}
}

func TestManagementClientUsesHTTPSWhenTLSEnabled(t *testing.T) {
	cfg := driver.Config{TLS: &driver.TLSConfig{Enabled: true}}
	client, err := newManagementClient("amqps://broker.example:5671/", cfg)
	if err != nil {
		t.Fatal(err)
	}
	if client.baseURL != "https://broker.example:15671" {
		t.Fatalf("management base URL = %q, want HTTPS management endpoint", client.baseURL)
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
	client, err := newManagementClient("amqp://localhost:5673/", cfg)
	if err != nil {
		t.Fatal(err)
	}
	if client.baseURL != "http://localhost:18080" {
		t.Fatalf("management base URL = %q, want configured port", client.baseURL)
	}
}

func TestManagementClientUsesConfiguredPortForAMQPSTransport(t *testing.T) {
	cfg := driver.Config{DriverOptions: map[string]string{managementPortOption: "18080"}}
	client, err := newManagementClient("amqps://broker.example:5671/", cfg)
	if err != nil {
		t.Fatal(err)
	}
	if client.baseURL != "https://broker.example:18080" {
		t.Fatalf("management base URL = %q, want configured HTTPS port", client.baseURL)
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

// TestManagementUnavailableNamesPurposeEndpointAndAction pins the message an
// operator reads when the management API cannot be reached. It is the SDK's
// only answer to the most likely single cause of a first deployment failing,
// so every part of it that a reader acts on is asserted: which inspection
// needed the API, the endpoint that was tried, and the two actions that
// resolve it. The endpoint is built from an AMQP URL that carries a user and
// password, which is the case the message must not leak.
func TestManagementUnavailableNamesPurposeEndpointAndAction(t *testing.T) {
	const username = "fake-management-user"
	const password = "fake-management-pass"
	cause := errors.New("management endpoint is unreachable")

	purposes := []string{
		"binding verification",
		`argument drift verification for destination "orders"`,
		`argument drift verification for parking destination "orders.park"`,
	}
	for _, test := range []struct {
		name string
		cfg  driver.Config
	}{
		{name: "derived port", cfg: driver.Config{}},
		{
			name: "configured port",
			cfg:  driver.Config{DriverOptions: map[string]string{managementPortOption: "18080"}},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			client, err := newManagementClient("amqp://"+username+":"+password+"@localhost:5672/", test.cfg)
			if err != nil {
				t.Fatalf("newManagementClient: %v", err)
			}
			admin := &adminOperations{conn: &conn{management: client}}
			for _, purpose := range purposes {
				t.Run(purpose, func(t *testing.T) {
					failure := admin.managementUnavailable(purpose, cause)
					message := failure.Error()
					for _, want := range []string{purpose, client.baseURL, "RabbitMQ management plugin", managementPortOption} {
						if !strings.Contains(message, want) {
							t.Fatalf("management failure %q does not name %q, so an operator cannot act on it", message, want)
						}
					}
					if !errors.Is(failure, cause) {
						t.Fatalf("management failure %q does not wrap the cause %v", message, cause)
					}
					if strings.Contains(message, username) || strings.Contains(message, password) {
						t.Fatalf("management failure %q contains endpoint credentials", message)
					}
				})
			}
		})
	}
}
