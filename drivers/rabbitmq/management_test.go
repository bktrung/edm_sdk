package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

func TestManagementClientListsAndDeletesExchanges(t *testing.T) {
	const username = "management-user"
	const password = "management-password"
	const exchange = "orders/fanout"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if usernameFromRequest, requestPassword, ok := r.BasicAuth(); !ok || usernameFromRequest != username || requestPassword != password {
			t.Errorf("BasicAuth() = %q, %q, %t; want %q, %q, true", usernameFromRequest, requestPassword, ok, username, password)
		}
		switch {
		case r.Method == http.MethodGet && r.URL.EscapedPath() == "/api/exchanges/%2F":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode([]managementExchange{{Name: exchange}})
		case r.Method == http.MethodDelete && r.URL.EscapedPath() == "/api/exchanges/%2F/orders%2Ffanout":
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client := &managementClient{
		baseURL:  server.URL,
		username: username,
		password: password,
		vhost:    "/",
		client:   *server.Client(),
	}
	exchanges, err := client.listExchanges(t.Context())
	if err != nil {
		t.Fatalf("listExchanges: %v", err)
	}
	if len(exchanges) != 1 || exchanges[0].Name != exchange {
		t.Fatalf("listExchanges() = %+v, want one exchange named %q", exchanges, exchange)
	}
	deleted, err := client.deleteExchange(t.Context(), exchange)
	if err != nil {
		t.Fatalf("deleteExchange: %v", err)
	}
	if !deleted {
		t.Fatal("deleteExchange() = false, want true")
	}
}

func TestManagementClientDeleteExchangeTreatsNotFoundAsAbsent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Errorf("request method = %s, want DELETE", r.Method)
		}
		http.NotFound(w, r)
	}))
	defer server.Close()
	client := &managementClient{
		baseURL: server.URL,
		vhost:   "/",
		client:  *server.Client(),
	}

	deleted, err := client.deleteExchange(t.Context(), "missing")
	if err != nil {
		t.Fatalf("deleteExchange: %v", err)
	}
	if deleted {
		t.Fatal("deleteExchange() = true, want false for 404")
	}
}

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

// TestManagementClientPort pins where the management API is addressed: the
// AMQP port plus 10000 when nothing is configured, the configured port when one
// is, the scheme following the AMQP transport, and a refusal for a port outside
// the valid range whichever way it was reached.
func TestManagementClientPort(t *testing.T) {
	for _, test := range []struct {
		name     string
		endpoint string
		port     string
		want     string
	}{
		{name: "derived from the standard port", endpoint: "amqp://localhost:5672/", want: "http://localhost:15672"},
		{name: "derived from an alternate port", endpoint: "amqp://localhost:25672/", want: "http://localhost:35672"},
		{name: "derived port out of range", endpoint: "amqp://localhost:55536/"},
		{name: "configured", endpoint: "amqp://localhost:5673/", port: "18080", want: "http://localhost:18080"},
		{name: "configured over amqps", endpoint: "amqps://broker.example:5671/", port: "18080", want: "https://broker.example:18080"},
		{name: "configured not a port", endpoint: "amqp://localhost:5672/", port: "not-a-port"},
		{name: "configured zero", endpoint: "amqp://localhost:5672/", port: "0"},
		{name: "configured out of range", endpoint: "amqp://localhost:5672/", port: "65536"},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := driver.Config{}
			if test.port != "" {
				cfg.DriverOptions = map[string]string{managementPortOption: test.port}
			}
			client, err := newManagementClient(test.endpoint, cfg)
			if test.want == "" {
				if err == nil || client != nil {
					t.Fatalf("newManagementClient(%q) = client %v, error %v; want an invalid port error", test.endpoint, client, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if client.baseURL != test.want {
				t.Fatalf("management base URL = %q, want %q", client.baseURL, test.want)
			}
		})
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
// client refuses before derivation: the driver does not support endpoints
// without an explicit host, so the row pins that refusal instead.
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
			name:     "configured mismatch is refused",
			endpoint: "amqp://localhost:5672/orders",
			options:  map[string]string{"rabbitmq.vhost": "configured-vhost"},
			wantErr:  `"configured-vhost" does not match endpoint vhost "orders"`,
		},
		{
			name:     "configured matching vhost",
			endpoint: "amqp://localhost:5672/orders",
			options:  map[string]string{"rabbitmq.vhost": "orders"},
			want:     "orders",
		},
		{
			name:     "authority-less endpoint is refused before any derivation",
			endpoint: "amqp:///orders",
			wantErr:  "without a host is unsupported",
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
// every accepted configuration addresses the vhost the AMQP connection's own
// parse reads from the same endpoint, including a matching configured option.
func TestManagementClientVhostMatchesAMQPParse(t *testing.T) {
	for _, test := range managementVhostCases() {
		if test.wantErr != "" {
			continue
		}
		t.Run(test.name, func(t *testing.T) {
			client, err := newManagementClient(test.endpoint, driver.Config{DriverOptions: test.options})
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

// TestManagementClientEndpointValidityDoesNotDependOnConfiguredVhost keeps the
// AMQP parser as the authority for endpoint validity with an absent or matching
// configured vhost.
func TestManagementClientEndpointValidityDoesNotDependOnConfiguredVhost(t *testing.T) {
	tests := []struct {
		name     string
		endpoint string
		accepted bool
	}{
		{name: "valid endpoint with host", endpoint: "amqp://localhost:5672/orders", accepted: true},
		{name: "AMQP-invalid whitespace", endpoint: "amqp://localhost:5672/order s"},
		{name: "host-less endpoint", endpoint: "amqp:///orders"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for _, configured := range []bool{false, true} {
				t.Run(map[bool]string{false: "vhost from endpoint", true: "configured vhost"}[configured], func(t *testing.T) {
					options := map[string]string{}
					if configured {
						options["rabbitmq.vhost"] = "orders"
					}
					client, err := newManagementClient(test.endpoint, driver.Config{DriverOptions: options})
					if (err == nil) != test.accepted {
						t.Fatalf("newManagementClient(%q) with configured=%t = client %v, error %v; accepted=%t", test.endpoint, configured, client, err, test.accepted)
					}
				})
			}
		})
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
		DriverOptions:  map[string]string{"rabbitmq.vhost": "from-url"},
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
	if client.vhost != "from-url" {
		t.Fatalf("management vhost = %q, want from-url", client.vhost)
	}
	if client.username != "configured-user" || client.password != "configured-pass" {
		t.Fatal("management credentials do not match configured PLAIN identity")
	}
	transport, ok := client.client.Transport.(*http.Transport)
	if !ok || transport.TLSClientConfig == nil || !transport.TLSClientConfig.InsecureSkipVerify {
		t.Fatalf("management TLS transport = %#v, want configured TLS client", client.client.Transport)
	}
}

func TestManagementClientUsesConfiguredTLSServerName(t *testing.T) {
	cfg := driver.Config{
		TLS: &driver.TLSConfig{Enabled: true, InsecureSkipVerify: true, ServerName: "broker.alias.example"},
	}
	client, err := newManagementClient("amqps://broker.example:5671/", cfg)
	if err != nil {
		t.Fatal(err)
	}
	transport, ok := client.client.Transport.(*http.Transport)
	if !ok || transport.TLSClientConfig == nil {
		t.Fatalf("management TLS transport = %#v, want configured TLS client", client.client.Transport)
	}
	if transport.TLSClientConfig.ServerName != "broker.alias.example" {
		t.Fatalf("management tls.Config.ServerName = %q, want %q", transport.TLSClientConfig.ServerName, "broker.alias.example")
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

type managementResponseTransport struct {
	status int
	body   *managementResponseBody
	err    error
}

func (r managementResponseTransport) RoundTrip(*http.Request) (*http.Response, error) {
	if r.err != nil {
		return nil, r.err
	}
	return &http.Response{
		StatusCode: r.status,
		Status:     fmt.Sprintf("%d %s", r.status, http.StatusText(r.status)),
		Body:       r.body,
		Header:     make(http.Header),
	}, nil
}

type managementResponseBody struct {
	io.Reader
	read   int
	closed bool
}

func (b *managementResponseBody) Read(p []byte) (int, error) {
	n, err := b.Reader.Read(p)
	b.read += n
	return n, err
}

func (b *managementResponseBody) Close() error {
	b.closed = true
	return nil
}

// requireManagementStatus uses reflection so the regression compiles before
// the private HTTP error exists, and fails on lost status rather than a missing type.
func requireManagementStatus(t *testing.T, err error, status int, method, resource string) {
	t.Helper()
	for cause := err; cause != nil; cause = errors.Unwrap(cause) {
		value := reflect.ValueOf(cause)
		if value.Kind() != reflect.Pointer || value.Elem().Kind() != reflect.Struct {
			continue
		}
		value = value.Elem()
		if value.Type().Name() != "managementHTTPError" {
			continue
		}
		target := reflect.New(reflect.TypeOf(cause))
		if !errors.As(err, target.Interface()) {
			t.Fatalf("errors.As(%v) did not reach the HTTP error", err)
		}
		if value.FieldByName("statusCode").Int() != int64(status) ||
			value.FieldByName("method").String() != method ||
			value.FieldByName("resource").String() != resource {
			t.Fatalf("HTTP error = %v, want status %d, method %s, resource %s", cause, status, method, resource)
		}
		return
	}
	t.Fatalf("error %v lost HTTP status %d", err, status)
}

func TestManagementHTTPErrorPreservesStatus(t *testing.T) {
	calls := []struct {
		resource string
		method   string
		call     func(*managementClient) error
	}{
		{"queues", http.MethodGet, func(m *managementClient) error { _, err := m.listQueues(t.Context()); return err }},
		{"exchanges", http.MethodGet, func(m *managementClient) error { _, err := m.listExchanges(t.Context()); return err }},
		{"bindings", http.MethodGet, func(m *managementClient) error { _, err := m.listBindings(t.Context()); return err }},
		{`queue "orders/slash"`, http.MethodGet, func(m *managementClient) error { _, err := m.getQueue(t.Context(), "orders/slash"); return err }},
		{`exchange "orders/slash"`, http.MethodDelete, func(m *managementClient) error { _, err := m.deleteExchange(t.Context(), "orders/slash"); return err }},
	}
	for _, call := range calls {
		for _, status := range []int{401, 403, 503} {
			t.Run(fmt.Sprintf("%s/%d", call.resource, status), func(t *testing.T) {
				for _, bodyText := range []string{" denied\n", " " + strings.Repeat("x", 5000)} {
					body := &managementResponseBody{Reader: strings.NewReader(bodyText)}
					m := &managementClient{baseURL: "http://localhost", vhost: "/", client: http.Client{
						Transport: managementResponseTransport{status: status, body: body},
					}}
					err := call.call(m)
					if !body.closed || body.read > 4096 {
						t.Fatalf("response ownership: closed=%t, read=%d", body.closed, body.read)
					}
					wantBody := strings.TrimSpace(bodyText[:min(len(bodyText), 4096)])
					want := fmt.Sprintf("management API %s %s: %d %s: %s", call.method, call.resource, status, http.StatusText(status), wantBody)
					if err == nil || err.Error() != want {
						t.Fatalf("error = %v, want %q", err, want)
					}
					admin := &adminOperations{conn: &conn{management: m}}
					wrapped := classify("prune", driver.KindTransient, admin.managementUnavailable("inspection", err))
					requireManagementStatus(t, wrapped, status, call.method, call.resource)
				}
			})
		}
	}
	t.Run("queue not found", func(t *testing.T) {
		body := &managementResponseBody{Reader: strings.NewReader("missing")}
		m := &managementClient{baseURL: "http://localhost", client: http.Client{Transport: managementResponseTransport{status: 404, body: body}}}
		_, err := m.getQueue(t.Context(), "missing")
		if !errors.Is(err, errQueueNotFound) || !body.closed {
			t.Fatalf("getQueue 404 = %v, closed=%t", err, body.closed)
		}
	})
}

func TestPruneManagementAuthorizationErrors(t *testing.T) {
	for _, stage := range []string{"queues", "exchanges", "bindings", "delete"} {
		for _, status := range []int{401, 403, 503} {
			t.Run(fmt.Sprintf("%s/%d", stage, status), func(t *testing.T) {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					resource := strings.Split(r.URL.Path, "/")[2]
					if (stage == resource && r.Method == http.MethodGet) || (stage == "delete" && r.Method == http.MethodDelete) {
						w.WriteHeader(status)
						_, _ = io.WriteString(w, "denied\n")
						return
					}
					if resource == "exchanges" {
						_, _ = io.WriteString(w, `[{"name":"t047-exchange"}]`)
					} else {
						_, _ = io.WriteString(w, `[]`)
					}
				}))
				defer server.Close()
				m := &managementClient{baseURL: server.URL, vhost: "/", client: *server.Client()}
				// These branches stop before channel admission; IsClosed on the
				// zero connection reads its open-state flag without doing I/O.
				admin := &adminOperations{conn: &conn{amqp: &amqp.Connection{}, management: m}}
				_, err := admin.Prune(t.Context(), []string{"t047-exchange"})
				want := driver.KindPermission
				if status == 503 {
					want = driver.KindTransient
				}
				if kind, ok := driver.Classify(err); !ok || kind != want {
					t.Fatalf("Prune = %v, classification %v/%t, want %v", err, kind, ok, want)
				}
				var portErr *driver.Error
				if !errors.As(err, &portErr) || portErr.Driver != "rabbitmq" || portErr.Op != "prune" {
					t.Fatalf("Prune metadata = %v", err)
				}
				resource, method := stage, http.MethodGet
				if stage == "delete" {
					resource, method = `exchange "t047-exchange"`, http.MethodDelete
				}
				requireManagementStatus(t, err, status, method, resource)
			})
		}
	}
}

func TestTopologyManagementAuthorizationErrors(t *testing.T) {
	for _, policy := range []driver.TopologyPolicy{driver.TopologyDeclare, driver.TopologyVerify} {
		for _, status := range []int{401, 403, 503} {
			t.Run(fmt.Sprintf("%v/%d", policy, status), func(t *testing.T) {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					w.WriteHeader(status)
					_, _ = io.WriteString(w, "denied\n")
				}))
				defer server.Close()
				m := &managementClient{baseURL: server.URL, vhost: "/", client: *server.Client()}
				admin := &adminOperations{conn: &conn{management: m, deferred: make(map[string]time.Duration)}}
				spec := driver.TopologySpec{Policy: policy, Bindings: []driver.BindingSpec{{Source: "source", Destination: "destination"}}}
				_, err := admin.ensureTopology(context.Background(), spec)
				want := driver.KindPermission
				if status == 503 {
					want = driver.KindTransient
				}
				if kind, ok := driver.Classify(err); !ok || kind != want {
					t.Fatalf("topology = %v, classification %v/%t, want %v", err, kind, ok, want)
				}
				requireManagementStatus(t, err, status, http.MethodGet, "bindings")
			})
		}
	}
}

func TestManagementClientUsesDefaultURIIdentity(t *testing.T) {
	for _, settings := range []*driver.SASLConfig{nil, {}} {
		assertManagementLogin(t, "amqp://localhost:5672/", settings, "guest", "guest")
	}
}

func TestManagementClientUsesURIIdentityWithoutPassword(t *testing.T) {
	assertManagementLogin(t, "amqp://orders-user@localhost:5672/", nil, "orders-user", "guest")
}

func TestManagementClientUsesSelectedSASLIdentity(t *testing.T) {
	for _, test := range []struct {
		name     string
		settings driver.SASLConfig
		username string
		password string
	}{
		{name: "PLAIN", settings: driver.SASLConfig{Mechanism: "PLAIN", Username: "sasl-user", Password: "sasl-secret"}, username: "sasl-user", password: "sasl-secret"},
		{name: "AMQPLAIN", settings: driver.SASLConfig{Mechanism: "AMQPLAIN", Username: "sasl-user", Password: "sasl-secret"}, username: "sasl-user", password: "sasl-secret"},
		{name: "EXTERNAL", settings: driver.SASLConfig{Mechanism: "EXTERNAL", Username: "ignored-user", Password: "ignored-secret"}, username: "uri-user", password: "uri-secret"},
		{name: "empty PLAIN identity", settings: driver.SASLConfig{Mechanism: "plain"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			assertManagementLogin(t, "amqp://uri-user:uri-secret@localhost:5672/", &test.settings, test.username, test.password)
			assertOpenSASL(t, &test.settings)
		})
	}
}

func assertManagementLogin(t *testing.T, endpoint string, settings *driver.SASLConfig, username, password string) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotUser, gotPassword, ok := r.BasicAuth()
		if !ok || gotUser != username || gotPassword != password {
			t.Error("management HTTP Basic Auth does not match the AMQP login")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, "[]")
	}))
	defer server.Close()
	port := server.URL[strings.LastIndex(server.URL, ":")+1:]
	client, err := newManagementClient(endpoint, driver.Config{
		SASL: settings, DriverOptions: map[string]string{managementPortOption: port},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.listQueues(t.Context()); err != nil {
		t.Fatal(err)
	}
}
