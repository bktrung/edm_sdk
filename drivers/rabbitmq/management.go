package rabbitmq

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

const defaultManagementTimeout = 30 * time.Second

const managementPortOption = "rabbitmq.managementPort"

// errQueueNotFound marks a management API 404 for one queue, distinct from a
// transport or authorisation failure so callers can decide separately
// whether "the queue is gone" is expected.
var errQueueNotFound = errors.New("rabbitmq: queue not found")

type managementClient struct {
	baseURL  string
	username string
	password string
	vhost    string
	client   http.Client
}

type managementQueue struct {
	Name                 string         `json:"name"`
	Messages             int64          `json:"messages"`
	MessagesReady        int64          `json:"messages_ready"`
	Consumers            int64          `json:"consumers"`
	Arguments            map[string]any `json:"arguments"` // broker-reported argument values are heterogeneous JSON scalars
	HeadMessageTimestamp int64          `json:"head_message_timestamp"`
}
type managementExchange struct {
	Name string `json:"name"`
}

type managementBinding struct {
	Source          string `json:"source"`
	Destination     string `json:"destination"`
	DestinationType string `json:"destination_type"`
	RoutingKey      string `json:"routing_key"`
}

// invalidEndpointError reports an endpoint that cannot be read as an AMQP URI
// without repeating the endpoint: a malformed one can carry credential-looking
// text. net/url quotes its input in the error it returns, so drop that layer
// before wrapping.
func invalidEndpointError(err error) error {
	if urlErr, ok := errors.AsType[*url.Error](err); ok {
		err = urlErr.Err
	}
	return fmt.Errorf("rabbitmq: invalid management endpoint: %w", err)
}

// managementTransport returns the transport that reaches the management API
// over TLS. It starts from a clone of http.DefaultTransport while that is the
// standard transport, and from a fresh one with the standard transport's
// timeouts when an application or an instrumentation library has replaced it
// with another RoundTripper, which has no TLS configuration to set.
func managementTransport(tlsClientConfig *tls.Config) *http.Transport {
	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: time.Second,
	}
	if standard, ok := http.DefaultTransport.(*http.Transport); ok {
		transport = standard.Clone()
	}
	transport.TLSClientConfig = tlsClientConfig
	return transport
}

type resolvedEndpoint struct {
	endpoint string
	parsed   *url.URL
	vhost    string
	username string
	password string
	sasl     []amqp.Authentication
}

func resolveEndpoint(endpoint string, cfg driver.Config) (resolvedEndpoint, error) {
	resolved := resolvedEndpoint{endpoint: endpoint}
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return resolved, invalidEndpointError(err)
	}
	if parsed.Hostname() == "" {
		redacted := url.URL{Scheme: parsed.Scheme}
		return resolved, fmt.Errorf("rabbitmq: invalid management endpoint %q: %w", redacted.String(), errUnsupportedHostlessEndpoint)
	}
	if err := validateEndpoint(endpoint); err != nil {
		return resolved, err
	}
	uri, err := amqp.ParseURI(endpoint)
	if err != nil {
		return resolved, invalidEndpointError(err)
	}
	if configured := cfg.DriverOptions["rabbitmq.vhost"]; configured != "" && configured != uri.Vhost {
		return resolved, fmt.Errorf("rabbitmq: rabbitmq.vhost %q does not match endpoint vhost %q", configured, uri.Vhost)
	}
	if err := validateSASL(cfg.SASL); err != nil {
		return resolved, err
	}
	resolved.parsed = parsed
	resolved.vhost = uri.Vhost
	resolved.username, resolved.password = uri.Username, uri.Password
	if cfg.SASL != nil {
		switch strings.ToLower(cfg.SASL.Mechanism) {
		case "plain", "amqplain":
			resolved.username, resolved.password = cfg.SASL.Username, cfg.SASL.Password
			var auth amqp.Authentication
			if strings.EqualFold(cfg.SASL.Mechanism, "plain") {
				auth = &amqp.PlainAuth{Username: resolved.username, Password: resolved.password}
			} else {
				auth = &amqp.AMQPlainAuth{Username: resolved.username, Password: resolved.password}
			}
			resolved.sasl = []amqp.Authentication{auth}
		case "external":
			// EXTERNAL authenticates AMQP by certificate, not HTTP Basic Auth.
			resolved.sasl = []amqp.Authentication{&amqp.ExternalAuth{}}
		}
	}
	return resolved, nil
}

func newManagementClient(endpoint string, cfg driver.Config) (*managementClient, error) {
	resolved, err := resolveEndpoint(endpoint, cfg)
	if err != nil {
		return nil, err
	}
	return managementClientForEndpoint(resolved, cfg)
}

func managementClientForEndpoint(endpoint resolvedEndpoint, cfg driver.Config) (*managementClient, error) {
	parsed := endpoint.parsed
	scheme := "http"
	if parsed.Scheme == "amqps" || (cfg.TLS != nil && cfg.TLS.Enabled) {
		scheme = "https"
	}
	managementPort, portErr := resolveManagementPort(parsed, cfg)
	if portErr != nil {
		return nil, portErr
	}
	host := net.JoinHostPort(parsed.Hostname(), strconv.Itoa(managementPort))
	timeout := cfg.ConnectTimeout
	if timeout <= 0 {
		timeout = defaultManagementTimeout
	}
	client := http.Client{Timeout: timeout}
	if cfg.TLS != nil && cfg.TLS.Enabled {
		tlsSettings := *cfg.TLS
		tlsSettings.InsecureSkipVerify = cfg.TLS.InsecureSkipVerify
		tlsClientConfig, tlsErr := tlsConfig(&tlsSettings)
		if tlsErr != nil {
			return nil, tlsErr
		}
		client.Transport = managementTransport(tlsClientConfig)
	}
	return &managementClient{
		baseURL:  scheme + "://" + host,
		username: endpoint.username,
		password: endpoint.password,
		vhost:    endpoint.vhost,
		client:   client,
	}, nil
}

func resolveManagementPort(endpoint *url.URL, cfg driver.Config) (int, error) {
	if configured := strings.TrimSpace(cfg.DriverOptions[managementPortOption]); configured != "" {
		port, err := strconv.Atoi(configured)
		if err != nil || port < 1 || port > 65535 {
			return 0, fmt.Errorf("rabbitmq: invalid %s %q: want a port from 1 to 65535", managementPortOption, configured)
		}
		return port, nil
	}

	amqpPort := endpoint.Port()
	if amqpPort == "" {
		amqpPort = "5672"
		if endpoint.Scheme == "amqps" {
			amqpPort = "5671"
		}
	}
	port, err := strconv.Atoi(amqpPort)
	if err != nil || port < 1 || port > 65535 {
		return 0, fmt.Errorf("rabbitmq: invalid AMQP endpoint port %q", amqpPort)
	}
	if port > 65535-10000 {
		return 0, fmt.Errorf("rabbitmq: invalid derived management port from AMQP port %q", amqpPort)
	}
	return port + 10000, nil
}

func (m *managementClient) queuePath(name string) string {
	return m.baseURL + "/api/queues/" + url.PathEscape(m.vhost) + "/" + url.PathEscape(name)
}

func (m *managementClient) queuesPath() string {
	return m.baseURL + "/api/queues/" + url.PathEscape(m.vhost)
}

func (m *managementClient) exchangePath(name string) string {
	return m.baseURL + "/api/exchanges/" + url.PathEscape(m.vhost) + "/" + url.PathEscape(name)
}

func (m *managementClient) exchangesPath() string {
	return m.baseURL + "/api/exchanges/" + url.PathEscape(m.vhost)
}

func (m *managementClient) bindingsPath() string {
	return m.baseURL + "/api/bindings/" + url.PathEscape(m.vhost)
}

// do sends an authenticated management request. The caller closes the
// response body.
func (m *managementClient) do(ctx context.Context, method, path string) (*http.Response, error) {
	request, err := http.NewRequestWithContext(ctx, method, path, nil)
	if err != nil {
		return nil, err
	}
	request.SetBasicAuth(m.username, m.password)
	return m.client.Do(request)
}

type managementHTTPError struct {
	statusCode int
	method     string
	resource   string
	status     string
	body       string
}

// Error preserves the management response's diagnostic text.
func (e *managementHTTPError) Error() string {
	return fmt.Sprintf("management API %s %s: %s: %s", e.method, e.resource, e.status, e.body)
}

func newManagementHTTPError(response *http.Response, method, resource string) error {
	body, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
	return &managementHTTPError{
		statusCode: response.StatusCode,
		method:     method,
		resource:   resource,
		status:     response.Status,
		body:       strings.TrimSpace(string(body)),
	}
}

func classifyManagement(op string, err error) error {
	kind := driver.KindTransient
	if httpErr, ok := errors.AsType[*managementHTTPError](err); ok {
		if httpErr.statusCode == http.StatusUnauthorized || httpErr.statusCode == http.StatusForbidden {
			kind = driver.KindPermission
		}
	}
	return classify(op, kind, err)
}

func managementGet[T any](ctx context.Context, m *managementClient, path, resource string) (T, error) {
	var value T
	response, err := m.do(ctx, http.MethodGet, path)
	if err != nil {
		return value, err
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return value, newManagementHTTPError(response, http.MethodGet, resource)
	}
	if err := json.NewDecoder(response.Body).Decode(&value); err != nil {
		var zero T
		return zero, fmt.Errorf("management API decode %s: %w", resource, err)
	}
	return value, nil
}

func (m *managementClient) listQueues(ctx context.Context) ([]managementQueue, error) {
	return managementGet[[]managementQueue](ctx, m, m.queuesPath(), "queues")
}

func (m *managementClient) listExchanges(ctx context.Context) ([]managementExchange, error) {
	return managementGet[[]managementExchange](ctx, m, m.exchangesPath(), "exchanges")
}

// getQueue fetches one queue's current state, including its broker-recorded
// arguments. Unlike QueueDeclarePassive, this reports what the broker
// actually holds, which is what argument-drift detection compares against.
func (m *managementClient) getQueue(ctx context.Context, name string) (managementQueue, error) {
	response, err := m.do(ctx, http.MethodGet, m.queuePath(name))
	if err != nil {
		return managementQueue{}, err
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNotFound {
		return managementQueue{}, fmt.Errorf("management API GET queue %q: %w", name, errQueueNotFound)
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return managementQueue{}, newManagementHTTPError(response, http.MethodGet, fmt.Sprintf("queue %q", name))
	}
	var queue managementQueue
	if err := json.NewDecoder(response.Body).Decode(&queue); err != nil {
		return managementQueue{}, fmt.Errorf("management API decode queue %q: %w", name, err)
	}
	return queue, nil
}

func (m *managementClient) listBindings(ctx context.Context) ([]managementBinding, error) {
	return managementGet[[]managementBinding](ctx, m, m.bindingsPath(), "bindings")
}

func (m *managementClient) deleteExchange(ctx context.Context, name string) (bool, error) {
	response, err := m.do(ctx, http.MethodDelete, m.exchangePath(name))
	if err != nil {
		return false, err
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNotFound {
		return false, nil
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return false, newManagementHTTPError(response, http.MethodDelete, fmt.Sprintf("exchange %q", name))
	}
	return true, nil
}

func (queue managementQueue) totalMessages() int64 {
	if queue.Messages > 0 {
		return queue.Messages
	}
	return queue.MessagesReady
}

func (queue managementQueue) headEnqueuedAt() (time.Time, driver.EnqueueSource) {
	if queue.HeadMessageTimestamp <= 0 {
		return time.Time{}, driver.EnqueueSourceUnknown
	}
	return time.Unix(queue.HeadMessageTimestamp, 0), driver.EnqueueSourceProducer
}
