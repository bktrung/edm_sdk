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

func newManagementClient(endpoint string, cfg driver.Config) (*managementClient, error) {
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return nil, invalidEndpointError(err)
	}
	if parsed.Hostname() == "" {
		// Opaque and path fields can retain raw credential-looking input; keep only the scheme.
		redacted := url.URL{Scheme: parsed.Scheme}
		return nil, fmt.Errorf("rabbitmq: invalid management endpoint %q: %w", redacted.String(), errUnsupportedHostlessEndpoint)
	}
	scheme := "http"
	if parsed.Scheme == "amqps" || (cfg.TLS != nil && cfg.TLS.Enabled) {
		scheme = "https"
	}
	managementPort, portErr := resolveManagementPort(parsed, cfg)
	if portErr != nil {
		return nil, portErr
	}
	if parsed.Scheme != "amqps" && !isLoopbackEndpoint(endpoint) {
		return nil, errors.New("rabbitmq: plaintext connection to non-loopback host requires an amqps:// endpoint")
	}
	host := net.JoinHostPort(parsed.Hostname(), strconv.Itoa(managementPort))
	username, password := "", ""
	if parsed.User != nil {
		username = parsed.User.Username()
		password, _ = parsed.User.Password()
	}
	if cfg.SASL != nil && (cfg.SASL.Username != "" || cfg.SASL.Password != "") {
		username = cfg.SASL.Username
		password = cfg.SASL.Password
	}
	amqpURI, uriErr := amqp.ParseURI(endpoint)
	if uriErr != nil {
		return nil, invalidEndpointError(uriErr)
	}
	vhost := cfg.DriverOptions["rabbitmq.vhost"]
	if vhost == "" {
		// The AMQP connection reads its vhost from this same endpoint through
		// this parse, so re-deriving it here is how the two readers drifted
		// apart. One parse, one answer.
		vhost = amqpURI.Vhost
	}
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
		username: username,
		password: password,
		vhost:    vhost,
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

func (m *managementClient) listQueues(ctx context.Context) ([]managementQueue, error) {
	response, err := m.do(ctx, http.MethodGet, m.queuesPath())
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return nil, fmt.Errorf("management API GET queues: %s: %s", response.Status, strings.TrimSpace(string(body)))
	}
	var queues []managementQueue
	if err := json.NewDecoder(response.Body).Decode(&queues); err != nil {
		return nil, fmt.Errorf("management API decode queues: %w", err)
	}
	return queues, nil
}

func (m *managementClient) listExchanges(ctx context.Context) ([]managementExchange, error) {
	response, err := m.do(ctx, http.MethodGet, m.exchangesPath())
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return nil, fmt.Errorf("management API GET exchanges: %s: %s", response.Status, strings.TrimSpace(string(body)))
	}
	var exchanges []managementExchange
	if err := json.NewDecoder(response.Body).Decode(&exchanges); err != nil {
		return nil, fmt.Errorf("management API decode exchanges: %w", err)
	}
	return exchanges, nil
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
		body, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return managementQueue{}, fmt.Errorf("management API GET queue %q: %s: %s", name, response.Status, strings.TrimSpace(string(body)))
	}
	var queue managementQueue
	if err := json.NewDecoder(response.Body).Decode(&queue); err != nil {
		return managementQueue{}, fmt.Errorf("management API decode queue %q: %w", name, err)
	}
	return queue, nil
}

func (m *managementClient) listBindings(ctx context.Context) ([]managementBinding, error) {
	response, err := m.do(ctx, http.MethodGet, m.bindingsPath())
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return nil, fmt.Errorf("management API GET bindings: %s: %s", response.Status, strings.TrimSpace(string(body)))
	}
	var bindings []managementBinding
	if err := json.NewDecoder(response.Body).Decode(&bindings); err != nil {
		return nil, fmt.Errorf("management API decode bindings: %w", err)
	}
	return bindings, nil
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
		body, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return false, fmt.Errorf("management API DELETE exchange %q: %s: %s", name, response.Status, strings.TrimSpace(string(body)))
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
