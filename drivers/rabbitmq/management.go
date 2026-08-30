package rabbitmq

import (
	"context"
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
	Name          string         `json:"name"`
	Messages      int64          `json:"messages"`
	MessagesReady int64          `json:"messages_ready"`
	Consumers     int64          `json:"consumers"`
	Arguments     map[string]any `json:"arguments"` // broker-reported argument values are heterogeneous JSON scalars
}

type managementBinding struct {
	Source          string `json:"source"`
	Destination     string `json:"destination"`
	DestinationType string `json:"destination_type"`
	RoutingKey      string `json:"routing_key"`
}

func newManagementClient(endpoint string, cfg driver.Config) (*managementClient, error) {
	parsed, err := url.Parse(endpoint)
	if err != nil {
		var urlErr *url.Error
		if errors.As(err, &urlErr) {
			err = urlErr.Err
		}
		return nil, fmt.Errorf("rabbitmq: invalid management endpoint: %w", err)
	}
	if parsed.Hostname() == "" {
		// Opaque and path fields can retain raw credential-looking input; keep only the scheme.
		redacted := url.URL{Scheme: parsed.Scheme}
		return nil, fmt.Errorf("rabbitmq: invalid management endpoint %q: %w", redacted.String(), errors.New("missing host"))
	}
	scheme := "http"
	managementPort := 15672
	if parsed.Scheme == "amqps" || (cfg.TLS != nil && cfg.TLS.Enabled) {
		scheme = "https"
		managementPort = 15671
	}
	if parsed.Port() == "5671" || parsed.Port() == "15671" {
		managementPort = 15671
	}
	if parsed.Port() == "15672" {
		managementPort = 15672
	}
	if configured := strings.TrimSpace(cfg.DriverOptions[managementPortOption]); configured != "" {
		port, parseErr := strconv.Atoi(configured)
		if parseErr != nil || port < 1 || port > 65535 {
			return nil, fmt.Errorf("rabbitmq: invalid %s %q: want a port from 1 to 65535", managementPortOption, configured)
		}
		managementPort = port
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
	vhost := cfg.DriverOptions["rabbitmq.vhost"]
	if vhost == "" {
		vhost = "/"
		if escaped := strings.TrimPrefix(parsed.EscapedPath(), "/"); escaped != "" {
			decoded, decodeErr := url.PathUnescape(escaped)
			if decodeErr != nil {
				return nil, fmt.Errorf("rabbitmq: invalid management endpoint vhost %q: %w", parsed.EscapedPath(), decodeErr)
			}
			if decoded != "" {
				vhost = "/" + strings.TrimPrefix(decoded, "/")
			}
		}
	}
	if vhost == "" {
		vhost = "/"
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
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.TLSClientConfig = tlsClientConfig
		client.Transport = transport
	}
	return &managementClient{
		baseURL:  scheme + "://" + host,
		username: username,
		password: password,
		vhost:    vhost,
		client:   client,
	}, nil
}

func (m *managementClient) queuePath(name string) string {
	return m.baseURL + "/api/queues/" + url.PathEscape(m.vhost) + "/" + url.PathEscape(name)
}

func (m *managementClient) queuesPath() string {
	return m.baseURL + "/api/queues/" + url.PathEscape(m.vhost)
}

func (m *managementClient) bindingsPath() string {
	return m.baseURL + "/api/bindings/" + url.PathEscape(m.vhost)
}

func (m *managementClient) listQueues(ctx context.Context) ([]managementQueue, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, m.queuesPath(), nil)
	if err != nil {
		return nil, err
	}
	request.SetBasicAuth(m.username, m.password)
	response, err := m.client.Do(request)
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

// getQueue fetches one queue's current state, including its broker-recorded
// arguments. Unlike QueueDeclarePassive, this reports what the broker
// actually holds, which is what argument-drift detection compares against.
func (m *managementClient) getQueue(ctx context.Context, name string) (managementQueue, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, m.queuePath(name), nil)
	if err != nil {
		return managementQueue{}, err
	}
	request.SetBasicAuth(m.username, m.password)
	response, err := m.client.Do(request)
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
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, m.bindingsPath(), nil)
	if err != nil {
		return nil, err
	}
	request.SetBasicAuth(m.username, m.password)
	response, err := m.client.Do(request)
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

func (m *managementClient) deleteQueue(ctx context.Context, name string) (bool, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodDelete, m.queuePath(name), nil)
	if err != nil {
		return false, err
	}
	request.SetBasicAuth(m.username, m.password)
	response, err := m.client.Do(request)
	if err != nil {
		return false, err
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNotFound {
		return false, nil
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return false, fmt.Errorf("management API DELETE queue %q: %s: %s", name, response.Status, strings.TrimSpace(string(body)))
	}
	return true, nil
}

func (queue managementQueue) totalMessages() int64 {
	if queue.Messages > 0 {
		return queue.Messages
	}
	return queue.MessagesReady
}
