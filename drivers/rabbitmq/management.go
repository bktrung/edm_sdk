package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

type managementClient struct {
	baseURL  string
	username string
	password string
	vhost    string
	client   http.Client
}

type managementQueue struct {
	Name          string `json:"name"`
	Messages      int64  `json:"messages"`
	MessagesReady int64  `json:"messages_ready"`
	Consumers     int64  `json:"consumers"`
}

func newManagementClient(endpoint string) *managementClient {
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Hostname() == "" {
		return &managementClient{baseURL: "http://localhost:15672", username: "guest", vhost: "/"}
	}
	scheme := "http"
	managementPort := 15672
	if parsed.Scheme == "amqps" {
		scheme = "https"
		managementPort = 15671
	}
	if parsed.Port() == "5671" {
		managementPort = 15671
	}
	host := net.JoinHostPort(parsed.Hostname(), strconv.Itoa(managementPort))
	username, password := "guest", "guest"
	if parsed.User != nil {
		username = parsed.User.Username()
		password, _ = parsed.User.Password()
	}
	vhost := "/"
	if escaped := strings.TrimPrefix(parsed.EscapedPath(), "/"); escaped != "" {
		if decoded, decodeErr := url.PathUnescape(escaped); decodeErr == nil && decoded != "" {
			vhost = "/" + strings.TrimPrefix(decoded, "/")
		}
	}
	return &managementClient{
		baseURL:  scheme + "://" + host,
		username: username,
		password: password,
		vhost:    vhost,
	}
}

func (m *managementClient) queuePath(name string) string {
	return m.baseURL + "/api/queues/" + url.PathEscape(m.vhost) + "/" + url.PathEscape(name)
}

func (m *managementClient) queuesPath() string {
	return m.baseURL + "/api/queues/" + url.PathEscape(m.vhost)
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
