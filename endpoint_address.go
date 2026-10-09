package f1

import (
	"net"
	"net/url"
	"strconv"
	"strings"
)

// endpointAddress parses the first configured endpoint into its host and port.
// A string with a scheme uses net/url so userinfo, path and query never reach
// the result. One without a scheme uses net.SplitHostPort and falls back to
// the bare host. Empty or unparsable input gives "", 0.
func endpointAddress(endpoint string) (string, int) {
	if endpoint == "" {
		return "", 0
	}
	if strings.Contains(endpoint, "://") {
		parsed, err := url.Parse(endpoint)
		if err != nil {
			return "", 0
		}
		host := parsed.Hostname()
		if host == "" {
			return "", 0
		}
		portText := parsed.Port()
		if portText == "" {
			return host, 0
		}
		port, err := strconv.Atoi(portText)
		if err != nil {
			return "", 0
		}
		return host, port
	}
	host, portText, err := net.SplitHostPort(endpoint)
	if err == nil {
		if host == "" {
			return "", 0
		}
		port, convErr := strconv.Atoi(portText)
		if convErr != nil {
			return "", 0
		}
		return host, port
	}
	if strings.ContainsAny(endpoint, " /?#@") {
		return "", 0
	}
	trimmed := strings.Trim(endpoint, "[]")
	if trimmed == "" || strings.Contains(trimmed, ":") {
		return "", 0
	}
	return trimmed, 0
}
