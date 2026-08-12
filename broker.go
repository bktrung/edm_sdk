package f1

import (
	"fmt"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

// BrokerConfig configures how a Client connects to a selected broker driver.
// DriverOptions contains validated broker-specific settings with dotted keys.
type BrokerConfig struct {
	Driver               string
	Endpoints            []string
	ConnectTimeout       time.Duration
	MaxReconnectAttempts int
	DefaultPrefetch      int
	TLS                  driver.TLSConfig
	SASL                 driver.SASLConfig
	DriverOptions        map[string]string
}

type rawBroker BrokerConfig

func (b rawBroker) config() BrokerConfig { return BrokerConfig(b) }

func (b *rawBroker) UnmarshalYAML(value *yaml.Node) error {
	if err := requireKnownKeys(value, "driver", "endpoints", "connectTimeout", "maxReconnectAttempts", "defaultPrefetch", "tls", "sasl", "kafka", "rabbitmq"); err != nil {
		return err
	}
	raw := struct {
		Driver               string            `yaml:"driver"`
		Endpoints            []string          `yaml:"endpoints"`
		ConnectTimeout       time.Duration     `yaml:"connectTimeout"`
		MaxReconnectAttempts int               `yaml:"maxReconnectAttempts"`
		DefaultPrefetch      int               `yaml:"defaultPrefetch"`
		TLS                  rawTLS            `yaml:"tls"`
		SASL                 rawSASL           `yaml:"sasl"`
		Kafka                map[string]string `yaml:"kafka"`
		RabbitMQ             map[string]string `yaml:"rabbitmq"`
	}{
		Driver: b.Driver, Endpoints: b.Endpoints, ConnectTimeout: b.ConnectTimeout,
		MaxReconnectAttempts: b.MaxReconnectAttempts, DefaultPrefetch: b.DefaultPrefetch,
		TLS:   rawTLS{Enabled: b.TLS.Enabled, CAFile: b.TLS.CAFile, CertFile: b.TLS.CertFile, KeyFile: b.TLS.KeyFile, InsecureSkipVerify: b.TLS.InsecureSkipVerify},
		SASL:  rawSASL{Mechanism: b.SASL.Mechanism, Username: b.SASL.Username, Password: b.SASL.Password},
		Kafka: optionSubset(b.DriverOptions, "kafka."), RabbitMQ: optionSubset(b.DriverOptions, "rabbitmq."),
	}
	if err := value.Decode(&raw); err != nil {
		return err
	}
	options := make(map[string]string, len(raw.Kafka)+len(raw.RabbitMQ))
	for key, option := range raw.Kafka {
		if !validKafkaOption(key) {
			return fmt.Errorf("f1: unknown broker.kafka.%s", key)
		}
		options["kafka."+key] = option
	}
	for key, option := range raw.RabbitMQ {
		if !validRabbitMQOption(key) {
			return fmt.Errorf("f1: unknown broker.rabbitmq.%s", key)
		}
		options["rabbitmq."+key] = option
	}
	*b = rawBroker(BrokerConfig{Driver: raw.Driver, Endpoints: raw.Endpoints, ConnectTimeout: raw.ConnectTimeout, MaxReconnectAttempts: raw.MaxReconnectAttempts, DefaultPrefetch: raw.DefaultPrefetch, TLS: raw.TLS.config(), SASL: raw.SASL.config(), DriverOptions: options})
	return nil
}

func optionSubset(options map[string]string, prefix string) map[string]string {
	result := make(map[string]string)
	for key, value := range options {
		if name, ok := strings.CutPrefix(key, prefix); ok {
			result[name] = value
		}
	}
	return result
}

type rawTLS struct {
	Enabled            bool   `yaml:"enabled"`
	CAFile             string `yaml:"caFile"`
	CertFile           string `yaml:"certFile"`
	KeyFile            string `yaml:"keyFile"`
	InsecureSkipVerify bool   `yaml:"insecureSkipVerify"`
}

func (r rawTLS) config() driver.TLSConfig {
	return driver.TLSConfig{Enabled: r.Enabled, CAFile: r.CAFile, CertFile: r.CertFile, KeyFile: r.KeyFile, InsecureSkipVerify: r.InsecureSkipVerify}
}

type rawSASL struct {
	Mechanism string `yaml:"mechanism"`
	Username  string `yaml:"username"`
	Password  string `yaml:"password"`
}

func (r rawSASL) config() driver.SASLConfig {
	return driver.SASLConfig{Mechanism: r.Mechanism, Username: r.Username, Password: r.Password}
}

func validKafkaOption(key string) bool {
	switch key {
	case "compression", "batchLinger", "fetchMaxBytes", "sessionTimeout", "rebalanceTimeout", "useShareGroups", "staticMembership", "maxAckGap", "balancer":
		return true
	}
	return false
}

func validRabbitMQOption(key string) bool {
	switch key {
	case "vhost", "queueType", "quorumInitialGroupSize", "publisherConfirmTimeout", "useNativeDelay", "consumerTimeout", "deliveryLimitMargin", "deadLetterStrategy", "maxLength":
		return true
	}
	return false
}
