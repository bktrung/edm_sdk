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
	// The keys are checked by validateConfig, not here. A caller that fills
	// BrokerConfig.DriverOptions in Go reaches that check and never reaches
	// this decoder, so a refusal that lived here would be a property of the
	// file format rather than of the configuration.
	options := make(map[string]string, len(raw.Kafka)+len(raw.RabbitMQ))
	for key, option := range raw.Kafka {
		options["kafka."+key] = option
	}
	for key, option := range raw.RabbitMQ {
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

// validateOptionKeys refuses every driver option that has no reader, and is the
// one place that decision is made: the configuration file and a caller that
// fills BrokerConfig.DriverOptions in Go both arrive here.
//
// A key names its driver and then the option, the way the file spells it:
// "kafka.compression" is Kafka's compression. Both halves are checked, so a key
// is accepted only when the prefix names the driver this configuration declares
// and the rest is a name on that driver's list.
//
// The refused shapes are the ones that reach no reader. A key of the other
// broker, or one carrying no driver prefix, is delivered into a map the opened
// driver never looks at; a name the driver's list does not hold is dropped by
// the resolver that would otherwise read it. Both leave the driver on its own
// default, so the deployment runs on a value it never chose and nothing says
// so. And because the list is what makes a name a name, a driver the core holds
// no list for - inmem reads no option - reads none of its namespace either.
//
// The refusal names the key and the section. It is the sentence the YAML
// decoder produced before this check moved here, so a caller who moves an
// option out of the file and into Go reads the same words.
func validateOptionKeys(options map[string]string, driver string) error {
	for key := range options {
		name, prefixed := strings.CutPrefix(key, driver+".")
		if prefixed && driverReadsOption(driver, name) {
			continue
		}
		return fmt.Errorf("f1: unknown broker.%s", key)
	}
	return nil
}

// driverReadsOption reports whether the named driver resolves the option. A
// driver this list does not name resolves nothing.
func driverReadsOption(driver, name string) bool {
	switch driver {
	case "kafka":
		return validKafkaOption(name)
	case "rabbitmq":
		return validRabbitMQOption(name)
	}
	return false
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
	case "compression", "batchLinger", "fetchMaxBytes", "sessionTimeout", "rebalanceTimeout", "staticMembership", "balancer", "maxExpectedInstances":
		return true
	}
	return false
}

func validRabbitMQOption(key string) bool {
	switch key {
	case "vhost", "queueType", "consumerTimeout", "managementPort":
		return true
	}
	return false
}
