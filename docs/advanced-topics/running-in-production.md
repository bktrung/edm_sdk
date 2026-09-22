# Running in production

A production service should connect to a real broker driver, provision its topology separately, expose a readiness probe, and shut down through `Client.Close`. The sections below connect those operational choices to F1's enforced configuration and runtime behavior.

For a runnable reference, see [`examples/consumer/main.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/examples/consumer/main.go).

## Production configuration

Set `f1.env` to exactly `prod`. `production` and `prd` are recognized as production aliases, but F1 refuses them because production configuration must use the canonical value. This exact-value check is enforced by `validateConfig`. `F1_ENV` overrides the file value; see [Environment overrides](/drivers-and-capabilities#environment-overrides) for the complete override table.

The following rules are enforced when `env: prod` is validated:

| Rule | Applies to | Enforcement |
| --- | --- | --- |
| The in-memory driver is not allowed. | Every driver selection | F1 refuses `inmem` rather than allowing a process to start without a broker. |
| SASL requires TLS. | Kafka and RabbitMQ configurations that select a SASL mechanism | Set `broker.tls.enabled: true` before enabling SASL. The [Security](/drivers-and-capabilities#security) section documents the TLS and SASL fields. |
| Insecure server-certificate verification is not allowed. | Kafka and RabbitMQ | `broker.tls.insecureSkipVerify: true` is refused. Use a CA file and normal certificate verification; see [Security](/drivers-and-capabilities#security). |
| Topology auto-creation is not allowed. | Every driver | Set `topology.autoCreate: false` and provision destinations before startup. |
| RabbitMQ queues must use quorum type. | RabbitMQ | Set `broker.rabbitmq.queueType: quorum`. The raw value must be exactly `quorum`; see [RabbitMQ options](/drivers-and-capabilities#rabbitmq-options). |

Every row above is enforced by [`validateConfig`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/config.go) during configuration validation.

These are validation rules, not a replacement for broker security configuration. Review the [Security](/drivers-and-capabilities#security) section for endpoint schemes, CA files, client certificates, and supported SASL mechanisms. Keep credentials and driver-specific settings in the service configuration layer; do not put them in handler code.

## Health probes

Use `Client.Health` for readiness. It first checks whether the client admits a health call, then pings the current broker connection, and finally reports any subscription failures recorded by the client. A non-nil error from admission or `Ping` is returned before failed subscription errors are inspected. The implementation is [`Client.Health`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/client.go); `admit` decides whether a health call is allowed before any broker call is made.

Give the readiness request a short deadline so a probe does not hold an HTTP worker while a broker call is stuck:

```go
func readinessHandler(client *f1.Client) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()

		if err := client.Health(ctx); err != nil {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
}
```

The `2*time.Second` value is an example probe deadline, not an F1 default. Choose it for the service and broker latency budget. Do not use `Health` for liveness. A broker outage makes the broker ping or connection admission fail while F1 is reconnecting, so a liveness restart would interrupt recovery and can create a restart loop.

During a transient outage, F1's reconnect supervisor abandons affected runners, waits for in-flight publishing to become idle, and tries to open a replacement connection. Each attempt uses full-jitter exponential backoff, rechecks publisher topology, and installs the replacement before retiring the old connection. Transient failures are retried until `broker.maxReconnectAttempts` is exhausted when that limit is set; a successful replacement wakes waiters and reports the restored connection to the observer when one is installed. See [`reconnect.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/reconnect.go) for the supervisor and the single attempt it repeats.

Keep liveness independent of broker reachability. It should answer whether the process and its serving loop are alive; readiness is the check that should remove the instance from traffic while F1 reconnects.

## Shutdown on SIGTERM

Use `signal.NotifyContext` to observe `SIGTERM`, then call `Close` with a
fresh timeout context. The timeout must finish before the orchestrator's
termination grace period. The runnable consumer reserves 5 seconds for
readiness shutdown, 15 seconds for draining, 5 seconds for `Client.Close`,
and 2 seconds for telemetry-provider shutdown, 27 seconds within a
30-second Kubernetes `terminationGracePeriodSeconds`. These are deployment
choices, not SDK defaults; the runnable consumer shows the composition and `LifecycleConfig`
owns the SDK defaults.

The example below drains no runner of its own, so its `Close` absorbs the drain
as well and is budgeted accordingly. Split the budget the way the runnable
consumer does when the service drains its runners first.

```go
func shutdownOnSignal(client *f1.Client) error {
	signalCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	<-signalCtx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := client.Close(shutdownCtx); err != nil {
		return fmt.Errorf("close F1 client: %w", err)
	}
	return nil
}
```

Do not pass `signalCtx` to `Close`: it is already canceled when the signal branch runs. `Close` stops new work, drains registered runners, waits for in-flight publishing, closes the shared producer, and then closes the driver connection. A timed-out phase continues in the background; a retried `Close` rejoins that phase instead of starting a second driver call. A concurrent close is rejected, while a later call is safe after shutdown completes. See [`Client.Close`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/client.go) and [Lifecycle and shutdown](/advanced-topics/lifecycle-and-shutdown) for the full sequence and lifecycle budgets.

## Observability

Install `f1otel` with `f1.WithObserver` and follow the [observability guide](/advanced-topics/observability) for the adapter contract and setup. For alert thresholds and escalation policy, see [Observability alerts](/advanced-topics/alerts) and keep service-specific policy with the service or platform team.

## Pre-launch checklist

- [ ] Set `f1.env` to exactly `prod`; confirm any `F1_ENV` override in [Environment overrides](/drivers-and-capabilities#environment-overrides).
- [ ] Select Kafka or RabbitMQ for production; confirm the in-memory driver is not selected ([Production configuration](#production-configuration)).
- [ ] Review endpoint TLS, CA, client certificate, and SASL settings in [Security](/drivers-and-capabilities#security).
- [ ] Confirm `broker.tls.insecureSkipVerify` is false and production SASL has TLS enabled ([Production configuration](#production-configuration)).
- [ ] Provision destinations before startup and leave topology auto-creation disabled ([Production configuration](#production-configuration)).
- [ ] Use RabbitMQ quorum queues when the RabbitMQ driver is selected ([Production configuration](#production-configuration)).
- [ ] Expose `Client.Health` as readiness with a short request deadline ([Health probes](#health-probes)).
- [ ] Keep liveness independent of broker reachability ([Health probes](#health-probes)).
- [ ] Handle `SIGTERM` with a fresh shutdown context and a deadline shorter than the orchestrator grace period ([Shutdown on SIGTERM](#shutdown-on-sigterm)).
- [ ] Set lifecycle budgets from handler and broker behavior ([Lifecycle and shutdown](/advanced-topics/lifecycle-and-shutdown#configure-shutdown-budgets)).
- [ ] Install `f1otel` and review the [observability guide](/advanced-topics/observability).
