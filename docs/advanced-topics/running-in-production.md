# Running in production

A production service should use a real broker driver, provision topology separately, expose readiness, and shut down through `Client.Close`.

For a runnable composition, see [`examples/consumer/main.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/examples/consumer/main.go).

## Set production configuration

Use `f1.env: prod`. Only that exact value turns on the production checks; `production`, `prd`, and other spellings such as `PROD` are rejected at startup. Every environment name may use only letters, digits, `-`, and `_`, so a value such as `prod/us` is rejected instead of sharing production's destinations without its checks. `F1_ENV` overrides the file value; see [Environment overrides](/drivers-and-capabilities#environment-overrides).

When `env: prod` is validated, F1 enforces these rules:

| Rule | Applies to |
| --- | --- |
| The in-memory driver is not allowed. | Every driver selection |
| SASL requires TLS. | Kafka and RabbitMQ configurations that select SASL |
| Insecure server-certificate verification is not allowed. | Kafka and RabbitMQ |
| Topology auto-creation is not allowed. | Every driver |
| RabbitMQ queues must use quorum type. | RabbitMQ |

Set `topology.autoCreate: false` and provision destinations before startup. Use a CA file and normal certificate verification. Keep credentials and driver-specific settings in the service configuration layer, not in handler code. See [Security](/drivers-and-capabilities#security) for the transport fields.

## Use health as readiness

Use `Client.Health` for readiness. It checks that the client still accepts work, pings the current broker connection, and then reports subscription failures. A closed-client or ping error is returned before a recorded subscription error.

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

The two-second deadline is an example probe time limit, not an F1 default. Do not use `Health` for liveness. A broker outage can make readiness fail while F1 reconnects; restarting for that failure can interrupt recovery and create a restart loop.

Keep liveness independent of broker reachability. It should answer whether the process and its serving loop are alive; readiness should remove the instance from traffic while F1 reconnects.

## Shut down on SIGTERM

Observe `SIGTERM` with `signal.NotifyContext`, then call `Client.Close` with a fresh timeout context. The timeout must finish before the orchestrator's termination grace period.

The runnable consumer reserves 5 seconds for readiness shutdown, 15 seconds for draining, 5 seconds for close, and 2 seconds for telemetry shutdown, 27 seconds total. That example is sized for Kubernetes' default `terminationGracePeriodSeconds` of 30 seconds. These are deployment choices, not SDK defaults; configure lifecycle time limits for the service.

`Client.Close` drains every registered runner, waits for in-flight publishing, closes the shared producer, and then closes the driver connection. A timed-out phase can continue in the background, and a later close rejoins it instead of starting a duplicate driver call. Use the [Close sequence](/advanced-topics/lifecycle-and-shutdown#close-in-ack-last-order) as the process shutdown contract.

## RabbitMQ's own dead-letter queue

RabbitMQ production queues must use quorum type. F1's main and retry quorum queue declarations carry `x-dead-letter-strategy=at-least-once` and `x-overflow=reject-publish` and dead-letter to [the broker's own dead-letter queue](/learn/glossary#backstop) `f1.<environment>.<topic>.dlq.<subscription>.backstop`. When topology is provisioned outside F1, those arguments must match. F1 does not consume this queue or invoke `OnDeadLetter` for entries that reach it.

The broker's delivery limit is the retry policy's `MaxAttempts` plus 5, so about `MaxAttempts + 5` consecutive crash restarts can move a message no handler saw to that queue; stop the crash loop rather than raise the limit.

On a quorum queue, a graceful `Close` returns deliveries that RabbitMQ handed out but no handler had started. F1 returns each one with a requeue nack, so it counts once against the broker's delivery limit above. It does not use an F1 retry attempt.

RabbitMQ [parking queues](/learn/glossary#parking-queue) have no length cap. A parked retry leaves when its delay has passed, so a step's queue holds about its retry rate times its delay. A parking queue that keeps growing means handlers fail faster than retries drain; alert on its depth rather than expecting the broker to refuse new retries.

::: warning
Treat a non-empty broker dead-letter queue as an operational incident. Inspect and drain it with broker tooling; it is not the normal F1 dead-letter path.
:::

## Add observability

Install `f1otel` with `f1.WithObserver` and use the [observability guide](/advanced-topics/observability) for metrics, tracing, provider ownership, and timestamp prerequisites. Use [Alerts](/advanced-topics/alerts) for starting PromQL and keep service-specific thresholds with the service or platform team.

## Pre-launch checklist

- [ ] Set `f1.env` to exactly `prod`; confirm any `F1_ENV` override in [Environment overrides](/drivers-and-capabilities#environment-overrides).
- [ ] Select Kafka or RabbitMQ for production; do not select the in-memory driver.
- [ ] Review endpoint TLS, CA, client certificate, and SASL settings in [Security](/drivers-and-capabilities#security).
- [ ] Confirm `broker.tls.insecureSkipVerify` is false and production SASL has TLS enabled.
- [ ] Provision destinations before startup and leave topology auto-creation disabled.
- [ ] Use RabbitMQ quorum queues when the RabbitMQ driver is selected.
- [ ] Expose `Client.Health` as readiness with a short request deadline.
- [ ] Keep liveness independent of broker reachability.
- [ ] Handle `SIGTERM` with a fresh shutdown context and a deadline shorter than the orchestrator grace period.
- [ ] Set lifecycle time limits from handler and broker behavior.
- [ ] Install `f1otel` and review its timestamp and provider shutdown requirements.

## Go further

- [Lifecycle and shutdown](/advanced-topics/lifecycle-and-shutdown) - runner drain and client close;
- [Observability](/advanced-topics/observability) - metrics, tracing, and provider ownership;
- [Alerts](/advanced-topics/alerts) - operational signals and triage; and
- [Drivers and capabilities](/drivers-and-capabilities) - security and provider configuration.
