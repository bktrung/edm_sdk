# Graceful shutdown

Stop accepting new work, drain active runners, and close the client with a
deadline. F1's drain protocol settles accepted deliveries before the client
releases driver resources.

## Drain a consumer on a signal

The runnable consumer example uses this pattern:

```go
signals := make(chan os.Signal, 1)
signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
defer signal.Stop(signals)

select {
case err := <-runDone:
    return err
case <-signals:
    drainCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
    defer cancel()
    if err := runner.Drain(drainCtx); err != nil {
        return fmt.Errorf("drain: %w", err)
    }
    if err := <-runDone; err != nil {
        return err
    }
}
```

Always close the client after the runner has stopped:

```go
closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()
if err := client.Close(closeCtx); err != nil {
    return fmt.Errorf("close F1 client: %w", err)
}
```

`Client.Close` also drains registered runners, so it is the final safety net
when a service owns multiple subscriptions. Producer-close and
connection-close errors are returned; keep and report them rather than
discarding them. A successful close is safe to call again.

See [`examples/consumer/main.go`](https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/examples/consumer/main.go) for the full
signal and cleanup flow, and [Lifecycle and shutdown](/advanced-topics/lifecycle-and-shutdown)
for the runtime state machine.
