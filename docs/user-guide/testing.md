# Testing

Use the in-memory driver for deterministic handler tests. The `f1test` package
wraps it with a fake clock, captures published messages, and cleans up the
client through `t.Cleanup`.

## Test a successful handler

```go
func TestOrderHandler(t *testing.T) {
    client := f1test.NewClient(t)
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    handled := make(chan struct{})
    runner, err := client.Subscribe(ctx, f1.Subscription{
        Name:   "orders-worker",
        Topics: []string{"orders.created"},
        Handlers: map[string]f1.Handler{
            "orders.created.v1": f1.HandlerFunc(func(context.Context, *f1.Event) error {
                close(handled)
                return nil
            }),
        },
    })
    require.NoError(t, err)

    runDone := make(chan error, 1)
    go func() { runDone <- runner.Run(ctx) }()
    defer func() {
        cancel()
        require.NoError(t, <-runDone)
    }()

    client.Deliver(t, "orders.created.v1", map[string]string{"id": "order-1"})
    select {
    case <-handled:
    case <-time.After(time.Second):
        t.Fatal("handler did not run")
    }
}
```

The complete runnable patterns live in [`f1test/f1test_test.go`](../../f1test/f1test_test.go)
and [`f1test/fanout_test.go`](../../f1test/fanout_test.go). Import the normal Go
testing and assertion packages used by the repository.

## Test retries and dead letters

Use `f1.RetryAfter`, `f1.Terminal`, or `f1.Drop` in the handler, then inspect
`client.Published()` and `client.DLQ()`. Advance retry time with
`client.Advance(duration)` instead of sleeping. See the retry and dead-letter
cases in [`f1test/f1test_test.go`](../../f1test/f1test_test.go).

## Test the public contract

Use the in-memory driver for application behavior. Use the repository's
conformance tests and RabbitMQ suite when implementing or validating a driver.
The Makefile owns the available commands; `make test-fast` is the narrow local
feedback loop and `make test` runs the full Go suite.
