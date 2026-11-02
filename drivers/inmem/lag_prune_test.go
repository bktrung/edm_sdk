package inmem

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

// TestLagAndBacklog_AfterStopAndPruneReturnClassifiedError covers a stopped
// consumer whose destination was pruned. The consumer still names "orders", so
// both reads must report the missing destination as a classified error rather
// than dereferencing it.
func TestLagAndBacklog_AfterStopAndPruneReturnClassifiedError(t *testing.T) {
	stopAndPrune := func(t *testing.T) (context.Context, driver.Conn, driver.Producer, driver.Consumer) {
		t.Helper()
		ctx, conn, producer := openTest(t, clock.NewFake(time.Unix(0, 0)), driver.DestinationSpec{Name: "orders"})
		consumer, err := conn.Consumer(ctx, driver.ConsumerConfig{Destinations: []string{"orders"}, Effective: testCaps()})
		require.NoError(t, err)
		require.NoError(t, consumer.Stop(ctx))
		maintenance, ok := conn.Admin().(driver.Maintenance)
		require.True(t, ok)
		results, err := maintenance.Prune(ctx, []string{"orders"})
		require.NoError(t, err)
		require.Len(t, results, 1)
		require.True(t, results[0].Deleted, "Prune must delete the stopped consumer's destination: %s", results[0].Reason)
		return ctx, conn, producer, consumer
	}
	requireMissingDestination := func(t *testing.T, err error, op string) {
		t.Helper()
		require.ErrorIs(t, err, driver.ErrDestinationMissing)
		var driverErr *driver.Error
		require.ErrorAs(t, err, &driverErr)
		require.Equal(t, driver.KindNotFound, driverErr.Kind())
		require.Equal(t, op, driverErr.Op)
	}

	t.Run("lag", func(t *testing.T) {
		ctx, conn, producer, consumer := stopAndPrune(t)
		var err error
		require.NotPanics(t, func() { _, err = consumer.Lag(ctx) })
		requireMissingDestination(t, err, "lag")
		closeTest(t, ctx, conn, producer)
	})

	t.Run("backlog", func(t *testing.T) {
		ctx, conn, producer, consumer := stopAndPrune(t)
		reader := consumer.(driver.BacklogReader)
		var err error
		require.NotPanics(t, func() { _, err = reader.Backlog(ctx) })
		requireMissingDestination(t, err, "backlog")
		closeTest(t, ctx, conn, producer)
	})
}
