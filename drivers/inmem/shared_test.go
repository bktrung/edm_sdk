package inmem

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

func TestSharedDriverRefcountsConnectionsAndKeepsDefaultIsolated(t *testing.T) {
	ctx := context.Background()
	shared := NewShared()
	first, err := shared.Open(ctx, driver.Config{})
	require.NoError(t, err)
	second, err := shared.Open(ctx, driver.Config{})
	require.NoError(t, err)
	_, err = first.Admin().EnsureTopology(ctx, driver.TopologySpec{
		Destinations: []driver.DestinationSpec{{Name: "shared.destination"}},
	})
	require.NoError(t, err)

	require.NoError(t, first.Close(ctx))
	_, err = second.Admin().DescribeTopology(ctx, []string{"shared.destination"})
	require.NoError(t, err)
	require.NoError(t, second.Close(ctx))

	isolatedFirst, err := (Driver{}).Open(ctx, driver.Config{})
	require.NoError(t, err)
	isolatedSecond, err := (Driver{}).Open(ctx, driver.Config{})
	require.NoError(t, err)
	_, err = isolatedFirst.Admin().EnsureTopology(ctx, driver.TopologySpec{
		Destinations: []driver.DestinationSpec{{Name: "isolated.destination"}},
	})
	require.NoError(t, err)
	_, err = isolatedSecond.Admin().DescribeTopology(ctx, []string{"isolated.destination"})
	require.ErrorIs(t, err, driver.ErrDestinationMissing)
	require.NoError(t, isolatedFirst.Close(ctx))
	require.NoError(t, isolatedSecond.Close(ctx))
}
