package inmem

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bktrung/edm_sdk/driver"
)

func TestSharedConn_ClosedHandleRejectsNewResources(t *testing.T) {
	ctx := context.Background()
	shared := NewShared()
	first, err := shared.Open(ctx, driver.Config{})
	require.NoError(t, err)
	second, err := shared.Open(ctx, driver.Config{})
	require.NoError(t, err)
	_, err = first.Admin().EnsureTopology(ctx, driver.TopologySpec{
		Destinations: []driver.DestinationSpec{{Name: "orders"}},
	})
	require.NoError(t, err)
	adm := first.Admin()

	require.NoError(t, first.Close(ctx))

	_, err = first.Producer(ctx, driver.ProducerConfig{})
	require.ErrorIs(t, err, driver.ErrResourcesOutstanding)
	var producerErr *driver.Error
	require.ErrorAs(t, err, &producerErr)
	require.Equal(t, driver.KindFatal, producerErr.K)

	_, err = first.Consumer(ctx, driver.ConsumerConfig{Destinations: []string{"orders"}})
	require.ErrorIs(t, err, driver.ErrResourcesOutstanding)
	var consumerErr *driver.Error
	require.ErrorAs(t, err, &consumerErr)
	require.Equal(t, driver.KindFatal, consumerErr.K)

	_, err = adm.EnsureTopology(ctx, driver.TopologySpec{
		Destinations: []driver.DestinationSpec{{Name: "other"}},
	})
	var adminErr *driver.Error
	require.ErrorAs(t, err, &adminErr)
	require.Equal(t, driver.KindTransient, adminErr.K)

	_, err = first.Admin().EnsureTopology(ctx, driver.TopologySpec{
		Destinations: []driver.DestinationSpec{{Name: "other"}},
	})
	require.ErrorAs(t, err, &adminErr)
	require.Equal(t, driver.KindTransient, adminErr.K)

	err = first.Ping(ctx)
	var pingErr *driver.Error
	require.ErrorAs(t, err, &pingErr)
	require.Equal(t, driver.KindTransient, pingErr.K)

	producer, err := second.Producer(ctx, driver.ProducerConfig{})
	require.NoError(t, err)
	require.NoError(t, producer.Close(ctx))
	require.NoError(t, second.Ping(ctx))
	require.Implements(t, (*driver.Maintenance)(nil), second.Admin())
	require.NoError(t, second.Close(ctx))
}

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
