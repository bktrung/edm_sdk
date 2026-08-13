package rabbitmq

import (
	"context"
	"errors"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

type admin struct{ conn *conn }

var _ driver.Admin = (*admin)(nil)

func (a *admin) EnsureTopology(ctx context.Context, spec driver.TopologySpec) (driver.TopologyDiff, error) {
	if err := ctx.Err(); err != nil {
		return driver.TopologyDiff{}, classify("ensure_topology", driver.KindTransient, err)
	}
	return a.ensureTopology(ctx, spec)
}

func (a *admin) DescribeTopology(ctx context.Context, _ []string) (driver.TopologyState, error) {
	if err := ctx.Err(); err != nil {
		return driver.TopologyState{}, classify("describe_topology", driver.KindTransient, err)
	}
	return driver.TopologyState{}, classify("describe_topology", driver.KindFatal, driver.ErrUnsupported)
}

func (a *admin) Purge(ctx context.Context, destination string) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, classify("purge", driver.KindTransient, err)
	}
	channel, err := a.openChannel(ctx)
	if err != nil {
		return 0, err
	}
	defer channel.Close()
	count, err := channel.QueuePurge(destination, false)
	if err != nil {
		if isNotFound(err) {
			return 0, classify("purge", driver.KindNotFound, errors.Join(driver.ErrDestinationMissing, err))
		}
		return 0, classifyAMQP("purge", driver.KindTransient, err)
	}
	a.conn.mu.RLock()
	_, hasParking := a.conn.deferred[destination]
	a.conn.mu.RUnlock()
	if !hasParking {
		return int64(count), nil
	}
	parked, err := channel.QueuePurge(destination+".park", false)
	if err != nil {
		return int64(count), classifyAMQP("purge", driver.KindTransient, err)
	}
	return int64(count + parked), nil
}

func (a *admin) Prune(ctx context.Context, _ []string) ([]driver.PruneResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, classify("prune", driver.KindTransient, err)
	}
	return nil, classify("prune", driver.KindFatal, driver.ErrUnsupported)
}
