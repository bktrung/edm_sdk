package rabbitmq

import (
	"context"

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

func (a *admin) Purge(ctx context.Context, _ string) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, classify("purge", driver.KindTransient, err)
	}
	return 0, classify("purge", driver.KindFatal, driver.ErrUnsupported)
}

func (a *admin) Prune(ctx context.Context, _ []string) ([]driver.PruneResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, classify("prune", driver.KindTransient, err)
	}
	return nil, classify("prune", driver.KindFatal, driver.ErrUnsupported)
}
