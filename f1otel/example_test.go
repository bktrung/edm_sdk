package f1otel_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	metricdata "go.opentelemetry.io/otel/sdk/metric/metricdata"

	f1 "fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/drivers/inmem"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/f1otel"
)

func ExampleNew() {
	ctx := context.Background()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	observer, err := f1otel.New(f1otel.WithMeterProvider(provider))
	if err != nil {
		panic(err)
	}

	client, err := f1.New(ctx, f1.Config{
		Env:     "example",
		Service: "godoc",
		Broker:  f1.BrokerConfig{Driver: "inmem"},
		Topology: f1.TopologyConfig{
			AutoCreate: true,
		},
	},
		f1.WithDriver(inmem.New()),
		f1.WithPublishTopics("events.created"),
		f1.WithObserver(observer),
		f1.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
	)
	if err != nil {
		panic(err)
	}
	if _, err := client.Publisher().Publish(ctx, "events.created.v1", struct {
		Message string `json:"message"`
	}{Message: "hello"}); err != nil {
		panic(err)
	}

	var data metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &data); err != nil {
		panic(err)
	}
	names := make([]string, 0)
	for _, scope := range data.ScopeMetrics {
		for _, metric := range scope.Metrics {
			names = append(names, metric.Name)
		}
	}
	sort.Strings(names)
	for _, name := range names {
		fmt.Println(name)
	}

	closeCtx, cancelClose := context.WithTimeout(context.Background(), time.Second)
	defer cancelClose()
	if err := client.Close(closeCtx); err != nil {
		panic(err)
	}

	// Output:
	// messaging.client.operation.duration
	// messaging.client.sent.messages
}
