package f1

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

func TestTopologyPolicyDerivesFromOptionAndConfig(t *testing.T) {
	cases := []struct {
		name          string
		autoCreate    bool
		verifyOnStart bool
		option        TopologyPolicy
		supplied      bool
		want          TopologyPolicy
	}{
		{"option wins", true, true, TopologyNone, true, TopologyNone},
		{"auto create", true, true, TopologyDeclare, false, TopologyDeclare},
		{"verify", false, true, TopologyDeclare, false, TopologyVerify},
		{"none", false, false, TopologyDeclare, false, TopologyNone},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			cfg := testClientConfig(t)
			cfg.Topology.AutoCreate = test.autoCreate
			cfg.Topology.VerifyOnStart = test.verifyOnStart
			testDriver := &topologyTestDriver{conn: &topologyTestConn{
				testConn: &testConn{caps: driver.Capabilities{MaxHeaderBytes: CoreMaxHeaderBytes}},
				admin:    &topologyRecordingAdmin{},
			}}
			opts := []Option{WithDriver(testDriver)}
			if test.supplied {
				opts = append(opts, WithTopology(test.option))
			}
			client, err := New(context.Background(), cfg, opts...)
			require.NoError(t, err)
			require.Equal(t, test.want, client.topologyPolicy())
			require.NoError(t, client.Close(context.Background()))
		})
	}
}
