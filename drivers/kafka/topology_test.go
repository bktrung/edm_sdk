package kafka

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/kversion"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/internal/clock"
)

func TestEnsureTopologyCleanupPolicyWaitsForExistingTopicVisibility(t *testing.T) {
	fakeClock := clock.NewFake(time.Unix(0, 0))
	var visible atomic.Bool
	calls := 0
	installTopicWaitTestState(t, fakeClock, 0, func(_ *admin, _ context.Context, _ string, names ...string) (kadm.TopicDetails, error) {
		calls++
		if calls == 1 {
			fakeClock.Advance(kafkaTopologyVisibilityPoll)
			return kadm.TopicDetails{}, nil
		}
		visible.Store(true)
		return visibleTopicDetails(names...), nil
	})
	a := topologyPolicyWireAdmin(t, []string{"shared-dlq"}, func(req *kmsg.DescribeConfigsRequest) []byte {
		resource := topologyPolicyResource("shared-dlq", new("delete"))
		if !visible.Load() {
			resource.ErrorCode = kerr.UnknownTopicOrPartition.Code
		}
		return topologyPolicyReply(req, []kmsg.DescribeConfigsResponseResource{resource})
	})
	diff, err := a.EnsureTopology(context.Background(), driver.TopologySpec{
		Destinations: []driver.DestinationSpec{{Name: "shared-dlq", Kind: driver.DestDLQ}},
	})
	if err != nil {
		t.Fatalf("concurrently created delete-only DLQ refused before becoming visible: %v", err)
	}
	if !slices.Equal(diff.ExistingDestinations, []string{"shared-dlq"}) {
		t.Fatalf("ExistingDestinations = %v, want admitted shared DLQ", diff.ExistingDestinations)
	}
}

func TestEnsureTopologyCleanupPolicyBoundaryErrors(t *testing.T) {
	for _, policy := range []driver.TopologyPolicy{driver.TopologyDeclare, driver.TopologyVerify} {
		for _, tc := range []struct {
			name      string
			resources []kmsg.DescribeConfigsResponseResource
			kind      driver.Kind
			cause     error
		}{
			{name: "missing_resource", kind: driver.KindNotFound},
			{name: "missing_policy", resources: []kmsg.DescribeConfigsResponseResource{{ResourceType: kmsg.ConfigResourceTypeTopic, ResourceName: "retry"}}, kind: driver.KindFatal, cause: errProtocolResponse},
			{name: "nil_policy", resources: []kmsg.DescribeConfigsResponseResource{topologyPolicyResource("retry", nil)}, kind: driver.KindFatal, cause: errProtocolResponse},
			{name: "empty_policy", resources: []kmsg.DescribeConfigsResponseResource{topologyPolicyResource("retry", new(""))}, kind: driver.KindFatal, cause: errProtocolResponse},
			{name: "leader_unavailable", resources: []kmsg.DescribeConfigsResponseResource{{ResourceType: kmsg.ConfigResourceTypeTopic, ResourceName: "retry", ErrorCode: kerr.LeaderNotAvailable.Code}}, kind: driver.KindTransient, cause: kerr.LeaderNotAvailable},
			{name: "permission_denied", resources: []kmsg.DescribeConfigsResponseResource{{ResourceType: kmsg.ConfigResourceTypeTopic, ResourceName: "retry", ErrorCode: kerr.TopicAuthorizationFailed.Code}}, kind: driver.KindPermission, cause: kerr.TopicAuthorizationFailed},
		} {
			t.Run(topologyPolicyName(policy)+"/"+tc.name, func(t *testing.T) {
				var calls atomic.Int32
				a := topologyPolicyWireAdmin(t, []string{"retry"}, func(req *kmsg.DescribeConfigsRequest) []byte {
					calls.Add(1)
					return topologyPolicyReply(req, tc.resources)
				})
				_, err := a.EnsureTopology(context.Background(), driver.TopologySpec{Policy: policy, Destinations: []driver.DestinationSpec{{Name: "retry", Kind: driver.DestRetry}}})
				topologyPolicyAssertError(t, err, tc.kind, tc.cause)
				if calls.Load() != 1 {
					t.Fatalf("DescribeConfigs calls = %d, want 1", calls.Load())
				}
			})
		}
	}
}

func TestEnsureTopologyCleanupPolicyTokens(t *testing.T) {
	for _, policy := range []driver.TopologyPolicy{driver.TopologyDeclare, driver.TopologyVerify} {
		for _, kind := range []driver.DestKind{driver.DestRetry, driver.DestDLQ} {
			for _, tc := range []struct {
				value   string
				compact bool
			}{
				{value: "compact", compact: true},
				{value: "delete,compact", compact: true},
				{value: " \tdelete , \tcompact\t ", compact: true},
				{value: "delete"},
				{value: " compacted , delete "},
			} {
				role := "retry"
				if kind == driver.DestDLQ {
					role = "dlq"
				}
				t.Run(topologyPolicyName(policy)+"/"+role+"/"+tc.value, func(t *testing.T) {
					a := topologyPolicyWireAdmin(t, []string{role}, func(req *kmsg.DescribeConfigsRequest) []byte {
						return topologyPolicyReply(req, []kmsg.DescribeConfigsResponseResource{topologyPolicyResource(role, &tc.value)})
					})
					_, err := a.EnsureTopology(context.Background(), driver.TopologySpec{Policy: policy, Destinations: []driver.DestinationSpec{{Name: role, Kind: kind}}})
					if !tc.compact {
						if err != nil {
							t.Fatalf("non-compact cleanup.policy %q refused: %v", tc.value, err)
						}
						return
					}
					topologyPolicyAssertError(t, err, driver.KindFatal, nil)
					if !strings.Contains(err.Error(), role) || !strings.Contains(err.Error(), "cleanup.policy="+fmt.Sprintf("%q", tc.value)) {
						t.Fatalf("policy refusal lacks topic and observed policy: %v", err)
					}
				})
			}
		}
	}
}

func TestEnsureTopologyCleanupPolicyBatchMatchesNames(t *testing.T) {
	for _, policy := range []driver.TopologyPolicy{driver.TopologyDeclare, driver.TopologyVerify} {
		for _, name := range []string{"reordered", "reordered_compact", "missing_second_resource"} {
			t.Run(topologyPolicyName(policy)+"/"+name, func(t *testing.T) {
				var calls atomic.Int32
				a := topologyPolicyWireAdmin(t, []string{"retry", "dlq", "main", "backstop"}, func(req *kmsg.DescribeConfigsRequest) []byte {
					calls.Add(1)
					var names []string
					for _, resource := range req.Resources {
						names = append(names, resource.ResourceName)
					}
					slices.Sort(names)
					if !slices.Equal(names, []string{"dlq", "retry"}) {
						t.Errorf("DescribeConfigs names = %v, want only dlq and retry in one batch", names)
					}
					resources := []kmsg.DescribeConfigsResponseResource{topologyPolicyResource("retry", new("delete"))}
					if name != "missing_second_resource" {
						value := "delete"
						if name == "reordered_compact" {
							value = "compact"
						}
						resources = append([]kmsg.DescribeConfigsResponseResource{topologyPolicyResource("dlq", &value)}, resources...)
					}
					return topologyPolicyReply(req, resources)
				})
				_, err := a.EnsureTopology(context.Background(), driver.TopologySpec{Policy: policy, Destinations: []driver.DestinationSpec{
					{Name: "retry", Kind: driver.DestRetry},
					{Name: "dlq", Kind: driver.DestDLQ},
					{Name: "main", Kind: driver.DestMain},
					{Name: "backstop", Kind: driver.DestBackstopDLQ},
				}})
				if name == "missing_second_resource" {
					topologyPolicyAssertError(t, err, driver.KindNotFound, nil)
				} else if name == "reordered_compact" {
					topologyPolicyAssertError(t, err, driver.KindFatal, nil)
					if !strings.Contains(err.Error(), `destination "dlq"`) {
						t.Fatalf("reordered reply attributed compact policy to wrong topic: %v", err)
					}
				} else if err != nil {
					t.Fatalf("reordered complete reply refused: %v", err)
				}
				if calls.Load() != 1 {
					t.Fatalf("DescribeConfigs calls = %d, want 1", calls.Load())
				}
			})
		}
	}
}

func TestEnsureTopologyCleanupPolicyIgnoredRolesAndNone(t *testing.T) {
	for _, policy := range []driver.TopologyPolicy{driver.TopologyDeclare, driver.TopologyVerify, driver.TopologyNone} {
		t.Run(topologyPolicyName(policy), func(t *testing.T) {
			var calls atomic.Int32
			a := topologyPolicyWireAdmin(t, []string{"main", "backstop", "retry"}, func(req *kmsg.DescribeConfigsRequest) []byte {
				calls.Add(1)
				return topologyPolicyReply(req, nil)
			})
			destinations := []driver.DestinationSpec{{Name: "main", Kind: driver.DestMain}, {Name: "backstop", Kind: driver.DestBackstopDLQ}}
			if policy == driver.TopologyNone {
				destinations = append(destinations, driver.DestinationSpec{Name: "retry", Kind: driver.DestRetry})
			}
			if _, err := a.EnsureTopology(context.Background(), driver.TopologySpec{Policy: policy, Destinations: destinations}); err != nil {
				t.Fatalf("ignored roles/policy refused: %v", err)
			}
			if calls.Load() != 0 {
				t.Fatalf("DescribeConfigs calls = %d, want none", calls.Load())
			}
		})
	}
}

func TestEnsureTopologyCleanupPolicyRequestFailures(t *testing.T) {
	for _, policy := range []driver.TopologyPolicy{driver.TopologyDeclare, driver.TopologyVerify} {
		for _, cancelRequest := range []bool{false, true} {
			name := "malformed_reply"
			if cancelRequest {
				name = "cancel_inflight_describe"
			}
			t.Run(topologyPolicyName(policy)+"/"+name, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				var calls atomic.Int32
				a := topologyPolicyWireAdmin(t, []string{"retry"}, func(_ *kmsg.DescribeConfigsRequest) []byte {
					calls.Add(1)
					if cancelRequest {
						cancel()
						return nil
					}
					return []byte{0} // A framed response with a truncated DescribeConfigs body.
				})
				_, err := a.EnsureTopology(ctx, driver.TopologySpec{Policy: policy, Destinations: []driver.DestinationSpec{{Name: "retry", Kind: driver.DestRetry}}})
				var cause error
				if cancelRequest {
					cause = context.Canceled
				}
				topologyPolicyAssertError(t, err, driver.KindTransient, cause)
				if calls.Load() != 1 {
					t.Fatalf("DescribeConfigs calls = %d, want 1", calls.Load())
				}
			})
		}
	}
}

func topologyPolicyAssertError(t *testing.T, err error, kind driver.Kind, cause error) {
	t.Helper()
	var classified *driver.Error
	if !errors.As(err, &classified) || classified.Driver != "kafka" || classified.Op != "ensure_topology" || classified.K != kind {
		t.Fatalf("EnsureTopology error = %v, want *driver.Error kafka/ensure_topology/%v", err, kind)
	}
	if cause != nil && !errors.Is(err, cause) {
		t.Fatalf("EnsureTopology error = %v, want preserved cause %v", err, cause)
	}
}

func topologyPolicyName(policy driver.TopologyPolicy) string {
	switch policy {
	case driver.TopologyDeclare:
		return "declare"
	case driver.TopologyVerify:
		return "verify"
	default:
		return "none"
	}
}

func topologyPolicyResource(name string, value *string) kmsg.DescribeConfigsResponseResource {
	return kmsg.DescribeConfigsResponseResource{
		ResourceType: kmsg.ConfigResourceTypeTopic, ResourceName: name,
		Configs: []kmsg.DescribeConfigsResponseResourceConfig{{Name: "cleanup.policy", Value: value}},
	}
}

func topologyPolicyReply(req *kmsg.DescribeConfigsRequest, resources []kmsg.DescribeConfigsResponseResource) []byte {
	resp := kmsg.NewPtrDescribeConfigsResponse()
	resp.Version = req.Version
	resp.Resources = resources
	return resp.AppendTo(nil)
}

// topologyPolicyWireAdmin is an isolated three-request wire fixture, not a Kafka
// emulator. Metadata advertises existing one-partition topics; CreateTopics
// returns TopicAlreadyExists. Only DescribeConfigs varies. net.Pipe prevents any
// shared broker access, and version pins avoid ApiVersions and flexible headers.
func topologyPolicyWireAdmin(t *testing.T, topics []string, describe func(*kmsg.DescribeConfigsRequest) []byte) *admin {
	t.Helper()
	versions := kversion.FromString("2.0.0")
	versions.SetMaxKeyVersion(18, -1)
	versions.SetMaxKeyVersion(3, 4)
	versions.SetMaxKeyVersion(19, 0)
	versions.SetMaxKeyVersion(32, 0)
	var mu sync.Mutex
	var pipes []net.Conn
	var closed bool
	var workers sync.WaitGroup
	client, err := kgo.NewClient(kgo.SeedBrokers("policy-wire:9092"), kgo.MaxVersions(versions), kgo.RequestRetries(0),
		kgo.Dialer(func(_ context.Context, _, _ string) (net.Conn, error) {
			mu.Lock()
			defer mu.Unlock()
			if closed {
				return nil, net.ErrClosed
			}
			front, back := net.Pipe()
			pipes = append(pipes, front, back)
			workers.Go(func() {
				defer back.Close()
				topologyPolicyServe(t, back, topics, describe)
			})
			return front, nil
		}))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() {
		mu.Lock()
		closed = true
		for _, pipe := range pipes {
			_ = pipe.Close()
		}
		mu.Unlock()
		client.Close()
		workers.Wait()
	})
	return &admin{client: kadm.NewClient(client)}
}

func topologyPolicyServe(t *testing.T, pipe net.Conn, topics []string, describe func(*kmsg.DescribeConfigsRequest) []byte) {
	for {
		var size [4]byte
		if _, err := io.ReadFull(pipe, size[:]); err != nil {
			return
		}
		n := binary.BigEndian.Uint32(size[:])
		if n < 10 || n > 1<<20 {
			t.Errorf("invalid policy fixture request size %d", n)
			return
		}
		frame := make([]byte, n)
		if _, err := io.ReadFull(pipe, frame); err != nil {
			return
		}
		key := binary.BigEndian.Uint16(frame)
		wireVersion := binary.BigEndian.Uint16(frame[2:])
		if wireVersion > 4 {
			t.Errorf("unexpected policy fixture API version %d", wireVersion)
			return
		}
		version := int16(wireVersion)
		clientIDLen := binary.BigEndian.Uint16(frame[8:])
		offset := 10
		if clientIDLen != 0xffff {
			offset += int(clientIDLen)
		}
		if offset > len(frame) {
			t.Error("truncated policy fixture request header")
			return
		}
		var request kmsg.Request
		switch key {
		case 3:
			request = kmsg.NewPtrMetadataRequest()
		case 19:
			request = kmsg.NewPtrCreateTopicsRequest()
		case 32:
			request = kmsg.NewPtrDescribeConfigsRequest()
		default:
			t.Errorf("unexpected policy fixture API key %d", key)
			return
		}
		request.SetVersion(version)
		if err := request.ReadFrom(frame[offset:]); err != nil {
			t.Errorf("reading policy fixture request: %v", err)
			return
		}
		var body []byte
		switch req := request.(type) {
		case *kmsg.MetadataRequest:
			resp := kmsg.NewPtrMetadataResponse()
			resp.Version = version
			resp.ControllerID = 1
			resp.Brokers = []kmsg.MetadataResponseBroker{{NodeID: 1, Host: "policy-wire", Port: 9092}}
			for _, name := range topics {
				requested := len(req.Topics) == 0
				for _, topic := range req.Topics {
					if topic.Topic != nil && *topic.Topic == name {
						requested = true
						break
					}
				}
				if !requested {
					continue
				}
				resp.Topics = append(resp.Topics, kmsg.MetadataResponseTopic{Topic: &name, Partitions: []kmsg.MetadataResponseTopicPartition{{Partition: 0, Leader: 1, Replicas: []int32{1}, ISR: []int32{1}}}})
			}
			body = resp.AppendTo(nil)
		case *kmsg.CreateTopicsRequest:
			resp := kmsg.NewPtrCreateTopicsResponse()
			resp.Version = version
			for _, topic := range req.Topics {
				resp.Topics = append(resp.Topics, kmsg.CreateTopicsResponseTopic{Topic: topic.Topic, ErrorCode: kerr.TopicAlreadyExists.Code})
			}
			body = resp.AppendTo(nil)
		case *kmsg.DescribeConfigsRequest:
			body = describe(req)
		}
		if body == nil {
			continue // Cancellation test deliberately leaves the request unanswered.
		}
		responseSize := 4 + len(body)
		if responseSize < 4 || responseSize > 1<<20 {
			t.Errorf("invalid policy fixture response size %d", responseSize)
			return
		}
		response := make([]byte, 8, 4+responseSize)
		binary.BigEndian.PutUint32(response, uint32(responseSize))
		copy(response[4:], frame[4:8])
		response = append(response, body...)
		if _, err := pipe.Write(response); err != nil {
			return
		}
	}
}
