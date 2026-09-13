// Package rabbitmq provides a RabbitMQ driver for the SDK driver port.
//
// The driver reads AMQP for transport and the broker's management HTTP API for
// topology inspection. The management API is a deployment requirement of the
// subscription path, not an optional extra: a subscription under the SDK's
// TopologyDeclare or TopologyVerify policy reads its bindings, and under
// TopologyVerify also the broker's queue arguments, through that API before
// the consumer opens, and fails at that point when the API is unreachable.
// AMQP's passive declare confirms that a queue exists but cannot report the
// arguments it holds, so there is no AMQP-only fallback that verifies the same
// thing.
//
// The management endpoint defaults to the AMQP host with the AMQP port plus
// 10000 and reuses the AMQP credentials. Set the "rabbitmq.managementPort"
// driver option when the management plugin listens elsewhere, and make sure
// the management API is enabled and reachable from the host running the SDK.
//
// TopologyNone is the only policy under which a subscription starts without
// the management API: the driver makes no management call, so the topology
// must already exist.
package rabbitmq
