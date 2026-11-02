package rabbitmq

import (
	"fmt"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

// TestAMQPErrorCodeClassifiers pins which AMQP codes each classifier matches,
// both bare and wrapped, so a change to how the classifiers unwrap an error
// cannot move a code into or out of a retry or failure decision unnoticed.
func TestAMQPErrorCodeClassifiers(t *testing.T) {
	// closeBlame only blames a message on a missing-exchange close, so every
	// row carries that reason. The other classifiers ignore it.
	const reason = "NOT_FOUND - no exchange 'events' in vhost '/'"
	tests := []struct {
		name            string
		code            int
		wrapped         bool
		precondition    bool
		notFound        bool
		permission      bool
		missingExchange bool
	}{
		{name: "403 bare", code: 403, permission: true},
		{name: "403 wrapped", code: 403, wrapped: true, permission: true},
		{name: "404 bare", code: 404, notFound: true, missingExchange: true},
		{name: "404 wrapped", code: 404, wrapped: true, notFound: true, missingExchange: true},
		{name: "406 bare", code: 406, precondition: true},
		{name: "406 wrapped", code: 406, wrapped: true, precondition: true},
		{name: "500 bare", code: 500},
		{name: "500 wrapped", code: 500, wrapped: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var err error = &amqp.Error{Code: tt.code, Reason: reason}
			if tt.wrapped {
				err = fmt.Errorf("publish: %w", err)
			}
			if got := isPreconditionFailed(err); got != tt.precondition {
				t.Errorf("isPreconditionFailed = %t, want %t", got, tt.precondition)
			}
			if got := isNotFound(err); got != tt.notFound {
				t.Errorf("isNotFound = %t, want %t", got, tt.notFound)
			}
			if got := isPermission(err); got != tt.permission {
				t.Errorf("isPermission = %t, want %t", got, tt.permission)
			}
			if _, got := closeBlame(err); got != tt.missingExchange {
				t.Errorf("closeBlame named = %t, want %t", got, tt.missingExchange)
			}
		})
	}
}
