package f1

import (
	"context"
	"errors"
	"testing"

	"fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/driver"
)

type observingSettler struct {
	ackErr  error
	nackErr error
	ack     func()
	nack    func()
}

func (s *observingSettler) Ack(context.Context) error {
	if s.ack != nil {
		s.ack()
	}
	return s.ackErr
}

func (s *observingSettler) Nack(context.Context, driver.NackOptions) error {
	if s.nack != nil {
		s.nack()
	}
	return s.nackErr
}

var _ driver.Settler = (*observingSettler)(nil)

// TestSettlementMarksAttemptAfterCallReturns verifies a failed Ack or Nack
// leaves an attempted, unsettled state that records which call to repeat.
func TestSettlementMarksAttemptAfterCallReturns(t *testing.T) {
	for _, test := range []struct {
		name      string
		operation settlementOperation
		settle    func(driver.InboundMessage, *deliveryState) bool
	}{
		{
			name:      "Ack",
			operation: settlementOperationAck,
			settle: func(message driver.InboundMessage, state *deliveryState) bool {
				return ackDelivery(nil, context.Background(), message, state)
			},
		},
		{
			name:      "Nack",
			operation: settlementOperationNack,
			settle: func(message driver.InboundMessage, state *deliveryState) bool {
				return nackDelivery(nil, context.Background(), message, driver.NackOptions{Requeue: true}, state) == nil
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			state := &deliveryState{}
			called := false
			observe := func() {
				called = true
				if state.attempted {
					t.Fatalf("attempted set before %s returned", test.name)
				}
			}
			settler := &observingSettler{
				ackErr:  errors.New("ack unavailable"),
				nackErr: errors.New("nack unavailable"),
				ack:     observe,
				nack:    observe,
			}

			if test.settle(driver.InboundMessage{Settle: settler}, state) {
				t.Fatalf("%s returned success for a failed call", test.name)
			}
			if !called {
				t.Fatalf("%s was not called", test.name)
			}
			if !state.attempted {
				t.Fatalf("attempted was not set after %s returned", test.name)
			}
			if state.settled {
				t.Fatalf("failed %s was marked settled", test.name)
			}
			if state.operation != test.operation {
				t.Fatalf("failed %s did not record itself, so the retry cannot know what to repeat", test.name)
			}
		})
	}
}
