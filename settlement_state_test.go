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

// TestAckDeliveryMarksAttemptAfterCallReturns verifies failed ack calls leave an attempted unknown state.
func TestAckDeliveryMarksAttemptAfterCallReturns(t *testing.T) {
	state := &deliveryState{}
	called := false
	settler := &observingSettler{
		ackErr: errors.New("ack unavailable"),
		ack: func() {
			called = true
			if state.attempted {
				t.Fatal("attempted set before Ack returned")
			}
		},
	}

	if ackDelivery(nil, context.Background(), driver.InboundMessage{Settle: settler}, state) {
		t.Fatal("ackDelivery returned success for a failed Ack")
	}
	if !called {
		t.Fatal("Ack was not called")
	}
	if !state.attempted {
		t.Fatal("attempted was not set after Ack returned")
	}
	if state.settled {
		t.Fatal("failed Ack was marked settled")
	}
	forgetSettlementOperation(state)
}

// TestNackDeliveryMarksAttemptAfterCallReturns verifies failed nack calls leave an attempted unknown state.
func TestNackDeliveryMarksAttemptAfterCallReturns(t *testing.T) {
	state := &deliveryState{}
	called := false
	settler := &observingSettler{
		nackErr: errors.New("nack unavailable"),
		nack: func() {
			called = true
			if state.attempted {
				t.Fatal("attempted set before Nack returned")
			}
		},
	}

	if err := nackDelivery(nil, context.Background(), driver.InboundMessage{Settle: settler}, driver.NackOptions{Requeue: true}, state); err == nil {
		t.Fatal("nackDelivery returned success for a failed Nack")
	}
	if !called {
		t.Fatal("Nack was not called")
	}
	if !state.attempted {
		t.Fatal("attempted was not set after Nack returned")
	}
	if state.settled {
		t.Fatal("failed Nack was marked settled")
	}
	forgetSettlementOperation(state)
}
