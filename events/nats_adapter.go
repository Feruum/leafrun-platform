package events

import (
	"context"
	"errors"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// JetStreamClient is the synchronous JetStream publish method used by the adapter.
type JetStreamClient interface {
	PublishMsg(context.Context, *nats.Msg, ...jetstream.PublishOpt) (*jetstream.PubAck, error)
}

// JetStreamPublisher adapts JetStream's durable publish ACK to Publisher.
type JetStreamPublisher struct {
	Client JetStreamClient
}

// Publish sets Nats-Msg-Id to eventID and returns only after JetStream responds.
// The PostgreSQL outbox remains the source of retry and idempotency guarantees.
func (p JetStreamPublisher) Publish(ctx context.Context, subject string, payload []byte, eventID string) error {
	if p.Client == nil {
		return errors.New("JetStream publisher client is required")
	}
	if err := validateSubject(subject); err != nil {
		return err
	}
	if err := validateIdentifier("eventId", eventID, true); err != nil {
		return err
	}
	message := &nats.Msg{Subject: subject, Data: payload, Header: make(nats.Header)}
	message.Header.Set(jetstream.MsgIDHeader, eventID)
	ack, err := p.Client.PublishMsg(ctx, message, jetstream.WithMsgID(eventID))
	if err != nil {
		return err
	}
	if ack == nil {
		return errors.New("JetStream publish returned without a durable acknowledgement")
	}
	return nil
}

// NATSDelivery adapts a JetStream message to the consumer delivery interface.
type NATSDelivery struct {
	Message *nats.Msg
}

func (d NATSDelivery) Payload() []byte {
	if d.Message == nil {
		return nil
	}
	return d.Message.Data
}

func (d NATSDelivery) Ack() error {
	if d.Message == nil {
		return errors.New("NATS message is required")
	}
	return d.Message.Ack()
}

func (d NATSDelivery) NakWithDelay(delay time.Duration) error {
	if d.Message == nil {
		return errors.New("NATS message is required")
	}
	return d.Message.NakWithDelay(delay)
}
