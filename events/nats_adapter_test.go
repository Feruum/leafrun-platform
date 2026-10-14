package events

import (
	"context"
	"errors"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type jetStreamStub struct {
	message *nats.Msg
	err     error
	calls   int
}

func (s *jetStreamStub) PublishMsg(_ context.Context, message *nats.Msg, _ ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	s.calls++
	s.message = message
	if s.err != nil {
		return nil, s.err
	}
	return &jetstream.PubAck{}, nil
}

func TestJetStreamPublisherSetsStableEventIDHeaderAndWaitsForAck(t *testing.T) {
	stub := &jetStreamStub{}
	publisher := JetStreamPublisher{Client: stub}
	ctx := context.Background()
	payload := []byte(`{"eventId":"evt-1"}`)
	if err := publisher.Publish(ctx, "leafrun.test.event.v1", payload, "evt-1"); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	if stub.calls != 1 || stub.message == nil {
		t.Fatalf("PublishMsg calls = %d", stub.calls)
	}
	if stub.message.Header.Get(jetstream.MsgIDHeader) != "evt-1" {
		t.Fatalf("message dedup header = %q", stub.message.Header.Get(jetstream.MsgIDHeader))
	}
	if stub.message.Subject != "leafrun.test.event.v1" || string(stub.message.Data) != string(payload) {
		t.Fatalf("published message = %#v", stub.message)
	}
}

func TestJetStreamPublisherReturnsDurablePublishErrors(t *testing.T) {
	want := errors.New("stream unavailable")
	stub := &jetStreamStub{err: want}
	err := (JetStreamPublisher{Client: stub}).Publish(context.Background(), "leafrun.test.event.v1", []byte(`{}`), "evt-1")
	if !errors.Is(err, want) {
		t.Fatalf("Publish() error = %v, want %v", err, want)
	}
}
