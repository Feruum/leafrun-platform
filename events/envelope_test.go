package events

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

func validEnvelope() Envelope {
	return Envelope{
		SchemaVersion:    1,
		EventID:          "evt_01JABCDEF0123456789ABCDEFG",
		EventType:        "generation.completed",
		Producer:         "leafrun-generation",
		AggregateID:      "job_123",
		AggregateVersion: 2,
		WorkspaceID:      "workspace_123",
		ProjectID:        "project_456",
		OccurredAt:       time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC),
		CorrelationID:    "request_123",
		CausationID:      "evt_parent",
		Data:             json.RawMessage(`{"jobId":"job_123"}`),
	}
}

func TestEnvelopeSupportsAdmittedScopeIdentifierBounds(t *testing.T) {
	e := validEnvelope()
	e.WorkspaceID = strings.Repeat("w", 256)
	e.ProjectID = strings.Repeat("p", 256)
	if _, err := Encode(e); err != nil {
		t.Fatal("Generation can admit these scope IDs", err)
	}
	e.ProjectID += "p"
	if _, err := Encode(e); err == nil {
		t.Fatal("unbounded identifier")
	}
}

func TestEncodeNeverEmitsEnvelopeRejectedByDecode(t *testing.T) {
	for _, count := range []int{170000, 175000, MaxDataBytes / 2} {
		e := validEnvelope()
		e.Data = json.RawMessage(`{"x":"` + strings.Repeat("<", count) + `"}`)
		encoded, err := Encode(e)
		if err != nil {
			continue
		}
		if _, err = Decode(encoded); err != nil {
			t.Fatalf("encoder emitted undecodable envelope size=%d: %v", len(encoded), err)
		}
	}
}

func TestBoundedFailureTextIsDatabaseSafe(t *testing.T) {
	for _, text := range []string{"a€tail", "bad\x00value", "invalid\xffutf8"} {
		got := boundedText(text, 3)
		if len(got) > 3 || !utf8.ValidString(got) || strings.ContainsRune(got, 0) {
			t.Fatalf("invalid failure text: %q", got)
		}
	}
}

func TestIdentifiersRejectInvalidDatabaseText(t *testing.T) {
	for _, id := range []string{"bad\x00id", "bad\xffid"} {
		e := validEnvelope()
		e.EventID = id
		if _, err := Encode(e); err == nil {
			t.Errorf("accepted identifier %q", id)
		}
	}
}

func TestEnvelopeRoundTripsWithStableJSONNames(t *testing.T) {
	want := validEnvelope()
	encoded, err := Encode(want)
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &fields); err != nil {
		t.Fatalf("encoded envelope is invalid JSON: %v", err)
	}
	for _, name := range []string{"schemaVersion", "eventId", "eventType", "producer", "aggregateId", "aggregateVersion", "workspaceId", "projectId", "occurredAt", "correlationId", "causationId", "data"} {
		if _, ok := fields[name]; !ok {
			t.Errorf("encoded envelope is missing %q", name)
		}
	}
	got, err := Decode(encoded)
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if got.EventID != want.EventID || got.EventType != want.EventType || got.AggregateVersion != want.AggregateVersion || string(got.Data) != string(want.Data) {
		t.Fatalf("round-trip envelope mismatch: got %#v, want %#v", got, want)
	}
}

func TestValidateRejectsUnboundedOrIncompleteEnvelope(t *testing.T) {
	tests := []struct {
		name string
		edit func(*Envelope)
	}{
		{name: "missing event id", edit: func(e *Envelope) { e.EventID = " " }},
		{name: "unsupported schema", edit: func(e *Envelope) { e.SchemaVersion = 2 }},
		{name: "zero aggregate version", edit: func(e *Envelope) { e.AggregateVersion = 0 }},
		{name: "invalid event type", edit: func(e *Envelope) { e.EventType = "Generation Completed" }},
		{name: "oversized data", edit: func(e *Envelope) { e.Data = json.RawMessage(`"` + strings.Repeat("x", MaxDataBytes) + `"`) }},
		{name: "malformed data", edit: func(e *Envelope) { e.Data = json.RawMessage(`{"jobId":`) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := validEnvelope()
			tt.edit(&e)
			if err := e.Validate(); err == nil {
				t.Fatal("Validate() succeeded for invalid envelope")
			}
		})
	}
}

func TestGenerationCompletedUsesStableSubjectAndOnlyCallerJobID(t *testing.T) {
	e, subject, err := NewGenerationCompleted(GenerationCompletedInput{
		EventID: "evt_01JABCDEF0123456789ABCDEFG", Producer: "leafrun-generation",
		JobID: "job_123", WorkspaceID: "workspace_123", ProjectID: "project_456",
		AggregateVersion: 2, OccurredAt: time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC),
		CorrelationID: "request_123", CausationID: "evt_parent",
	})
	if err != nil {
		t.Fatalf("NewGenerationCompleted() error = %v", err)
	}
	if subject != GenerationCompletedSubject || e.EventType != "generation.completed" {
		t.Fatalf("generation event route mismatch: subject=%q type=%q", subject, e.EventType)
	}
	var data map[string]json.RawMessage
	if err := json.Unmarshal(e.Data, &data); err != nil {
		t.Fatalf("invalid generation data: %v", err)
	}
	if len(data) != 1 || string(data["jobId"]) != `"job_123"` {
		t.Fatalf("generation payload must contain only the caller's job id, got %s", e.Data)
	}
	var typed GenerationCompletedData
	if err := json.Unmarshal(e.Data, &typed); err != nil || typed.JobID != "job_123" {
		t.Fatalf("generation payload DTO = %#v, error = %v", typed, err)
	}
}

type repairStub struct {
	called int
	target uint64
	result uint64
}

func (r *repairStub) Resynchronize(_ context.Context, _ string, _ string, _ string, target uint64) (uint64, error) {
	r.called++
	r.target = target
	return r.result, nil
}

func TestCheckAggregateVersionIgnoresOldAndRepairsGaps(t *testing.T) {
	e := validEnvelope()
	e.AggregateVersion = 3
	decision, err := CheckAggregateVersion(context.Background(), e, 3, nil)
	if err != nil || decision.Action != IgnoreEvent {
		t.Fatalf("old event decision = %#v, error = %v", decision, err)
	}

	repair := &repairStub{result: 2}
	decision, err = CheckAggregateVersion(context.Background(), e, 1, repair)
	if err != nil || decision.Action != ApplyEvent || repair.target != 2 || repair.called != 1 {
		t.Fatalf("gap decision = %#v, repair = %#v, error = %v", decision, repair, err)
	}
}

func TestCheckAggregateVersionRejectsUnrepairedGap(t *testing.T) {
	e := validEnvelope()
	e.AggregateVersion = 4
	if _, err := CheckAggregateVersion(context.Background(), e, 1, nil); err == nil {
		t.Fatal("gap without owner repair succeeded")
	}
}
