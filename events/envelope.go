// Package events provides transport-neutral event envelopes and PostgreSQL-backed
// outbox and consumer bookkeeping. Domain records remain in each service's database.
package events

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	// MaxDataBytes caps an individual domain payload at one mebibyte.
	MaxDataBytes = 1 << 20
	// MaxIdentifierBytes bounds event, producer, and aggregate identifiers.
	MaxIdentifierBytes = 256
	// MaxAggregateVersion is the largest version supported by PostgreSQL bigint.
	MaxAggregateVersion uint64 = 1<<63 - 1
	maxEnvelopeBytes           = MaxDataBytes + 8*MaxIdentifierBytes + 4096
)

var eventTypePattern = regexp.MustCompile(`^[a-z][a-z0-9]*(\.[a-z][a-z0-9]*)*$`)

// Envelope is the stable transport envelope used by platform event packages.
// Data is owned by the producer's domain and must not contain shared database records.
type Envelope struct {
	SchemaVersion    int             `json:"schemaVersion"`
	EventID          string          `json:"eventId"`
	EventType        string          `json:"eventType"`
	Producer         string          `json:"producer"`
	AggregateID      string          `json:"aggregateId"`
	AggregateVersion uint64          `json:"aggregateVersion"`
	WorkspaceID      string          `json:"workspaceId"`
	ProjectID        string          `json:"projectId"`
	OccurredAt       time.Time       `json:"occurredAt"`
	CorrelationID    string          `json:"correlationId"`
	CausationID      string          `json:"causationId"`
	Data             json.RawMessage `json:"data"`
}

// Validate checks the current envelope version and the bounds required for safe
// storage, logging, and broker publication.
func (e Envelope) Validate() error {
	if e.SchemaVersion != 1 {
		return fmt.Errorf("schemaVersion must be 1, got %d", e.SchemaVersion)
	}
	for name, value := range map[string]string{
		"eventId": e.EventID, "producer": e.Producer, "aggregateId": e.AggregateID,
		"workspaceId": e.WorkspaceID, "projectId": e.ProjectID,
	} {
		if err := validateIdentifier(name, value, true); err != nil {
			return err
		}
	}
	for name, value := range map[string]string{"correlationId": e.CorrelationID, "causationId": e.CausationID} {
		if err := validateIdentifier(name, value, false); err != nil {
			return err
		}
	}
	if len(e.EventType) > MaxIdentifierBytes || !eventTypePattern.MatchString(e.EventType) {
		return fmt.Errorf("eventType must be a bounded dotted lowercase identifier")
	}
	if e.AggregateVersion == 0 || e.AggregateVersion > MaxAggregateVersion {
		return fmt.Errorf("aggregateVersion must be between 1 and %d", MaxAggregateVersion)
	}
	if e.OccurredAt.IsZero() {
		return errors.New("occurredAt is required")
	}
	if len(e.Data) == 0 || len(e.Data) > MaxDataBytes || !json.Valid(e.Data) || !utf8.Valid(e.Data) {
		return fmt.Errorf("data must be valid JSON between 1 and %d bytes", MaxDataBytes)
	}
	return nil
}

func validateIdentifier(name, value string, required bool) error {
	if !utf8.ValidString(value) || strings.ContainsRune(value, 0) {
		return fmt.Errorf("%s must be valid UTF-8 without NUL", name)
	}
	if strings.TrimSpace(value) == "" {
		if required {
			return fmt.Errorf("%s is required", name)
		}
		return nil
	}
	if len(value) > MaxIdentifierBytes || strings.TrimSpace(value) != value {
		return fmt.Errorf("%s must be at most %d bytes and have no surrounding whitespace", name, MaxIdentifierBytes)
	}
	return nil
}

// Encode validates and encodes an envelope as compact JSON.
func Encode(e Envelope) ([]byte, error) {
	if err := e.Validate(); err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(e)
	if err != nil {
		return nil, fmt.Errorf("encode event envelope: %w", err)
	}
	if len(encoded) > maxEnvelopeBytes {
		return nil, errors.New("encoded envelope exceeds transport limit")
	}
	// RawMessage HTML escaping may expand the payload. Check the exact wire
	// representation that Decode will retain before admitting it to an outbox.
	var wire struct {
		Data json.RawMessage `json:"data"`
	}
	if err = json.Unmarshal(encoded, &wire); err != nil {
		return nil, err
	}
	if len(wire.Data) > MaxDataBytes {
		return nil, errors.New("encoded data exceeds transport limit")
	}
	return encoded, nil
}

// Decode parses one bounded JSON envelope, rejects unknown top-level fields, and validates it.
func Decode(encoded []byte) (Envelope, error) {
	var e Envelope
	if len(encoded) == 0 || len(encoded) > maxEnvelopeBytes {
		return e, fmt.Errorf("encoded event envelope must be between 1 and %d bytes", maxEnvelopeBytes)
	}
	if !utf8.Valid(encoded) {
		return e, errors.New("encoded event envelope contains invalid UTF-8")
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&e); err != nil {
		return e, fmt.Errorf("decode event envelope: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return e, errors.New("decode event envelope: trailing JSON value")
		}
		return e, fmt.Errorf("decode event envelope: %w", err)
	}
	if err := e.Validate(); err != nil {
		return e, err
	}
	return e, nil
}

const (
	// GenerationCompletedSubject is the durable v1 JetStream subject for completed jobs.
	GenerationCompletedSubject   = "leafrun.generation.job.completed.v1"
	GenerationCompletedEventType = "generation.completed"
)

// GenerationCompletedInput contains IDs already owned by the generation service.
// No package-level lookup or shared business database is required.
type GenerationCompletedInput struct {
	EventID          string
	Producer         string
	JobID            string
	WorkspaceID      string
	ProjectID        string
	AggregateVersion uint64
	OccurredAt       time.Time
	CorrelationID    string
	CausationID      string
}

// GenerationCompletedData is the owner-supplied ID payload for the generation event.
type GenerationCompletedData struct {
	JobID string `json:"jobId"`
}

// NewGenerationCompleted creates the v1 completed event and its stable broker subject.
func NewGenerationCompleted(input GenerationCompletedInput) (Envelope, string, error) {
	data, err := json.Marshal(GenerationCompletedData{JobID: input.JobID})
	if err != nil {
		return Envelope{}, "", fmt.Errorf("encode generation completion data: %w", err)
	}
	e := Envelope{
		SchemaVersion: 1, EventID: input.EventID, EventType: GenerationCompletedEventType,
		Producer: input.Producer, AggregateID: input.JobID, AggregateVersion: input.AggregateVersion,
		WorkspaceID: input.WorkspaceID, ProjectID: input.ProjectID, OccurredAt: input.OccurredAt,
		CorrelationID: input.CorrelationID, CausationID: input.CausationID, Data: data,
	}
	if err := e.Validate(); err != nil {
		return Envelope{}, "", err
	}
	return e, GenerationCompletedSubject, nil
}

// VersionAction describes whether an event can be applied against current aggregate state.
type VersionAction string

const (
	IgnoreEvent VersionAction = "ignore_old"
	ApplyEvent  VersionAction = "apply_next"
)

// VersionDecision is the result of checking an event's aggregate sequence.
type VersionDecision struct {
	Action         VersionAction
	CurrentVersion uint64
}

// VersionRepairer asks the owning service API to resynchronize its state up to targetVersion.
// It does not grant this package access to the owner's database.
type VersionRepairer interface {
	Resynchronize(ctx context.Context, workspaceID, projectID, aggregateID string, targetVersion uint64) (currentVersion uint64, err error)
}

// CheckAggregateVersion ignores stale events, applies the next version, or asks the
// owning service to repair a gap. The event handler must still compare the version
// inside its effect transaction to prevent a concurrent update between this check and apply.
func CheckAggregateVersion(ctx context.Context, e Envelope, current uint64, repairer VersionRepairer) (VersionDecision, error) {
	if e.AggregateVersion == 0 || e.AggregateVersion > MaxAggregateVersion || current > MaxAggregateVersion {
		return VersionDecision{}, errors.New("aggregate version is outside the supported range")
	}
	if e.AggregateVersion <= current {
		return VersionDecision{Action: IgnoreEvent, CurrentVersion: current}, nil
	}
	if current < MaxAggregateVersion && e.AggregateVersion == current+1 {
		return VersionDecision{Action: ApplyEvent, CurrentVersion: current}, nil
	}
	if repairer == nil {
		return VersionDecision{}, fmt.Errorf("aggregate version gap: current=%d event=%d", current, e.AggregateVersion)
	}
	refreshed, err := repairer.Resynchronize(ctx, e.WorkspaceID, e.ProjectID, e.AggregateID, e.AggregateVersion-1)
	if err != nil {
		return VersionDecision{}, fmt.Errorf("resynchronize aggregate %q: %w", e.AggregateID, err)
	}
	if refreshed > MaxAggregateVersion {
		return VersionDecision{}, errors.New("owner returned an unsupported aggregate version")
	}
	if refreshed >= e.AggregateVersion {
		return VersionDecision{Action: IgnoreEvent, CurrentVersion: refreshed}, nil
	}
	if refreshed+1 == e.AggregateVersion {
		return VersionDecision{Action: ApplyEvent, CurrentVersion: refreshed}, nil
	}
	return VersionDecision{}, fmt.Errorf("aggregate gap remains after repair: current=%d event=%d", refreshed, e.AggregateVersion)
}
