package events

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v5"
	accessv1 "github.com/leafrun/leafrun-contracts/access/v1"
	auditv1 "github.com/leafrun/leafrun-contracts/audit/v1"
	"regexp"
	"strings"
	"time"
)

const AuditEventType = "audit.action"

var auditIDPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

// ValidAuditID recognizes the new server-generated UUIDv4 fact/operation IDs.
// Syntax supplements producer provenance; it cannot prove arbitrary text safe.
func ValidAuditID(id string) bool { return auditIDPattern.MatchString(id) }

// AuditActionInput accepts facts selected by the owning service, never caller
// supplied credentials, raw idempotency keys, parameters or error messages.
type AuditActionInput struct {
	EventID    string
	Scope      accessv1.PermissionScope
	OccurredAt time.Time
	Action     auditv1.Action
}

func NewAuditAction(in AuditActionInput) (Envelope, string, error) {
	subject, err := auditv1.Subject(in.Action.Producer)
	if err != nil {
		return Envelope{}, "", errors.New("invalid audit envelope")
	}
	data, err := auditv1.EncodeAction(in.Action)
	if err != nil {
		return Envelope{}, "", errors.New("invalid audit envelope")
	}
	e := Envelope{SchemaVersion: 2, EventID: in.EventID, EventType: AuditEventType, Producer: string(in.Action.Producer), AggregateID: in.Action.Resource.ID, AggregateVersion: 1, OrganizationID: in.Scope.OrganizationID, WorkspaceID: in.Scope.WorkspaceID, ProjectID: in.Scope.ProjectID, OccurredAt: in.OccurredAt.UTC(), CorrelationID: in.Action.OperationID, Data: data}
	if _, err = ValidateAuditAction(subject, e); err != nil {
		return Envelope{}, "", err
	}
	return e, subject, nil
}

// ValidateAuditAction binds every envelope field used by persistence to the
// closed action. It returns fixed errors even for secret-bearing invalid input.
func ValidateAuditAction(subject string, e Envelope) (auditv1.Action, error) {
	invalid := errors.New("invalid audit envelope")
	if e.Validate() != nil || e.SchemaVersion != 2 || e.EventType != AuditEventType || e.AggregateVersion != 1 || e.CausationID != "" || !ValidAuditID(e.EventID) {
		return auditv1.Action{}, invalid
	}
	a, err := auditv1.DecodeAction(subject, e.Data)
	if err != nil || string(a.Producer) != e.Producer || a.Resource.ID != e.AggregateID || a.OperationID != e.CorrelationID || !ValidAuditID(a.OperationID) {
		return auditv1.Action{}, invalid
	}
	if a.Producer != auditv1.ProducerAccess && (e.OrganizationID == "" || e.WorkspaceID == "" || e.ProjectID == "") {
		return auditv1.Action{}, invalid
	}
	return a, nil
}

// AuditPublisher prevents a malformed outbox or broker diagnostic from entering
// audit retry metadata. It must wrap the durable publisher used by audit relays.
type AuditPublisher struct{ Publisher Publisher }

func (p AuditPublisher) Publish(ctx context.Context, subject string, raw []byte, eventID string) error {
	e, _, err := DecodeAuditAction(subject, raw)
	if err != nil || e.EventID != eventID || p.Publisher == nil {
		return errors.New("audit publication unavailable")
	}
	if p.Publisher.Publish(ctx, subject, raw, eventID) != nil {
		return errors.New("audit publication unavailable")
	}
	return nil
}

// OwnerAuditPublisher routes a mixed owner outbox through the strict Audit
// boundary while preserving its explicitly enumerated existing event subjects.
// Every failure is fixed text before the generic relay persists diagnostics.
type OwnerAuditPublisher struct {
	publisher Publisher
	producer  auditv1.Producer
	subjects  map[string]bool
}

func NewOwnerAuditPublisher(publisher Publisher, producer auditv1.Producer, subjects []string) (*OwnerAuditPublisher, error) {
	if publisher == nil {
		return nil, errors.New("invalid owner publisher")
	}
	if _, err := auditv1.Subject(producer); err != nil {
		return nil, errors.New("invalid owner publisher")
	}
	allowed := map[string]bool{}
	for _, subject := range subjects {
		if validateSubject(subject) != nil || !strings.HasPrefix(subject, "leafrun."+string(producer)+".") || allowed[subject] {
			return nil, errors.New("invalid owner publisher")
		}
		allowed[subject] = true
	}
	return &OwnerAuditPublisher{publisher, producer, allowed}, nil
}
func (p *OwnerAuditPublisher) Publish(ctx context.Context, subject string, raw []byte, eventID string) error {
	unavailable := errors.New("owner publication unavailable")
	if p == nil || p.publisher == nil {
		return unavailable
	}
	auditSubject, _ := auditv1.Subject(p.producer)
	if subject == auditSubject {
		if (AuditPublisher{Publisher: p.publisher}).Publish(ctx, subject, raw, eventID) != nil {
			return unavailable
		}
		return nil
	}
	if !p.subjects[subject] {
		return unavailable
	}
	e, err := Decode(raw)
	if err != nil || e.EventID != eventID {
		return unavailable
	}
	if p.publisher.Publish(ctx, subject, raw, eventID) != nil {
		return unavailable
	}
	return nil
}

// DecodeAuditAction must run before Consumer.Process or any raw inbox write.
// Re-encoding removes wire formatting; rejected wire never becomes replay data.
func DecodeAuditAction(subject string, raw []byte) (Envelope, auditv1.Action, error) {
	if len(raw) > auditv1.MaxActionBytes+8192 {
		return Envelope{}, auditv1.Action{}, errors.New("invalid audit envelope")
	}
	e, err := Decode(raw)
	if err != nil {
		return Envelope{}, auditv1.Action{}, errors.New("invalid audit envelope")
	}
	a, err := ValidateAuditAction(subject, e)
	if err != nil {
		return Envelope{}, auditv1.Action{}, err
	}
	e.Data, _ = auditv1.EncodeAction(a)
	e.OccurredAt = e.OccurredAt.UTC()
	return e, a, nil
}
func EnqueueAuditAction(ctx context.Context, tx pgx.Tx, in AuditActionInput) error {
	e, subject, err := NewAuditAction(in)
	if err != nil {
		return err
	}
	return Enqueue(ctx, tx, e, subject)
}
