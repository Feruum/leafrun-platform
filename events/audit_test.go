package events

import (
	"encoding/json"
	accessv1 "github.com/leafrun/leafrun-contracts/access/v1"
	auditv1 "github.com/leafrun/leafrun-contracts/audit/v1"
	identityv1 "github.com/leafrun/leafrun-contracts/identity/v1"
	"testing"
	"time"
)

func TestAuditCanonicalIngress(t *testing.T) {
	in := AuditActionInput{EventID: "11111111-1111-4111-8111-111111111111", Scope: accessv1.PermissionScope{OrganizationID: "org", WorkspaceID: "ws", ProjectID: "project"}, OccurredAt: time.Now().UTC(), Action: auditv1.Action{SchemaVersion: 1, Producer: auditv1.ProducerProjects, Actor: identityv1.Principal{Domain: "human", Subject: "actor"}, Authority: identityv1.AuthorityRef{Kind: identityv1.KindKratosSession, ID: "authority", Version: 1}, OperationID: "22222222-2222-4222-8222-222222222222", Action: auditv1.ActionReleasePublish, Resource: auditv1.ResourceRef{Kind: auditv1.ResourceRelease, ID: "release"}, Outcome: auditv1.OutcomeSucceeded}}
	e, subject, err := NewAuditAction(in)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := Encode(e)
	secretID := e
	secretID.EventID = "TOKEN_SENTINEL"
	badID, _ := Encode(secretID)
	if _, _, err := DecodeAuditAction(subject, badID); err == nil {
		t.Fatal("arbitrary event ID accepted")
	}
	if _, _, err = DecodeAuditAction(subject, raw); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*Envelope){func(e *Envelope) { e.CorrelationID = "forged" }, func(e *Envelope) { e.AggregateID = "forged" }, func(e *Envelope) { e.Producer = "access" }, func(e *Envelope) { e.OrganizationID = "" }, func(e *Envelope) { e.CausationID = "secret" }, func(e *Envelope) { e.SchemaVersion = 1 }} {
		bad := e
		mutate(&bad)
		raw, _ := json.Marshal(bad)
		if _, _, err := DecodeAuditAction(subject, raw); err == nil {
			t.Fatal("accepted invalid binding")
		}
	}
	if _, _, err := DecodeAuditAction("leafrun.audit.access.action.v1", raw); err == nil {
		t.Fatal("accepted subject mismatch")
	}
	if _, _, err := DecodeAuditAction(subject, []byte(`{"SENTINEL":true}`)); err == nil || err.Error() != "invalid audit envelope" {
		t.Fatal("unsafe wire error")
	}
}
