package events

import (
	"context"
	"errors"
	accessv1 "github.com/leafrun/leafrun-contracts/access/v1"
	auditv1 "github.com/leafrun/leafrun-contracts/audit/v1"
	identityv1 "github.com/leafrun/leafrun-contracts/identity/v1"
	"testing"
	"time"
)

type leakingPublisher struct{ calls *int }

func (p leakingPublisher) Publish(context.Context, string, []byte, string) error {
	if p.calls != nil {
		*p.calls++
	}
	return errors.New("TOKEN_SENTINEL")
}
func TestAuditPublisherSuppressesProviderErrors(t *testing.T) {
	calls := 0
	p := AuditPublisher{Publisher: leakingPublisher{calls: &calls}}
	if err := p.Publish(context.Background(), "foreign", []byte("SECRET"), "event"); err == nil || err.Error() != "audit publication unavailable" {
		t.Fatal("unsafe publisher boundary")
	}
	e, subject, err := NewAuditAction(AuditActionInput{EventID: "11111111-1111-4111-8111-111111111111", Scope: accessv1.PermissionScope{OrganizationID: "org", WorkspaceID: "ws", ProjectID: "project"}, OccurredAt: time.Now().UTC(), Action: auditv1.Action{SchemaVersion: 1, Producer: auditv1.ProducerStorage, Actor: identityv1.Principal{Domain: "human", Subject: "actor"}, Authority: identityv1.AuthorityRef{Kind: identityv1.KindKratosSession, ID: "authority", Version: 1}, OperationID: "22222222-2222-4222-8222-222222222222", Action: auditv1.ActionDownloadAuthorized, Resource: auditv1.ResourceRef{Kind: auditv1.ResourceArtifact, ID: "artifact"}, Outcome: auditv1.OutcomeSucceeded}})
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := Encode(e)
	if err := p.Publish(context.Background(), subject, raw, e.EventID); err == nil || err.Error() != "audit publication unavailable" || calls != 1 {
		t.Fatal("raw provider diagnostic escaped")
	}
}

func TestOwnerAuditPublisherRoutesOnlyExactOwnerSubjects(t *testing.T) {
	p, e := NewOwnerAuditPublisher(leakingPublisher{}, "generation", []string{GenerationCompletedSubject})
	if e != nil {
		t.Fatal(e)
	}
	if err := p.Publish(context.Background(), "leafrun.audit.storage.action.v1", nil, "event"); err == nil || err.Error() != "owner publication unavailable" {
		t.Fatal("foreign audit subject accepted")
	}
	if _, err := NewOwnerAuditPublisher(leakingPublisher{}, "generation", []string{"leafrun.projects.release.ready.v1"}); err == nil {
		t.Fatal("foreign normal subject accepted")
	}
	if _, err := NewOwnerAuditPublisher(leakingPublisher{}, "generation", []string{"leafrun.generation.>"}); err == nil {
		t.Fatal("wildcard normal subject accepted")
	}
	normal := validEnvelope()
	raw, _ := Encode(normal)
	if err := p.Publish(context.Background(), GenerationCompletedSubject, raw, normal.EventID); err == nil || err.Error() != "owner publication unavailable" {
		t.Fatal("normal provider diagnostic escaped")
	}
}
