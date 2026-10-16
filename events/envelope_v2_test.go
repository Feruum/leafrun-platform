package events

import (
	"strings"
	"testing"
)

const v1EnvelopeJSON = `{"schemaVersion":1,"eventId":"evt_01JABCDEF0123456789ABCDEFG","eventType":"generation.completed","producer":"leafrun-generation","aggregateId":"job_123","aggregateVersion":2,"workspaceId":"workspace_123","projectId":"project_456","occurredAt":"2026-10-14T12:00:00Z","correlationId":"request_123","causationId":"evt_parent","data":{"jobId":"job_123"}}`

func TestV1ExactEncodedFixture(t *testing.T) {
	b, err := Encode(validEnvelope())
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != v1EnvelopeJSON {
		t.Fatalf("v1 bytes changed: %s", b)
	}
	for _, mutate := range []func(*Envelope){func(e *Envelope) { e.WorkspaceID = "" }, func(e *Envelope) { e.ProjectID = "" }} {
		e := validEnvelope()
		mutate(&e)
		if e.Validate() == nil {
			t.Fatal("v1 scope became optional")
		}
	}
}
func TestV2TenantHierarchy(t *testing.T) {
	for _, tc := range []struct {
		org, workspace, project string
		valid                   bool
	}{{"", "", "", true}, {"o", "", "", true}, {"o", "w", "", true}, {"o", "w", "p", true}, {"", "w", "p", false}, {"", "w", "", false}, {"o", "", "p", false}, {"", "", "p", false}, {" o", "", "", false}, {strings.Repeat("o", 257), "", "", false}} {
		e := validEnvelope()
		e.SchemaVersion = 2
		e.OrganizationID = tc.org
		e.WorkspaceID = tc.workspace
		e.ProjectID = tc.project
		b, err := Encode(e)
		if (err == nil) != tc.valid {
			t.Errorf("%#v: %v", tc, err)
		}
		if err == nil {
			decoded, err := Decode(b)
			if err != nil || decoded.OrganizationID != tc.org {
				t.Fatalf("round trip %v", err)
			}
		}
	}
}
func TestEnvelopeStrictDecode(t *testing.T) {
	b, _ := Encode(validEnvelope())
	for _, bad := range []string{strings.Replace(string(b), `"schemaVersion":1`, `"schemaVersion":1,"schemaVersion":1`, 1), strings.Replace(string(b), `"eventId":`, `"EventID":`, 1), string(b) + ` {}`, strings.Replace(string(b), `"jobId":"job_123"`, `"jobId":"a","jobId":"b"`, 1), strings.Replace(string(b), `"jobId":"job_123"`, `"token":"\ud800"`, 1), strings.Replace(string(b), `"eventId":"evt_01JABCDEF0123456789ABCDEFG"`, `"eventId":"bad\xff"`, 1)} {
		if _, err := Decode([]byte(bad)); err == nil {
			t.Fatalf("accepted invalid envelope %q", bad)
		}
	}
}

func TestV2RejectsSuppliedBlankTenantIDs(t *testing.T) {
	for _, mutate := range []func(*Envelope){func(e *Envelope) { e.OrganizationID = " " }, func(e *Envelope) { e.WorkspaceID = " " }, func(e *Envelope) { e.ProjectID = " " }} {
		e := validEnvelope()
		e.SchemaVersion = 2
		e.OrganizationID = "o"
		mutate(&e)
		if e.Validate() == nil {
			t.Fatal("supplied blank tenant ID accepted")
		}
	}
}

func TestV1RejectsOrganizationFieldPresence(t *testing.T) {
	valid := []byte(v1EnvelopeJSON)
	for _, field := range []string{`"organizationId":"o"`, `"organizationId":""`, `"organizationId":null`, `"organizati\u006fnId":""`, `"OrganizationID":"o"`, `"OrganizationId":null`} {
		t.Run(field, func(t *testing.T) {
			wire := strings.Replace(string(valid), `"schemaVersion":1`, `"schemaVersion":1,`+field, 1)
			if _, err := Decode([]byte(wire)); err == nil {
				t.Fatal("v1 admitted organization field absent from its baseline schema")
			}
		})
	}
	e := validEnvelope()
	e.OrganizationID = "o"
	if err := e.Validate(); err == nil {
		t.Error("v1 Validate accepts organization")
	}
	if _, err := Encode(e); err == nil {
		t.Error("v1 Encode accepts organization")
	}
	if _, err := Decode(valid); err != nil {
		t.Fatalf("legacy v1 fixture rejected: %v", err)
	}
}
