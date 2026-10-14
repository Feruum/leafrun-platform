package events

import "testing"

func TestNextClaimNeverStartsApplicationSix(t *testing.T) {
	decision := nextClaim(inboxPending, MaxConsumerAttempts-1, false)
	if decision.action != claimApply || decision.attempt != MaxConsumerAttempts {
		t.Fatalf("fifth claim = %#v", decision)
	}
	decision = nextClaim(inboxPending, MaxConsumerAttempts, false)
	if decision.action != claimQuarantine || decision.attempt != MaxConsumerAttempts {
		t.Fatalf("expired fifth claim must quarantine without a sixth application: %#v", decision)
	}
}

func TestNextClaimDefersConcurrentLeaseAndAcknowledgesTerminalRows(t *testing.T) {
	decision := nextClaim(inboxPending, 2, true)
	if decision.action != claimBusy || decision.attempt != 2 {
		t.Fatalf("active lease must defer without consuming an attempt: %#v", decision)
	}
	if got := nextClaim(inboxApplied, 1, false).action; got != claimAck {
		t.Fatalf("applied duplicate action = %q, want ack", got)
	}
	if got := nextClaim(inboxQuarantined, MaxConsumerAttempts, false).action; got != claimAck {
		t.Fatalf("quarantined duplicate action = %q, want ack", got)
	}
}

func TestFailureAtFifthAttemptQuarantines(t *testing.T) {
	if got := failedAttemptAction(MaxConsumerAttempts); got != failureQuarantine {
		t.Fatalf("fifth failed attempt action = %q, want quarantine", got)
	}
	if got := failedAttemptAction(MaxConsumerAttempts - 1); got != failureRetry {
		t.Fatalf("fourth failed attempt action = %q, want retry", got)
	}
}
