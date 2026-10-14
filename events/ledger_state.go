package events

const MaxConsumerAttempts = 5

type inboxStatus string

const (
	inboxPending     inboxStatus = "pending"
	inboxApplied     inboxStatus = "applied"
	inboxQuarantined inboxStatus = "quarantined"
	inboxResolved    inboxStatus = "resolved"
)

type claimAction string

const (
	claimApply      claimAction = "apply"
	claimBusy       claimAction = "busy"
	claimQuarantine claimAction = "quarantine"
	claimAck        claimAction = "ack"
)

type claimDecision struct {
	action  claimAction
	attempt int
}

func nextClaim(status inboxStatus, attempts int, leaseActive bool) claimDecision {
	switch status {
	case inboxApplied, inboxQuarantined, inboxResolved:
		return claimDecision{action: claimAck, attempt: attempts}
	case inboxPending:
		if leaseActive {
			return claimDecision{action: claimBusy, attempt: attempts}
		}
		if attempts >= MaxConsumerAttempts {
			return claimDecision{action: claimQuarantine, attempt: attempts}
		}
		return claimDecision{action: claimApply, attempt: attempts + 1}
	default:
		return claimDecision{action: claimQuarantine, attempt: attempts}
	}
}

type failureAction string

const (
	failureRetry      failureAction = "retry"
	failureQuarantine failureAction = "quarantine"
)

func failedAttemptAction(attempt int) failureAction {
	if attempt >= MaxConsumerAttempts {
		return failureQuarantine
	}
	return failureRetry
}
