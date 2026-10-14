package events

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

var (
	ErrEventResolved  = errors.New("event is already applied or resolved")
	ErrActiveLease    = errors.New("event has an active application lease")
	ErrNotQuarantined = errors.New("event is not quarantined")
	ErrEventNotFound  = errors.New("event was not found")
)

const (
	DispositionApplied     = "applied"
	DispositionRetry       = "retry"
	DispositionQuarantined = "quarantined"
	DispositionResolved    = "resolved"
	DispositionBusy        = "busy"
)

type Handler func(context.Context, pgx.Tx, Envelope) error
type Delivery interface {
	Payload() []byte
	Ack() error
	NakWithDelay(time.Duration) error
}
type ConsumerDatabase interface {
	Begin(context.Context) (pgx.Tx, error)
	QueryRow(context.Context, string, ...any) pgx.Row
	Query(context.Context, string, ...any) (pgx.Rows, error)
}

type Consumer struct {
	DB            ConsumerDatabase
	Name          string
	Handler       Handler
	LeaseDuration time.Duration
	RetryDelay    time.Duration
}

type ownedAttempt struct {
	token   string
	attempt int
}

func (c *Consumer) validate() error {
	if c.DB == nil {
		return errors.New("consumer database is required")
	}
	if err := validateIdentifier("consumerName", c.Name, true); err != nil {
		return err
	}
	return nil
}

func (c *Consumer) leaseDuration() time.Duration {
	if c.LeaseDuration > 0 {
		return c.LeaseDuration
	}
	return 30 * time.Second
}

// Process applies an event without a broker delivery. It is useful for tests and
// operator tooling; a broker adapter should call Consume so ACK ordering is enforced.
func (c *Consumer) Process(ctx context.Context, e Envelope) (string, error) {
	if err := c.validate(); err != nil {
		return "", err
	}
	if c.Handler == nil {
		return "", errors.New("consumer handler is required")
	}
	encoded, err := Encode(e)
	if err != nil {
		return "", err
	}
	owned, disposition, err := c.claim(ctx, e, encoded)
	if err != nil {
		return "", err
	}
	if disposition != "" {
		return disposition, nil
	}
	return c.apply(ctx, e, *owned)
}

// Consume persists a durable application attempt before running the handler,
// commits the handler and inbox result together, and acknowledges only afterward.
func (c *Consumer) Consume(ctx context.Context, d Delivery) error {
	if d == nil {
		return errors.New("delivery is required")
	}
	e, err := Decode(d.Payload())
	if err != nil {
		return err
	}
	disposition, err := c.Process(ctx, e)
	if err != nil {
		return err
	}
	switch disposition {
	case DispositionApplied, DispositionQuarantined, DispositionResolved:
		return d.Ack()
	case DispositionRetry:
		delay := c.RetryDelay
		if delay <= 0 {
			delay = time.Second
		}
		return d.NakWithDelay(delay)
	case DispositionBusy:
		delay := c.RetryDelay
		if delay <= 0 {
			delay = time.Second
		}
		return d.NakWithDelay(delay)
	default:
		return fmt.Errorf("unknown event disposition %q", disposition)
	}
}

func (c *Consumer) claim(ctx context.Context, e Envelope, encoded []byte) (*ownedAttempt, string, error) {
	tx, err := c.DB.Begin(ctx)
	if err != nil {
		return nil, "", fmt.Errorf("begin inbox claim: %w", err)
	}
	defer tx.Rollback(ctx)
	hash := sha256.Sum256(encoded)
	if _, err := tx.Exec(ctx, `INSERT INTO platform_event_inbox(consumer_name,event_id,encoded_envelope,payload_hash)
		VALUES($1,$2,$3,$4) ON CONFLICT(consumer_name,event_id) DO NOTHING`, c.Name, e.EventID, encoded, hash[:]); err != nil {
		return nil, "", fmt.Errorf("insert inbox row: %w", err)
	}
	var status string
	var attempts int
	var storedHash []byte
	var active bool
	err = tx.QueryRow(ctx, `SELECT status,attempts,payload_hash,COALESCE(lease_until>clock_timestamp(),false)
		FROM platform_event_inbox WHERE consumer_name=$1 AND event_id=$2 FOR UPDATE`, c.Name, e.EventID).Scan(&status, &attempts, &storedHash, &active)
	if err != nil {
		return nil, "", fmt.Errorf("lock inbox row: %w", err)
	}
	if !equalBytes(storedHash, hash[:]) {
		return nil, "", ErrEventIDConflict
	}
	switch inboxStatus(status) {
	case inboxApplied:
		return nil, DispositionApplied, tx.Commit(ctx)
	case inboxQuarantined:
		return nil, DispositionQuarantined, tx.Commit(ctx)
	case inboxResolved:
		return nil, DispositionResolved, tx.Commit(ctx)
	case inboxPending:
	default:
		return nil, "", fmt.Errorf("unknown inbox status %q", status)
	}
	decision := nextClaim(inboxPending, attempts, active)
	switch decision.action {
	case claimBusy:
		if err := tx.Commit(ctx); err != nil {
			return nil, "", fmt.Errorf("commit busy inbox observation: %w", err)
		}
		return nil, DispositionBusy, nil
	case claimQuarantine:
		if err := c.quarantine(ctx, tx, e.EventID, attempts, "application lease expired after final attempt"); err != nil {
			return nil, "", err
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, "", fmt.Errorf("commit exhausted inbox quarantine: %w", err)
		}
		return nil, DispositionQuarantined, nil
	case claimApply:
	default:
		return nil, "", errors.New("invalid inbox claim decision")
	}
	token, err := newLeaseToken()
	if err != nil {
		return nil, "", fmt.Errorf("create inbox lease token: %w", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE platform_event_inbox SET attempts=$3,lease_token=$4,
		lease_until=clock_timestamp()+$5*interval '1 microsecond',updated_at=clock_timestamp()
		WHERE consumer_name=$1 AND event_id=$2`, c.Name, e.EventID, decision.attempt, token, c.leaseDuration().Microseconds()); err != nil {
		return nil, "", fmt.Errorf("persist application attempt: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, "", fmt.Errorf("commit application attempt: %w", err)
	}
	return &ownedAttempt{token: token, attempt: decision.attempt}, "", nil
}

func (c *Consumer) apply(ctx context.Context, e Envelope, owned ownedAttempt) (string, error) {
	tx, err := c.DB.Begin(ctx)
	if err != nil {
		return "", fmt.Errorf("begin event effect transaction: %w", err)
	}
	var current bool
	err = tx.QueryRow(ctx, `SELECT COALESCE(lease_token=$3 AND status='pending' AND lease_until>clock_timestamp(),false)
		FROM platform_event_inbox WHERE consumer_name=$1 AND event_id=$2 FOR UPDATE`, c.Name, e.EventID, owned.token).Scan(&current)
	if err != nil {
		_ = tx.Rollback(ctx)
		return "", fmt.Errorf("fence event effect: %w", err)
	}
	if !current {
		_ = tx.Rollback(ctx)
		return "", ErrLeaseLost
	}
	if err = c.Handler(ctx, tx, e); err != nil {
		_ = tx.Rollback(ctx)
		return c.recordFailure(ctx, e.EventID, owned, err)
	}
	tag, err := tx.Exec(ctx, `UPDATE platform_event_inbox SET status='applied',applied_at=clock_timestamp(),
		last_error=NULL,lease_token=NULL,lease_until=NULL,updated_at=clock_timestamp()
		WHERE consumer_name=$1 AND event_id=$2 AND lease_token=$3 AND lease_until>clock_timestamp()`, c.Name, e.EventID, owned.token)
	if err != nil {
		_ = tx.Rollback(ctx)
		return "", fmt.Errorf("mark inbox applied: %w", err)
	}
	if tag.RowsAffected() != 1 {
		_ = tx.Rollback(ctx)
		return "", ErrLeaseLost
	}
	if err := tx.Commit(ctx); err != nil {
		return "", fmt.Errorf("commit event effect: %w", err)
	}
	return DispositionApplied, nil
}

func (c *Consumer) recordFailure(ctx context.Context, eventID string, owned ownedAttempt, handlerErr error) (string, error) {
	tx, err := c.DB.Begin(ctx)
	if err != nil {
		return "", fmt.Errorf("begin handler failure record: %w", err)
	}
	defer tx.Rollback(ctx)
	var status string
	var token string
	if err := tx.QueryRow(ctx, `SELECT status,COALESCE(lease_token,'') FROM platform_event_inbox WHERE consumer_name=$1 AND event_id=$2 FOR UPDATE`, c.Name, eventID).Scan(&status, &token); err != nil {
		return "", fmt.Errorf("lock failed inbox row: %w", err)
	}
	if status != "pending" || token != owned.token {
		return "", ErrLeaseLost
	}
	message := boundedText(handlerErr.Error(), 2048)
	action := failedAttemptAction(owned.attempt)
	if action == failureQuarantine {
		if _, err := tx.Exec(ctx, `UPDATE platform_event_inbox SET status='quarantined',quarantined_at=clock_timestamp(),
			last_error=$3,lease_token=NULL,lease_until=NULL,updated_at=clock_timestamp() WHERE consumer_name=$1 AND event_id=$2`, c.Name, eventID, message); err != nil {
			return "", fmt.Errorf("persist final handler failure: %w", err)
		}
		if err := insertHistory(ctx, tx, c.Name, eventID, "quarantined", owned.attempt, message); err != nil {
			return "", err
		}
		if err := tx.Commit(ctx); err != nil {
			return "", fmt.Errorf("commit event quarantine: %w", err)
		}
		return DispositionQuarantined, nil
	}
	if _, err := tx.Exec(ctx, `UPDATE platform_event_inbox SET last_error=$3,lease_token=NULL,lease_until=NULL,updated_at=clock_timestamp()
		WHERE consumer_name=$1 AND event_id=$2`, c.Name, eventID, message); err != nil {
		return "", fmt.Errorf("persist handler failure: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return "", fmt.Errorf("commit handler failure: %w", err)
	}
	return DispositionRetry, nil
}

func (c *Consumer) quarantine(ctx context.Context, tx pgx.Tx, eventID string, attempt int, details string) error {
	if _, err := tx.Exec(ctx, `UPDATE platform_event_inbox SET status='quarantined',quarantined_at=clock_timestamp(),last_error=$3,
		lease_token=NULL,lease_until=NULL,updated_at=clock_timestamp() WHERE consumer_name=$1 AND event_id=$2`, c.Name, eventID, details); err != nil {
		return fmt.Errorf("quarantine exhausted event: %w", err)
	}
	return insertHistory(ctx, tx, c.Name, eventID, "quarantined", attempt, details)
}

func insertHistory(ctx context.Context, tx pgx.Tx, name, eventID, action string, attempt int, details string) error {
	if _, err := tx.Exec(ctx, `INSERT INTO platform_event_history(consumer_name,event_id,action,attempt,details) VALUES($1,$2,$3,$4,$5)`, name, eventID, action, attempt, details); err != nil {
		return fmt.Errorf("record event history: %w", err)
	}
	return nil
}

type EventStatus struct {
	ConsumerName  string
	EventID       string
	Status        string
	Attempts      int
	ReplayCount   int
	LastError     string
	LeaseUntil    *time.Time
	AppliedAt     *time.Time
	QuarantinedAt *time.Time
	ResolvedAt    *time.Time
}

func (c *Consumer) Status(ctx context.Context, eventID string) (EventStatus, error) {
	if err := c.validate(); err != nil {
		return EventStatus{}, err
	}
	var s EventStatus
	s.ConsumerName = c.Name
	s.EventID = eventID
	err := c.DB.QueryRow(ctx, `SELECT status,attempts,replay_count,COALESCE(last_error,''),lease_until,applied_at,quarantined_at,resolved_at
		FROM platform_event_inbox WHERE consumer_name=$1 AND event_id=$2`, c.Name, eventID).Scan(&s.Status, &s.Attempts, &s.ReplayCount, &s.LastError, &s.LeaseUntil, &s.AppliedAt, &s.QuarantinedAt, &s.ResolvedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return s, ErrEventNotFound
	}
	if err != nil {
		return s, fmt.Errorf("read inbox status: %w", err)
	}
	return s, nil
}

type HistoryEntry struct {
	Action    string
	Attempt   int
	Details   string
	CreatedAt time.Time
}

func (c *Consumer) History(ctx context.Context, eventID string) ([]HistoryEntry, error) {
	if err := c.validate(); err != nil {
		return nil, err
	}
	rows, err := c.DB.Query(ctx, `SELECT action,attempt,details,created_at FROM platform_event_history WHERE consumer_name=$1 AND event_id=$2 ORDER BY id`, c.Name, eventID)
	if err != nil {
		return nil, fmt.Errorf("read event history: %w", err)
	}
	defer rows.Close()
	var out []HistoryEntry
	for rows.Next() {
		var h HistoryEntry
		if err := rows.Scan(&h.Action, &h.Attempt, &h.Details, &h.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, h)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *Consumer) Replay(ctx context.Context, eventID string) error {
	if err := c.validate(); err != nil {
		return err
	}
	tx, err := c.DB.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin event replay: %w", err)
	}
	defer tx.Rollback(ctx)
	var status string
	var active bool
	if err := tx.QueryRow(ctx, `SELECT status,COALESCE(lease_until>clock_timestamp(),false) FROM platform_event_inbox WHERE consumer_name=$1 AND event_id=$2 FOR UPDATE`, c.Name, eventID).Scan(&status, &active); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrEventNotFound
		}
		return fmt.Errorf("lock event for replay: %w", err)
	}
	if status == string(inboxApplied) || status == string(inboxResolved) {
		return ErrEventResolved
	}
	if status != string(inboxQuarantined) {
		if active {
			return ErrActiveLease
		}
		return ErrNotQuarantined
	}
	if active {
		return ErrActiveLease
	}
	if _, err := tx.Exec(ctx, `UPDATE platform_event_inbox SET status='pending',attempts=0,replay_count=replay_count+1,
		last_error=NULL,quarantined_at=NULL,lease_token=NULL,lease_until=NULL,updated_at=clock_timestamp(),
		replay_requested=true,replay_available_at=clock_timestamp()
		WHERE consumer_name=$1 AND event_id=$2`, c.Name, eventID); err != nil {
		return fmt.Errorf("reset quarantined event: %w", err)
	}
	if err := insertHistory(ctx, tx, c.Name, eventID, "replayed", 0, "operator replay"); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit event replay: %w", err)
	}
	return nil
}

func (c *Consumer) Resolve(ctx context.Context, eventID, reason string) error {
	if err := c.validate(); err != nil {
		return err
	}
	reason = strings.TrimSpace(reason)
	if reason == "" {
		return errors.New("resolution reason is required")
	}
	reason = boundedText(reason, 2048)
	tx, err := c.DB.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin event resolution: %w", err)
	}
	defer tx.Rollback(ctx)
	var status string
	var active bool
	if err := tx.QueryRow(ctx, `SELECT status,COALESCE(lease_until>clock_timestamp(),false) FROM platform_event_inbox WHERE consumer_name=$1 AND event_id=$2 FOR UPDATE`, c.Name, eventID).Scan(&status, &active); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrEventNotFound
		}
		return fmt.Errorf("lock event for resolution: %w", err)
	}
	if status == string(inboxApplied) || status == string(inboxResolved) {
		return ErrEventResolved
	}
	if active {
		return ErrActiveLease
	}
	if status != string(inboxQuarantined) {
		return ErrNotQuarantined
	}
	if _, err := tx.Exec(ctx, `UPDATE platform_event_inbox SET status='resolved',resolved_at=clock_timestamp(),resolution_reason=$3,updated_at=clock_timestamp(),replay_requested=false WHERE consumer_name=$1 AND event_id=$2`, c.Name, eventID, reason); err != nil {
		return fmt.Errorf("resolve quarantined event: %w", err)
	}
	if err := insertHistory(ctx, tx, c.Name, eventID, "resolved", 0, reason); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit event resolution: %w", err)
	}
	return nil
}

type PurgeCounts struct {
	Outbox int64
	Inbox  int64
}

// PurgeExpired removes only acknowledged or administratively resolved records
// older than the 30-day retention window. Pending and quarantined rows survive.
func PurgeExpired(ctx context.Context, tx pgx.Tx, now time.Time) (PurgeCounts, error) {
	var counts PurgeCounts
	if tx == nil {
		return counts, errors.New("retention transaction is required")
	}
	cutoff := now.UTC().Add(-30 * 24 * time.Hour)
	tag, err := tx.Exec(ctx, `DELETE FROM platform_event_outbox WHERE sent_at IS NOT NULL AND sent_at<$1`, cutoff)
	if err != nil {
		return counts, fmt.Errorf("purge sent outbox rows: %w", err)
	}
	counts.Outbox = tag.RowsAffected()
	err = tx.QueryRow(ctx, `WITH gone AS (
		DELETE FROM platform_event_inbox WHERE status IN ('applied','resolved') AND NOT replay_requested AND COALESCE(resolved_at,applied_at)<$1
		RETURNING consumer_name,event_id
	), histories AS (
		DELETE FROM platform_event_history h USING gone g WHERE h.consumer_name=g.consumer_name AND h.event_id=g.event_id RETURNING h.id
	) SELECT count(*) FROM gone`, cutoff).Scan(&counts.Inbox)
	if err != nil {
		return counts, fmt.Errorf("purge resolved inbox rows: %w", err)
	}
	return counts, nil
}
