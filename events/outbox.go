package events

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

var (
	ErrEventIDConflict = errors.New("event ID is already bound to different immutable content")
	ErrLeaseLost       = errors.New("event application lease was lost or expired")
	subjectPattern     = regexp.MustCompile(`^[a-zA-Z0-9_-]+(\.[a-zA-Z0-9_-]+)*$`)
)

func validateSubject(subject string) error {
	if len(subject) == 0 || len(subject) > 256 || strings.ContainsAny(subject, "*> ") || !subjectPattern.MatchString(subject) {
		return fmt.Errorf("subject must be a bounded dotted name without wildcards")
	}
	return nil
}

// Enqueue stores an event in the caller's state transaction. Reusing an event ID
// is idempotent only when its subject and exact encoded payload are unchanged.
func Enqueue(ctx context.Context, tx pgx.Tx, e Envelope, subject string) error {
	if tx == nil {
		return errors.New("outbox transaction is required")
	}
	if err := validateSubject(subject); err != nil {
		return err
	}
	encoded, err := Encode(e)
	if err != nil {
		return err
	}
	hash := sha256.Sum256(encoded)
	if _, err := tx.Exec(ctx, `INSERT INTO platform_event_outbox(event_id, subject,encoded_envelope,payload_hash)
		VALUES ($1,$2,$3,$4) ON CONFLICT (event_id) DO NOTHING`, e.EventID, subject, encoded, hash[:]); err != nil {
		return fmt.Errorf("insert event outbox row: %w", err)
	}
	var storedSubject string
	var storedHash []byte
	if err := tx.QueryRow(ctx, `SELECT subject,payload_hash FROM platform_event_outbox WHERE event_id=$1`, e.EventID).Scan(&storedSubject, &storedHash); err != nil {
		return fmt.Errorf("verify event outbox identity: %w", err)
	}
	if storedSubject != subject || !equalBytes(storedHash, hash[:]) {
		return ErrEventIDConflict
	}
	return nil
}

func equalBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	var x byte
	for i := range a {
		x |= a[i] ^ b[i]
	}
	return x == 0
}

type Beginner interface {
	Begin(context.Context) (pgx.Tx, error)
}

type Publisher interface {
	Publish(context.Context, string, []byte, string) error
}

type Relay struct {
	DB            Beginner
	Publisher     Publisher
	LeaseDuration time.Duration
	RetryBase     time.Duration
	Now           func() time.Time
}

type outboxItem struct {
	id, subject, token string
	payload            []byte
	attempts           int
}

func newLeaseToken() (string, error) {
	var token [16]byte
	if _, err := rand.Read(token[:]); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", token[:]), nil
}

func (r *Relay) RunOnce(ctx context.Context) (int, error) {
	if r.DB == nil || r.Publisher == nil {
		return 0, errors.New("relay database and publisher are required")
	}
	tx, err := r.DB.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin outbox claim: %w", err)
	}
	defer tx.Rollback(ctx)
	var item outboxItem
	var leaseActive bool
	err = tx.QueryRow(ctx, `SELECT event_id,subject,encoded_envelope,attempts,COALESCE(lease_until > clock_timestamp(),false)
		FROM platform_event_outbox WHERE sent_at IS NULL AND available_at <= clock_timestamp()
		AND (lease_until IS NULL OR lease_until <= clock_timestamp())
		ORDER BY created_at,event_id FOR UPDATE SKIP LOCKED LIMIT 1`).Scan(&item.id, &item.subject, &item.payload, &item.attempts, &leaseActive)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, tx.Commit(ctx)
	}
	if err != nil {
		return 0, fmt.Errorf("select outbox row: %w", err)
	}
	item.token, err = newLeaseToken()
	if err != nil {
		return 0, err
	}
	lease := r.LeaseDuration
	if lease <= 0 {
		lease = 30 * time.Second
	}
	if _, err := tx.Exec(ctx, `UPDATE platform_event_outbox SET lease_token=$2,lease_until=clock_timestamp()+$3*interval '1 microsecond',attempts=attempts+1
		WHERE event_id=$1`, item.id, item.token, lease.Microseconds()); err != nil {
		return 0, fmt.Errorf("lease outbox row: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit outbox lease: %w", err)
	}
	item.attempts++
	if err := r.Publisher.Publish(ctx, item.subject, item.payload, item.id); err != nil {
		if releaseErr := r.release(ctx, item, err); releaseErr != nil {
			return 0, errors.Join(fmt.Errorf("publish event: %w", err), releaseErr)
		}
		return 0, fmt.Errorf("publish event %q: %w", item.id, err)
	}
	tx, err = r.DB.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin outbox acknowledgement: %w", err)
	}
	defer tx.Rollback(ctx)
	tag, err := tx.Exec(ctx, `UPDATE platform_event_outbox SET sent_at=clock_timestamp(),lease_token=NULL,lease_until=NULL,last_error=NULL
		WHERE event_id=$1 AND lease_token=$2 AND sent_at IS NULL`, item.id, item.token)
	if err != nil {
		return 0, fmt.Errorf("mark event sent: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return 0, ErrLeaseLost
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit outbox acknowledgement: %w", err)
	}
	return 1, nil
}

func (r *Relay) release(ctx context.Context, item outboxItem, publishErr error) error {
	tx, err := r.DB.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin outbox retry: %w", err)
	}
	defer tx.Rollback(ctx)
	base := r.RetryBase
	if base <= 0 {
		base = time.Second
	}
	shift := item.attempts - 1
	if shift > 10 {
		shift = 10
	}
	delay := base * time.Duration(1<<max(shift, 0))
	if delay > time.Hour {
		delay = time.Hour
	}
	msg := boundedText(publishErr.Error(), 1024)
	tag, err := tx.Exec(ctx, `UPDATE platform_event_outbox SET available_at=clock_timestamp()+$3*interval '1 microsecond',lease_token=NULL,lease_until=NULL,last_error=$4
		WHERE event_id=$1 AND lease_token=$2 AND sent_at IS NULL`, item.id, item.token, delay.Microseconds(), msg)
	if err != nil {
		return fmt.Errorf("persist outbox retry: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return ErrLeaseLost
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit outbox retry: %w", err)
	}
	return nil
}
