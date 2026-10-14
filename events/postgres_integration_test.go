package events

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPostgresDurableReplayAfterQuarantineAck(t *testing.T) {
	pool := openPlatformTestPool(t)
	runID, name, _ := integrationIdentity(t, pool)
	e := integrationEnvelope(t, runID, "scheduled_replay")
	e.Data = []byte(`{"z":"last","a":"first"}`)
	payload, err := Encode(e)
	if err != nil {
		t.Fatal(err)
	}
	failing := true
	calls := 0
	consumer := &Consumer{DB: pool, Name: name, Handler: effectHandler(runID, &failing, &calls)}
	for range 5 {
		delivery := &integrationDelivery{payload: payload}
		if err = consumer.Consume(context.Background(), delivery); err != nil {
			t.Fatal(err)
		}
	}
	status, err := consumer.Status(context.Background(), e.EventID)
	if err != nil || status.Status != DispositionQuarantined {
		t.Fatal(status, err)
	}
	if err = consumer.Replay(context.Background(), e.EventID); err != nil {
		t.Fatal(err)
	}
	// A new consumer process recovers the durable request without broker redelivery
	// or the operator manually reading an envelope from the database.
	failing = false
	restarted := &Consumer{DB: pool, Name: name, Handler: effectHandler(runID, &failing, &calls)}
	n, err := restarted.RunReplayOnce(context.Background())
	if err != nil || n != 1 {
		t.Fatal(n, err)
	}
	status, err = restarted.Status(context.Background(), e.EventID)
	if err != nil || status.Status != DispositionApplied || calls != 6 {
		t.Fatal(status, calls, err)
	}
	n, err = restarted.RunReplayOnce(context.Background())
	if err != nil || n != 0 {
		t.Fatal("repeated replay", n, err)
	}
}

func TestPostgresEventBodyDoesNotDependOnJSONBValueLimits(t *testing.T) {
	pool := openPlatformTestPool(t)
	runID, name, _ := integrationIdentity(t, pool)
	e := integrationEnvelope(t, runID, "json_bytes")
	e.Data = []byte(`{"zero":"\u0000","huge":1e200000}`)
	tx, err := pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if err = Enqueue(context.Background(), tx, e, GenerationCompletedSubject); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	consumer := &Consumer{DB: pool, Name: name, Handler: effectHandler(runID, nil, nil)}
	if disposition, err := consumer.Process(context.Background(), e); err != nil || disposition != DispositionApplied {
		t.Fatal(disposition, err)
	}
}

type integrationDelivery struct {
	payload []byte
	ackErr  error
	nakErr  error
	acks    int
	naks    int
}

func (d *integrationDelivery) Payload() []byte { return d.payload }
func (d *integrationDelivery) Ack() error {
	d.acks++
	return d.ackErr
}
func (d *integrationDelivery) NakWithDelay(time.Duration) error {
	d.naks++
	return d.nakErr
}

type integrationPublisher struct {
	ids     []string
	results []error
}

func (p *integrationPublisher) Publish(_ context.Context, _ string, _ []byte, eventID string) error {
	p.ids = append(p.ids, eventID)
	if len(p.results) == 0 {
		return nil
	}
	err := p.results[0]
	p.results = p.results[1:]
	return err
}

func openPlatformTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("PLATFORM_TEST_DATABASE_URL")
	if strings.TrimSpace(dsn) == "" {
		t.Skip("PLATFORM_TEST_DATABASE_URL is not set; skipping PostgreSQL integration test")
	}
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse PLATFORM_TEST_DATABASE_URL: %v", err)
	}
	if config.ConnConfig.Database != "platform_test" {
		t.Fatal("PLATFORM_TEST_DATABASE_URL must point to the isolated platform test database")
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		t.Fatalf("open platform test database: %v", err)
	}
	t.Cleanup(pool.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("ping platform test database: %v", err)
	}
	// Each test migrates a fresh private schema: no artifacts from a previous
	// in-progress implementation can mask missing columns in the initial schema.
	token, err := newLeaseToken()
	if err != nil {
		t.Fatal(err)
	}
	schema := pgx.Identifier{"platform_test_" + token}.Sanitize()
	if _, err = pool.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	schemaAdmin := pool
	t.Cleanup(func() { _, _ = schemaAdmin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE") })
	config.ConnConfig.RuntimeParams["search_path"] = "platform_test_" + token
	testPool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(testPool.Close)
	pool = testPool
	if err := ApplyMigration(ctx, pool); err != nil {
		t.Fatalf("apply platform event migration: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS platform_event_test_effects (
			run_id text NOT NULL,
			event_id text NOT NULL,
			value integer NOT NULL,
			PRIMARY KEY (run_id, event_id)
		)`); err != nil {
		t.Fatalf("create test effects table: %v", err)
	}
	return pool
}

func integrationIdentity(t *testing.T, pool *pgxpool.Pool) (string, string, func()) {
	t.Helper()
	runID, err := newLeaseToken()
	if err != nil {
		t.Fatal(err)
	}
	consumerName := "test_" + runID
	cleanup := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = pool.Exec(ctx, `DELETE FROM platform_event_inbox WHERE consumer_name = $1`, consumerName)
		_, _ = pool.Exec(ctx, `DELETE FROM platform_event_outbox WHERE event_id LIKE $1`, "evt_"+runID+"_%")
		_, _ = pool.Exec(ctx, `DELETE FROM platform_event_test_effects WHERE run_id = $1`, runID)
	}
	t.Cleanup(cleanup)
	return runID, consumerName, cleanup
}

func integrationEnvelope(t *testing.T, runID, suffix string) Envelope {
	t.Helper()
	e := validEnvelope()
	e.EventID = "evt_" + runID + "_" + suffix
	e.WorkspaceID = "workspace_" + runID
	e.ProjectID = "project_" + runID
	return e
}

func effectHandler(runID string, fail *bool, calls *int) Handler {
	return func(ctx context.Context, tx pgx.Tx, e Envelope) error {
		if calls != nil {
			(*calls)++
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO platform_event_test_effects (run_id, event_id, value)
			VALUES ($1, $2, 1)
			ON CONFLICT (run_id, event_id) DO UPDATE SET value = platform_event_test_effects.value + 1`, runID, e.EventID); err != nil {
			return err
		}
		if fail != nil && *fail {
			return errors.New("simulated handler failure")
		}
		return nil
	}
}

func TestPostgresConsumerDuplicateAndLostAckApplyEffectOnce(t *testing.T) {
	pool := openPlatformTestPool(t)
	runID, name, _ := integrationIdentity(t, pool)
	e := integrationEnvelope(t, runID, "duplicate")
	payload, err := Encode(e)
	if err != nil {
		t.Fatal(err)
	}
	consumer := &Consumer{DB: pool, Name: name, Handler: effectHandler(runID, nil, nil)}
	first := &integrationDelivery{payload: payload, ackErr: errors.New("simulated lost ACK")}
	if err := consumer.Consume(context.Background(), first); err == nil || first.acks != 1 {
		t.Fatalf("first Consume() error=%v ACKs=%d", err, first.acks)
	}
	second := &integrationDelivery{payload: payload}
	if err := consumer.Consume(context.Background(), second); err != nil {
		t.Fatalf("redelivery Consume() error = %v", err)
	}
	var effects, appliedAttempts int
	if err := pool.QueryRow(context.Background(), `
		SELECT count(*) FROM platform_event_test_effects WHERE run_id = $1 AND event_id = $2`, runID, e.EventID).Scan(&effects); err != nil {
		t.Fatal(err)
	}
	status, err := consumer.Status(context.Background(), e.EventID)
	if err != nil {
		t.Fatal(err)
	}
	appliedAttempts = status.Attempts
	if effects != 1 || appliedAttempts != 1 || second.acks != 1 {
		t.Fatalf("effects=%d attempts=%d ACKs=%d; want 1/1/1", effects, appliedAttempts, second.acks)
	}
}

func TestPostgresRejectsEventIDReuseWithDifferentPayload(t *testing.T) {
	pool := openPlatformTestPool(t)
	runID, name, _ := integrationIdentity(t, pool)
	e := integrationEnvelope(t, runID, "collision")
	ctx := context.Background()

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := Enqueue(ctx, tx, e, GenerationCompletedSubject); err != nil {
		tx.Rollback(ctx)
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	changed := e
	changed.ProjectID += "_changed"
	tx, err = pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := Enqueue(ctx, tx, changed, GenerationCompletedSubject); !errors.Is(err, ErrEventIDConflict) {
		tx.Rollback(ctx)
		t.Fatalf("Enqueue() collision error = %v, want ErrEventIDConflict", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	consumer := &Consumer{DB: pool, Name: name, Handler: effectHandler(runID, nil, nil)}
	if _, err := consumer.Process(ctx, e); err != nil {
		t.Fatal(err)
	}
	if _, err := consumer.Process(ctx, changed); !errors.Is(err, ErrEventIDConflict) {
		t.Fatalf("Process() collision error = %v, want ErrEventIDConflict", err)
	}
	var effects int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM platform_event_test_effects WHERE run_id=$1 AND event_id=$2`, runID, e.EventID).Scan(&effects); err != nil {
		t.Fatal(err)
	}
	if effects != 1 {
		t.Fatalf("effects after payload collision = %d, want 1", effects)
	}
}

func TestPostgresConsumerQuarantinesAtFiveAndNeverAppliesSixth(t *testing.T) {
	pool := openPlatformTestPool(t)
	runID, name, _ := integrationIdentity(t, pool)
	e := integrationEnvelope(t, runID, "failures")
	fail := true
	calls := 0
	consumer := &Consumer{DB: pool, Name: name, Handler: effectHandler(runID, &fail, &calls)}
	for attempt := 1; attempt <= MaxConsumerAttempts; attempt++ {
		disposition, err := consumer.Process(context.Background(), e)
		if err != nil {
			t.Fatalf("attempt %d: %v", attempt, err)
		}
		want := DispositionRetry
		if attempt == MaxConsumerAttempts {
			want = DispositionQuarantined
		}
		if disposition != want {
			t.Fatalf("attempt %d disposition = %q, want %q", attempt, disposition, want)
		}
	}
	disposition, err := consumer.Process(context.Background(), e)
	if err != nil || disposition != DispositionQuarantined {
		t.Fatalf("post-quarantine delivery = %q, %v", disposition, err)
	}
	status, err := consumer.Status(context.Background(), e.EventID)
	if err != nil {
		t.Fatal(err)
	}
	var effects int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM platform_event_test_effects WHERE run_id = $1`, runID).Scan(&effects); err != nil {
		t.Fatal(err)
	}
	if calls != MaxConsumerAttempts || status.Attempts != MaxConsumerAttempts || status.Status != string(inboxQuarantined) || effects != 0 {
		t.Fatalf("calls=%d status=%#v effects=%d", calls, status, effects)
	}
}

func TestPostgresCrashAfterFifthClaimQuarantinesOnRedelivery(t *testing.T) {
	pool := openPlatformTestPool(t)
	runID, name, _ := integrationIdentity(t, pool)
	e := integrationEnvelope(t, runID, "crashed-final-claim")
	consumer := &Consumer{DB: pool, Name: name, Handler: effectHandler(runID, nil, nil)}
	encoded, err := Encode(e)
	if err != nil {
		t.Fatal(err)
	}
	for attempt := 1; attempt <= MaxConsumerAttempts; attempt++ {
		owned, disposition, err := consumer.claim(context.Background(), e, encoded)
		if err != nil || disposition != "" || owned.attempt != attempt {
			t.Fatalf("claim %d = %#v, %q, %v", attempt, owned, disposition, err)
		}
		if _, err := pool.Exec(context.Background(), `
			UPDATE platform_event_inbox SET lease_until = clock_timestamp() - interval '1 second'
			WHERE consumer_name = $1 AND event_id = $2 AND lease_token = $3`, name, e.EventID, owned.token); err != nil {
			t.Fatal(err)
		}
	}
	disposition, err := consumer.Process(context.Background(), e)
	if err != nil || disposition != DispositionQuarantined {
		t.Fatalf("redelivery disposition = %q, error = %v", disposition, err)
	}
	status, err := consumer.Status(context.Background(), e.EventID)
	if err != nil {
		t.Fatal(err)
	}
	if status.Status != string(inboxQuarantined) || status.Attempts != MaxConsumerAttempts {
		t.Fatalf("status after final-claim crash = %#v", status)
	}
}

func TestPostgresReplayResetsAttemptsAndWritesHistory(t *testing.T) {
	pool := openPlatformTestPool(t)
	runID, name, _ := integrationIdentity(t, pool)
	e := integrationEnvelope(t, runID, "replay")
	fail := true
	consumer := &Consumer{DB: pool, Name: name, Handler: effectHandler(runID, &fail, nil)}
	for range MaxConsumerAttempts {
		if _, err := consumer.Process(context.Background(), e); err != nil {
			t.Fatal(err)
		}
	}
	if err := consumer.Replay(context.Background(), e.EventID); err != nil {
		t.Fatalf("Replay() error = %v", err)
	}
	if status, err := consumer.Status(context.Background(), e.EventID); err != nil || status.Status != string(inboxPending) || status.Attempts != 0 || status.ReplayCount != 1 {
		t.Fatalf("status after replay = %#v, error = %v", status, err)
	}
	fail = false
	if disposition, err := consumer.Process(context.Background(), e); err != nil || disposition != DispositionApplied {
		t.Fatalf("replayed event result = %q, error = %v", disposition, err)
	}
	if err := consumer.Replay(context.Background(), e.EventID); !errors.Is(err, ErrEventResolved) {
		t.Fatalf("Replay() on applied event error = %v, want ErrEventResolved", err)
	}
	history, err := consumer.History(context.Background(), e.EventID)
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 2 || history[0].Action != "quarantined" || history[1].Action != "replayed" {
		t.Fatalf("event history = %#v", history)
	}
}

func TestPostgresOutboxRetriesSameEventAfterLostPublishAck(t *testing.T) {
	pool := openPlatformTestPool(t)
	runID, _, _ := integrationIdentity(t, pool)
	e := integrationEnvelope(t, runID, "outbox-ack-loss")
	tx, err := pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := Enqueue(context.Background(), tx, e, GenerationCompletedSubject); err != nil {
		tx.Rollback(context.Background())
		t.Fatal(err)
	}
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	publisher := &integrationPublisher{results: []error{errors.New("simulated publish ACK loss"), nil}}
	relay := &Relay{DB: pool, Publisher: publisher, RetryBase: time.Microsecond}
	if sent, err := relay.RunOnce(context.Background()); sent != 0 || err == nil {
		t.Fatalf("first relay pass sent=%d error=%v", sent, err)
	}
	if _, err := pool.Exec(context.Background(), `UPDATE platform_event_outbox SET available_at = clock_timestamp() WHERE event_id = $1`, e.EventID); err != nil {
		t.Fatal(err)
	}
	if sent, err := relay.RunOnce(context.Background()); sent != 1 || err != nil {
		t.Fatalf("second relay pass sent=%d error=%v", sent, err)
	}
	var attempts int
	var sentAt *time.Time
	if err := pool.QueryRow(context.Background(), `SELECT attempts, sent_at FROM platform_event_outbox WHERE event_id = $1`, e.EventID).Scan(&attempts, &sentAt); err != nil {
		t.Fatal(err)
	}
	if len(publisher.ids) != 2 || publisher.ids[0] != e.EventID || publisher.ids[1] != e.EventID || attempts != 2 || sentAt == nil {
		t.Fatalf("publish IDs=%v attempts=%d sentAt=%v", publisher.ids, attempts, sentAt)
	}
}

func TestPostgresRetentionDeletesOnlyOldResolvedRows(t *testing.T) {
	pool := openPlatformTestPool(t)
	runID, name, _ := integrationIdentity(t, pool)
	ctx := context.Background()
	eApplied := integrationEnvelope(t, runID, "retention-applied")
	consumer := &Consumer{DB: pool, Name: name, Handler: effectHandler(runID, nil, nil)}
	if disposition, err := consumer.Process(ctx, eApplied); err != nil || disposition != DispositionApplied {
		t.Fatalf("create applied event = %q, %v", disposition, err)
	}
	ePending := integrationEnvelope(t, runID, "retention-pending")
	encodedPending, _ := Encode(ePending)
	if _, _, err := consumer.claim(ctx, ePending, encodedPending); err != nil {
		t.Fatal(err)
	}
	eQuarantined := integrationEnvelope(t, runID, "retention-quarantine")
	fail := true
	quarantiningConsumer := &Consumer{DB: pool, Name: name, Handler: effectHandler(runID, &fail, nil)}
	for range MaxConsumerAttempts {
		if _, err := quarantiningConsumer.Process(ctx, eQuarantined); err != nil {
			t.Fatal(err)
		}
	}
	eResolved := integrationEnvelope(t, runID, "retention-resolved")
	for range MaxConsumerAttempts {
		if _, err := quarantiningConsumer.Process(ctx, eResolved); err != nil {
			t.Fatal(err)
		}
	}
	if err := quarantiningConsumer.Resolve(ctx, eResolved.EventID, "closed for retention test"); err != nil {
		t.Fatal(err)
	}
	eSent := integrationEnvelope(t, runID, "retention-outbox-sent")
	eOutboxPending := integrationEnvelope(t, runID, "retention-outbox-pending")
	for _, e := range []Envelope{eSent, eOutboxPending} {
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := Enqueue(ctx, tx, e, GenerationCompletedSubject); err != nil {
			tx.Rollback(ctx)
			t.Fatal(err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
	}
	old := time.Now().UTC().Add(-31 * 24 * time.Hour)
	for _, eventID := range []string{eApplied.EventID, ePending.EventID, eQuarantined.EventID, eResolved.EventID} {
		if _, err := pool.Exec(ctx, `UPDATE platform_event_inbox SET created_at = $3, updated_at = $3 WHERE consumer_name = $1 AND event_id = $2`, name, eventID, old); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := pool.Exec(ctx, `UPDATE platform_event_inbox SET applied_at = $3 WHERE consumer_name = $1 AND event_id = $2`, name, eApplied.EventID, old); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE platform_event_inbox SET quarantined_at = $3 WHERE consumer_name = $1 AND event_id = $2`, name, eQuarantined.EventID, old); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE platform_event_inbox SET resolved_at = $3 WHERE consumer_name = $1 AND event_id = $2`, name, eResolved.EventID, old); err != nil {
		t.Fatal(err)
	}
	for _, eventID := range []string{eSent.EventID, eOutboxPending.EventID} {
		if _, err := pool.Exec(ctx, `UPDATE platform_event_outbox SET created_at = $2, available_at = $2 WHERE event_id = $1`, eventID, old); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := pool.Exec(ctx, `UPDATE platform_event_outbox SET sent_at = $2 WHERE event_id = $1`, eSent.EventID, old); err != nil {
		t.Fatal(err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	counts, err := PurgeExpired(ctx, tx, time.Now())
	if err != nil {
		tx.Rollback(ctx)
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if counts.Outbox != 1 || counts.Inbox != 2 {
		t.Fatalf("retention counts = %#v, want one outbox and two inbox rows", counts)
	}
	for _, tc := range []struct {
		eventID string
		table   string
		want    bool
	}{
		{eSent.EventID, "platform_event_outbox", false},
		{eOutboxPending.EventID, "platform_event_outbox", true},
		{eApplied.EventID, "platform_event_inbox", false},
		{eResolved.EventID, "platform_event_inbox", false},
		{ePending.EventID, "platform_event_inbox", true},
		{eQuarantined.EventID, "platform_event_inbox", true},
	} {
		query := fmt.Sprintf("SELECT EXISTS (SELECT 1 FROM %s WHERE event_id = $1)", tc.table)
		var exists bool
		if err := pool.QueryRow(ctx, query, tc.eventID).Scan(&exists); err != nil {
			t.Fatal(err)
		}
		if exists != tc.want {
			t.Errorf("%s row %s exists=%v, want %v", tc.table, tc.eventID, exists, tc.want)
		}
	}
}
