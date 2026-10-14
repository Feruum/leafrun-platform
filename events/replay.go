package events

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// RunReplayOnce recovers one durable operator replay without requiring a broker
// redelivery after quarantine was ACKed. Run this alongside the normal consumer.
// Process supplies the same durable attempt/lease fencing for both transports.
// A crash after the effect commit repeats Process, which observes applied and
// does not repeat the effect. The replay generation fences concurrent operators.
func (c *Consumer) RunReplayOnce(ctx context.Context) (int, error) {
	if err := c.validate(); err != nil {
		return 0, err
	}
	var eventID string
	var encoded []byte
	var generation int
	err := c.DB.QueryRow(ctx, `SELECT event_id,encoded_envelope,replay_count FROM platform_event_inbox
 WHERE consumer_name=$1 AND replay_requested AND replay_available_at<=clock_timestamp()
 AND (lease_until IS NULL OR lease_until<=clock_timestamp()) ORDER BY replay_available_at,event_id LIMIT 1`, c.Name).Scan(&eventID, &encoded, &generation)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("read durable replay: %w", err)
	}
	envelope, err := Decode(encoded)
	if err != nil {
		return 0, err
	}
	disposition, err := c.Process(ctx, envelope)
	if err != nil {
		return 0, err
	}
	if disposition == DispositionBusy {
		return 0, nil
	}
	tx, err := c.DB.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	switch disposition {
	case DispositionApplied, DispositionQuarantined, DispositionResolved:
		_, err = tx.Exec(ctx, `UPDATE platform_event_inbox SET replay_requested=false WHERE consumer_name=$1 AND event_id=$2 AND replay_count=$3 AND status IN ('applied','quarantined','resolved')`, c.Name, eventID, generation)
	case DispositionRetry:
		delay := c.RetryDelay
		if delay <= 0 {
			delay = time.Second
		}
		_, err = tx.Exec(ctx, `UPDATE platform_event_inbox SET replay_available_at=clock_timestamp()+$4*interval '1 microsecond' WHERE consumer_name=$1 AND event_id=$2 AND replay_count=$3 AND replay_requested`, c.Name, eventID, generation, delay.Microseconds())
	default:
		return 0, fmt.Errorf("unknown replay disposition %q", disposition)
	}
	if err != nil {
		return 0, err
	}
	if err = tx.Commit(ctx); err != nil {
		return 0, err
	}
	return 1, nil
}
