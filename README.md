# leafrun-platform

`leafrun-platform` is a small Go module for transport and reliability code shared by Leafrun services. The first package is `events`, which provides a versioned event envelope, a PostgreSQL outbox relay, a JetStream publisher adapter, and a PostgreSQL inbox for idempotent consumers.

Each service keeps its own data and applies the event migration to its own database. There is no shared business database. Producers call `Enqueue` with the same `pgx.Tx` that writes their domain state, and consumers pass their owner database transaction to the handler.

## Event contract

`events.Envelope` uses these JSON fields: `schemaVersion`, `eventId`, `eventType`, `producer`, `aggregateId`, `aggregateVersion`, `workspaceId`, `projectId`, `occurredAt`, `correlationId`, `causationId`, and `data`. Schema version 1 is accepted. IDs are limited to 128 bytes; aggregate versions are positive PostgreSQL `bigint` values; and `data` must be valid JSON no larger than 1 MiB. Correlation and causation IDs may be empty.

The generation completion route is:

| Field | Value |
| --- | --- |
| Subject | `leafrun.generation.job.completed.v1` |
| Event type | `generation.completed` |
| Data | `{"jobId":"<owner-supplied id>"}` |

`NewGenerationCompleted` accepts the generation service's job, workspace, and project IDs directly. Its domain data contains only the caller-supplied job ID. It does not look up or copy a record from another service.

## Install the migration

Apply the embedded migration to each service's database during that service's startup or deployment migration step:

```go
if err := events.ApplyMigration(ctx, pool); err != nil {
	return fmt.Errorf("install event tables: %w", err)
}
```

`ApplyMigration` is idempotent. It creates `platform_event_outbox`, `platform_event_inbox`, and `platform_event_history` in the current database. The service remains responsible for its database credentials, schema ownership, and migration timing.

## Write owner state and enqueue atomically

Create the event while the domain transaction is still open. If either write fails, roll back both. The outbox relay publishes only committed rows.

```go
tx, err := pool.Begin(ctx)
if err != nil {
	return err
}
defer tx.Rollback(ctx)

// Write the generation job's terminal state using tx first.
event, subject, err := events.NewGenerationCompleted(events.GenerationCompletedInput{
	EventID:          eventID,
	Producer:         "leafrun-generation",
	JobID:            jobID,
	WorkspaceID:      workspaceID,
	ProjectID:        projectID,
	AggregateVersion: nextVersion,
	OccurredAt:       time.Now().UTC(),
	CorrelationID:    correlationID,
	CausationID:      causationID,
})
if err != nil {
	return err
}
if err := events.Enqueue(ctx, tx, event, subject); err != nil {
	return err
}
return tx.Commit(ctx)
```

`Enqueue` is idempotent for an identical event ID, subject, and envelope. Reusing an event ID with different contents returns `events.ErrEventIDConflict`.

## Relay outbox rows to JetStream

Construct a JetStream publisher from the service's configured NATS connection, then poll the relay from a worker loop:

```go
js, err := jetstream.New(natsConn)
if err != nil {
	return err
}
relay := events.Relay{
	DB:        pool,
	Publisher: events.JetStreamPublisher{Client: js},
}
sent, err := relay.RunOnce(ctx)
```

`RunOnce` claims pending rows under PostgreSQL locks, commits a lease, publishes synchronously, waits for JetStream's publish ACK, then records `sent_at`. The adapter sets `Nats-Msg-Id` to the stable `eventId`. If publishing succeeds but the ACK or database update is lost, the row is retried with the same event ID. Broker deduplication is a useful optimization; the outbox lease and retry record provide the delivery guarantee.

Failed publishes receive bounded exponential backoff. Expired leases become claimable again after a relay crash. A sent row is retained for 30 days; pending or leased rows are never removed by retention.

## Apply consumer effects once in the owner database

Use a stable consumer name across restarts. The handler must write effects through the supplied `pgx.Tx`; the library marks the inbox row applied in that same transaction.

```go
consumer := &events.Consumer{
	DB:   pool,
	Name: "notifications-generation-completed-v1",
	Handler: func(ctx context.Context, tx pgx.Tx, event events.Envelope) error {
		var data events.GenerationCompletedData
		if err := json.Unmarshal(event.Data, &data); err != nil {
			return err
		}
		_, err := tx.Exec(ctx,
			`INSERT INTO notifications (job_id) VALUES ($1) ON CONFLICT DO NOTHING`,
			data.JobID,
		)
		return err
	},
}

// Call this for each message from the configured durable pull consumer.
err := consumer.Consume(ctx, events.NATSDelivery{Message: msg})
```

The order is deliberate: the application attempt is committed first; the handler effect and successful inbox state then commit together; only after that does `Consume` ACK JetStream. A lost ACK redelivers the message, and the durable inbox row ACKs it without applying the effect again. Database, handler-transaction, and commit errors return without ACK. Handler errors roll back the effect and NAK the delivery for retry.

The application ledger owns the five-attempt limit. Configure the JetStream durable consumer with explicit ACKs and unlimited broker deliveries so broker delivery counters cannot quarantine an event early. Active application leases prevent two deliveries from applying the same event concurrently. A lease token and final lease check fence effects if a handler runs past its lease.

After the fifth failed application, the failure and quarantine record commit before ACK. If the process crashes after claiming attempt five but before quarantine or application, the expired lease is quarantined on the next delivery without starting a sixth application. Quarantined deliveries ACK without running the handler. Use `Status` and `History` to inspect durable state.

`Replay(ctx, eventID)` is an explicit operator action. It atomically moves a quarantined event back to pending, resets its attempt count, increments `replay_count`, records a durable local replay request, and adds an audit entry. Run `RunReplayOnce(ctx)` alongside the broker consumer: it recovers the saved envelope after restart even though quarantine was already ACKed. Retry scheduling survives crashes; application uses the same inbox leases and fencing. A newer operator replay generation cannot be cleared by an older worker. Applied/resolved events and active leases are rejected. `Resolve(ctx, eventID, reason)` closes unresolved quarantine and records the reason; resolved events ACK on redelivery.

Exact encoded envelope bytes are stored as BYTEA. The relay and replay worker consume those original bytes, preserving the immutable payload hash and JSON number lexemes. No JSONB conversion limits the domain payload before the durable application ledger can record its handling. Identifiers require valid UTF-8 without NUL; encoded envelope/data sizes are checked after escaping. Pending replay requests block retention until resolved.

## Aggregate version gaps

`CheckAggregateVersion` ignores an event whose version is already applied and approves exactly the next version. On a gap it calls an owner-provided `VersionRepairer` with the event IDs and target version (`event version - 1`). The owner can fetch or resynchronize through its own API. The event handler must still check the version inside its effect transaction before applying, because owner state can change after the helper returns.

## Retention

Call `PurgeExpired` in a transaction owned by the same service database, for example from a daily maintenance task:

```go
tx, err := pool.Begin(ctx)
if err != nil {
	return err
}
defer tx.Rollback(ctx)
if _, err := events.PurgeExpired(ctx, tx, time.Now()); err != nil {
	return err
}
return tx.Commit(ctx)
```

Sent outbox rows and applied or resolved inbox/history rows older than 30 days are eligible for deletion. Pending rows, active or expired leases, and unresolved quarantines are preserved. Expired leases are retried; they are not age-purged.

## Configuration and permissions

Each service configures its own PostgreSQL pool, NATS connection, durable consumer name, and worker lifecycle. Use separate NATS credentials with only the publish or subscribe permissions that service needs. The generation producer publishes to `leafrun.generation.job.completed.v1`; consumers subscribe to the specific subjects they handle and use explicit ACKs. The package does not create streams, durables, users, or credentials.

The relay and consumer accept transaction-beginner interfaces, so they can use a `pgxpool.Pool`. The generation producer uses `Enqueue` with its existing `pgx.Tx`. A consumer handler receives a `pgx.Tx` from the consumer database and must not commit it itself.

## Tests

Run unit tests and compile the PostgreSQL integration suite with:

```sh
go test ./...
```

PostgreSQL integration tests run only when `PLATFORM_TEST_DATABASE_URL` points to a dedicated platform test database. They apply the embedded migration and exercise duplicate/lost ACK handling, five failed applications, final-claim crash recovery, replay audit, outbox ACK loss, and retention. They skip when the variable is absent and reject a database named `storage`.
## E06 authentication and event hierarchy

`authn.NewClient(baseURL, serviceBearer, cookieName, httpClient)` implements the
technical `Authenticator` and `Checker` interfaces using the versioned Contracts
client. An empty cookie name selects exactly `ory_kratos_session`; only that
cookie's opaque value is forwarded. Duplicate Authorization values, caller
bearer/cookie ambiguity, duplicate session cookies and fixed service credentials
used as public credentials are rejected before the network call. Remote service
and dev identities, including the service principal domain, cannot authenticate a
public caller. Internal checks can still carry authorized service references.

`WithIdentity(ctx, identity)` validates a resolved or durable server identity
before context propagation; `IdentityFromContext` retrieves that comparable
value. Neither helper stores caller tokens or trusts public identity headers.
Authorization policy, databases, explicit development adapters, and current
authority checks belong to their owners.

Event schema v1 keeps its exact JSON encoding and required workspace/project.
Schema v2 adds optional organization and supports identity events with no tenant,
organization-only, organization/workspace, and organization/workspace/project.
Workspace requires organization; project requires both ancestors. The shared
Contracts JSON decoder rejects duplicate, trailing, unknown and invalid-Unicode
input. Existing GenerationCompleted subjects and constructors remain v1.
