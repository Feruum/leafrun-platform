package events

import (
	"context"
	"embed"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

// ApplyMigration creates the event tables in the caller's service database.
// Each service owns and migrates its own database.
func ApplyMigration(ctx context.Context, db interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}) error {
	entries, err := migrationFS.ReadDir("migrations")
	if err != nil {
		return fmt.Errorf("read platform events migration: %w", err)
	}
	for _, entry := range entries {
		sql, err := migrationFS.ReadFile("migrations/" + entry.Name())
		if err != nil {
			return err
		}
		if _, err = db.Exec(ctx, string(sql)); err != nil {
			return fmt.Errorf("apply platform events migration %s: %w", entry.Name(), err)
		}
	}
	return nil
}
