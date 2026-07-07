package cdc

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestPublicationPermissionHint(t *testing.T) {
	t.Parallel()
	sql := "CREATE PUBLICATION foony_sync_abc FOR TABLE users, orders"
	cases := []struct {
		name     string
		err      error
		wantHint bool
	}{
		{
			name:     "permission denied carries the statement to run",
			err:      &pgconn.PgError{Code: "42501", Message: "permission denied for database foony-main"},
			wantHint: true,
		},
		{
			name:     "wrapped permission denied still matches",
			err:      fmt.Errorf("exec: %w", &pgconn.PgError{Code: "42501"}),
			wantHint: true,
		},
		{
			name:     "other postgres errors get no hint",
			err:      &pgconn.PgError{Code: "42P01", Message: "relation does not exist"},
			wantHint: false,
		},
		{
			name:     "non-postgres errors get no hint",
			err:      errors.New("connection reset"),
			wantHint: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			hint := publicationPermissionHint(tc.err, sql)
			if tc.wantHint && !strings.Contains(hint, sql) {
				t.Fatalf("hint %q does not carry the statement", hint)
			}
			if !tc.wantHint && hint != "" {
				t.Fatalf("unexpected hint %q", hint)
			}
		})
	}
}
