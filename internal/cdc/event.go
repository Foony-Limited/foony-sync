// Package cdc reads PostgreSQL logical replication (WAL) events and surfaces
// them as a stream of row-level change events.
//
// Derived from github.com/emoss08/gtc (MIT). See LICENSE in this directory.
package cdc

import (
	"context"
	"fmt"
	"time"
)

// Operation enumerates the kinds of row changes the WAL stream produces.
type Operation int

const (
	// OperationInsert is a new row being added.
	OperationInsert Operation = iota
	// OperationUpdate is an existing row being modified.
	OperationUpdate
	// OperationDelete is a row being removed.
	OperationDelete
	// OperationTruncate is an entire table being truncated.
	OperationTruncate
)

// String returns a human-readable name for the operation.
func (o Operation) String() string {
	switch o {
	case OperationInsert:
		return "INSERT"
	case OperationUpdate:
		return "UPDATE"
	case OperationDelete:
		return "DELETE"
	case OperationTruncate:
		return "TRUNCATE"
	default:
		return "UNKNOWN"
	}
}

// EventMetadata carries WAL-position metadata for traceability.
type EventMetadata struct {
	LSN           string
	TransactionID uint32
	Timestamp     time.Time
}

// Event represents a single row-level change captured from the WAL.
//
// For INSERT, NewData holds the new row.
// For UPDATE, NewData holds the new row; OldData (if present) holds the prior
// REPLICA IDENTITY columns (typically the primary key).
// For DELETE, OldData holds the deleted row's REPLICA IDENTITY columns.
// For TRUNCATE, both OldData and NewData are nil.
type Event struct {
	ID        string
	Operation Operation
	Schema    string
	Table     string
	OldData   map[string]any
	NewData   map[string]any
	Metadata  EventMetadata
}

// FullTableName returns the fully-qualified table name (schema.table).
func (e *Event) FullTableName() string {
	return fmt.Sprintf("%s.%s", e.Schema, e.Table)
}

// Handler is the callback the Reader invokes for each decoded event.
type Handler func(ctx context.Context, event Event) error
