// Derived from github.com/emoss08/gtc (MIT). See LICENSE in this directory.

package cdc

import (
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
)

// decoder turns raw pgproto3 messages from a logical-replication stream into
// CDC Events. It tracks RelationMessages so it can map column names/types and
// honours streamed transactions.
type decoder struct {
	relations map[uint32]*pglogrepl.RelationMessageV2
	typeMap   *pgtype.Map
	inStream  bool
}

func newDecoder() *decoder {
	return &decoder{
		relations: make(map[uint32]*pglogrepl.RelationMessageV2),
		typeMap:   pgtype.NewMap(),
	}
}

// decodeResult is the output of one ReceiveMessage call: zero-or-more decoded
// events plus the LSN we should ack on the next standby status update.
type decodeResult struct {
	events []Event
	lsn    pglogrepl.LSN
}

func (d *decoder) decode(rawMsg pgproto3.BackendMessage) (*decodeResult, error) {
	copyData, ok := rawMsg.(*pgproto3.CopyData)
	if !ok {
		return &decodeResult{}, nil
	}

	switch copyData.Data[0] {
	case pglogrepl.PrimaryKeepaliveMessageByteID:
		pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(copyData.Data[1:])
		if err != nil {
			return nil, err
		}
		return &decodeResult{lsn: pkm.ServerWALEnd}, nil

	case pglogrepl.XLogDataByteID:
		xld, err := pglogrepl.ParseXLogData(copyData.Data[1:])
		if err != nil {
			return nil, err
		}
		events, err := d.decodeWALData(xld.WALData, xld.WALStart)
		if err != nil {
			return nil, err
		}
		return &decodeResult{events: events, lsn: xld.WALStart}, nil
	}

	return &decodeResult{}, nil
}

func (d *decoder) decodeWALData(walData []byte, lsn pglogrepl.LSN) ([]Event, error) {
	logicalMsg, err := pglogrepl.ParseV2(walData, d.inStream)
	if err != nil {
		return nil, err
	}

	var events []Event

	switch msg := logicalMsg.(type) {
	case *pglogrepl.RelationMessageV2:
		d.relations[msg.RelationID] = msg

	case *pglogrepl.InsertMessageV2:
		rel := d.relations[msg.RelationID]
		if rel == nil {
			return nil, nil
		}
		events = append(events, Event{
			ID:        lsn.String(),
			Operation: OperationInsert,
			Schema:    rel.Namespace,
			Table:     rel.RelationName,
			NewData:   d.decodeTuple(msg.Tuple, rel),
			Metadata:  EventMetadata{LSN: lsn.String(), TransactionID: msg.Xid},
		})

	case *pglogrepl.UpdateMessageV2:
		rel := d.relations[msg.RelationID]
		if rel == nil {
			return nil, nil
		}
		event := Event{
			ID:        lsn.String(),
			Operation: OperationUpdate,
			Schema:    rel.Namespace,
			Table:     rel.RelationName,
			NewData:   d.decodeTuple(msg.NewTuple, rel),
			Metadata:  EventMetadata{LSN: lsn.String(), TransactionID: msg.Xid},
		}
		if msg.OldTuple != nil {
			event.OldData = d.decodeTuple(msg.OldTuple, rel)
		}
		events = append(events, event)

	case *pglogrepl.DeleteMessageV2:
		rel := d.relations[msg.RelationID]
		if rel == nil {
			return nil, nil
		}
		events = append(events, Event{
			ID:        lsn.String(),
			Operation: OperationDelete,
			Schema:    rel.Namespace,
			Table:     rel.RelationName,
			OldData:   d.decodeTuple(msg.OldTuple, rel),
			Metadata:  EventMetadata{LSN: lsn.String(), TransactionID: msg.Xid},
		})

	case *pglogrepl.TruncateMessageV2:
		for _, relID := range msg.RelationIDs {
			if rel, ok := d.relations[relID]; ok {
				events = append(events, Event{
					ID:        lsn.String(),
					Operation: OperationTruncate,
					Schema:    rel.Namespace,
					Table:     rel.RelationName,
					Metadata:  EventMetadata{LSN: lsn.String(), TransactionID: msg.Xid},
				})
			}
		}

	case *pglogrepl.StreamStartMessageV2:
		d.inStream = true

	case *pglogrepl.StreamStopMessageV2:
		d.inStream = false
	}

	return events, nil
}

func (d *decoder) decodeTuple(
	tuple *pglogrepl.TupleData,
	rel *pglogrepl.RelationMessageV2,
) map[string]any {
	if tuple == nil {
		return nil
	}

	values := make(map[string]any, len(tuple.Columns))
	for idx, col := range tuple.Columns {
		colName := rel.Columns[idx].Name
		switch col.DataType {
		case 'n':
			values[colName] = nil
		case 'u':
			// Unchanged TOAST column. We don't have the value here; use a
			// sentinel so subscribers can fetch the row fresh if needed.
			values[colName] = unchangedToast{}
		case 't':
			val, err := d.decodeTextColumn(col.Data, rel.Columns[idx].DataType)
			if err != nil {
				values[colName] = string(col.Data)
			} else {
				values[colName] = val
			}
		}
	}
	return values
}

func (d *decoder) decodeTextColumn(data []byte, dataType uint32) (any, error) {
	if dt, ok := d.typeMap.TypeForOID(dataType); ok {
		return dt.Codec.DecodeValue(d.typeMap, dataType, pgtype.TextFormatCode, data)
	}
	return string(data), nil
}

// unchangedToast is a sentinel value placed in tuple maps for columns whose
// values were not included in the WAL because they were unchanged TOASTed
// values. Consumers that need the column should re-query the row.
type unchangedToast struct{}
