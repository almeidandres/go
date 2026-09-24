package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/id"
)

// BatchCheckpoint retains the exact HTTP body and mapping until both the send and mapping succeed.
type BatchCheckpoint struct {
	Portal networkid.PortalKey
	RoomID id.RoomID
	Data   []byte
}

func (db *Database) GetBatchCheckpoint(ctx context.Context, key networkid.PortalKey) (*BatchCheckpoint, error) {
	cp := &BatchCheckpoint{Portal: key}
	err := db.QueryRow(ctx, `SELECT room_mxid, data FROM batch_checkpoint WHERE bridge_id=$1 AND room_id=$2 AND room_receiver=$3`, db.BridgeID, key.ID, key.Receiver).Scan(&cp.RoomID, &cp.Data)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return cp, nil
}

func (db *Database) InsertBatchCheckpoint(ctx context.Context, cp *BatchCheckpoint) error {
	_, err := db.Exec(ctx, `INSERT INTO batch_checkpoint (bridge_id, room_id, room_receiver, room_mxid, data) VALUES ($1,$2,$3,$4,$5)`, db.BridgeID, cp.Portal.ID, cp.Portal.Receiver, cp.RoomID, string(cp.Data))
	return err
}

func (db *Database) DeleteBatchCheckpoint(ctx context.Context, key networkid.PortalKey) error {
	_, err := db.Exec(ctx, `DELETE FROM batch_checkpoint WHERE bridge_id=$1 AND room_id=$2 AND room_receiver=$3`, db.BridgeID, key.ID, key.Receiver)
	return err
}

func (db *Database) ListBatchCheckpoints(ctx context.Context) ([]*BatchCheckpoint, error) {
	rows, err := db.Query(ctx, `SELECT cp.room_id, cp.room_receiver, cp.room_mxid, cp.data, p.mxid
        FROM batch_checkpoint cp JOIN portal p ON (cp.bridge_id=p.bridge_id AND cp.room_id=p.id AND cp.room_receiver=p.receiver)
        WHERE cp.bridge_id=$1`, db.BridgeID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []*BatchCheckpoint
	for rows.Next() {
		cp := &BatchCheckpoint{}
		var currentRoom sql.NullString
		if err = rows.Scan(&cp.Portal.ID, &cp.Portal.Receiver, &cp.RoomID, &cp.Data, &currentRoom); err != nil {
			return nil, err
		}
		if !currentRoom.Valid || currentRoom.String != string(cp.RoomID) {
			return nil, fmt.Errorf("batch checkpoint room changed for %v", cp.Portal)
		}
		result = append(result, cp)
	}
	return result, rows.Err()
}
