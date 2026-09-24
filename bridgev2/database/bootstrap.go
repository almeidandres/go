package database

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"maunium.net/go/mautrix/bridgev2/networkid"
)

type BootstrapJob struct {
	LoginID        networkid.UserLoginID
	Portal         networkid.PortalKey
	Status         string
	Cursor         string
	SourceComplete bool
	LastError      string
}

type BootstrapItem struct {
	StableID  string
	Kind      string
	Version   int
	Payload   string
	SourceTS  int64
	Order     int64
	Delivered bool
}

func (db *Database) GetBootstrapJob(ctx context.Context, login networkid.UserLoginID, portal networkid.PortalKey) (*BootstrapJob, error) {
	job := &BootstrapJob{LoginID: login, Portal: portal}
	err := db.QueryRow(ctx, `SELECT status, cursor, source_complete, COALESCE(last_error, '') FROM portal_bootstrap
		WHERE bridge_id=$1 AND user_login_id=$2 AND portal_id=$3 AND portal_receiver=$4`,
		db.BridgeID, login, portal.ID, portal.Receiver).Scan(&job.Status, &job.Cursor, &job.SourceComplete, &job.LastError)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return job, err
}

// GetUnfinishedBootstrapJobs returns selected chats that require import or replay.
func (db *Database) GetUnfinishedBootstrapJobs(ctx context.Context, login networkid.UserLoginID) ([]BootstrapJob, error) {
	rows, err := db.Query(ctx, `SELECT portal_id, portal_receiver, status, cursor, source_complete, COALESCE(last_error, '')
		FROM portal_bootstrap WHERE bridge_id=$1 AND user_login_id=$2 AND status NOT IN ('ready', 'reconcile') ORDER BY updated_at`, db.BridgeID, login)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var jobs []BootstrapJob
	for rows.Next() {
		var job BootstrapJob
		job.LoginID = login
		if err = rows.Scan(&job.Portal.ID, &job.Portal.Receiver, &job.Status, &job.Cursor, &job.SourceComplete, &job.LastError); err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}
	return jobs, rows.Err()
}

// EnsureBootstrapJob selects a chat without changing an existing cursor or completion state.
func (db *Database) EnsureBootstrapJob(ctx context.Context, login networkid.UserLoginID, portal networkid.PortalKey) error {
	if login == "" || portal.ID == "" {
		return fmt.Errorf("bootstrap requires a login and portal ID")
	}
	_, err := db.Exec(ctx, `INSERT INTO portal_bootstrap
		(bridge_id, user_login_id, portal_id, portal_receiver, updated_at)
		VALUES ($1,$2,$3,$4,$5) ON CONFLICT (bridge_id,user_login_id,portal_id,portal_receiver) DO NOTHING`,
		db.BridgeID, login, portal.ID, portal.Receiver, time.Now().UnixNano())
	return err
}

func (db *Database) stageBootstrapItem(ctx context.Context, login networkid.UserLoginID, portal networkid.PortalKey, item BootstrapItem) error {
	if item.StableID == "" || item.Kind == "" || item.Version <= 0 || item.Payload == "" {
		return fmt.Errorf("bootstrap item requires an ID, kind, version, and payload")
	}
	var oldKind, oldPayload string
	var oldVersion int
	var oldTS int64
	err := db.QueryRow(ctx, `SELECT kind, version, payload, source_ts FROM portal_bootstrap_item
		WHERE bridge_id=$1 AND user_login_id=$2 AND portal_id=$3 AND portal_receiver=$4 AND stable_id=$5`,
		db.BridgeID, login, portal.ID, portal.Receiver, item.StableID).Scan(&oldKind, &oldVersion, &oldPayload, &oldTS)
	if err == nil {
		if oldKind != item.Kind || oldVersion != item.Version || oldPayload != item.Payload || oldTS != item.SourceTS {
			return fmt.Errorf("bootstrap item %s was received with different content", item.StableID)
		}
		return nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	var order int64
	err = db.QueryRow(ctx, `UPDATE portal_bootstrap SET next_order=next_order+1, updated_at=$5,
		status=CASE WHEN $6='history' AND EXISTS (
			SELECT 1 FROM message m WHERE m.bridge_id=$1 AND m.room_id=$3 AND m.room_receiver=$4
			AND m.timestamp >= $7 AND m.mxid NOT LIKE '~fake:%'
		) THEN 'reconcile' WHEN status='ready' AND $6='history' THEN 'reconcile'
		WHEN status='ready' THEN 'incomplete' ELSE status END,
		source_complete=CASE WHEN status='ready' THEN false ELSE source_complete END
		WHERE bridge_id=$1 AND user_login_id=$2 AND portal_id=$3 AND portal_receiver=$4 RETURNING next_order`,
		db.BridgeID, login, portal.ID, portal.Receiver, time.Now().UnixNano(), item.Kind, time.UnixMilli(item.SourceTS).UnixNano()).Scan(&order)
	if err != nil {
		return err
	}
	_, err = db.Exec(ctx, `INSERT INTO portal_bootstrap_item
		(bridge_id,user_login_id,portal_id,portal_receiver,stable_id,kind,version,payload,source_ts,arrival_order)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
		db.BridgeID, login, portal.ID, portal.Receiver, item.StableID, item.Kind, item.Version, item.Payload, item.SourceTS, order)
	return err
}

// StageBootstrapIncoming must commit before the connector acknowledges a source event.
func (db *Database) StageBootstrapIncoming(ctx context.Context, login networkid.UserLoginID, portal networkid.PortalKey, item BootstrapItem) error {
	return db.DoTxn(ctx, nil, func(txCtx context.Context) error {
		if err := db.EnsureBootstrapJob(txCtx, login, portal); err != nil {
			return err
		}
		return db.stageBootstrapItem(txCtx, login, portal, item)
	})
}

// StageBootstrapPage commits source data and its cursor together; failed pages remain retryable.
func (db *Database) StageBootstrapPage(ctx context.Context, login networkid.UserLoginID, portal networkid.PortalKey, items []BootstrapItem, cursor string, complete bool) error {
	return db.DoTxn(ctx, nil, func(txCtx context.Context) error {
		if err := db.EnsureBootstrapJob(txCtx, login, portal); err != nil {
			return err
		}
		job, err := db.GetBootstrapJob(txCtx, login, portal)
		if err != nil {
			return err
		}
		if !complete && cursor != "" && cursor == job.Cursor {
			return fmt.Errorf("bootstrap page has no advancing cursor")
		}
		for _, item := range items {
			if err = db.stageBootstrapItem(txCtx, login, portal, item); err != nil {
				return err
			}
		}
		_, err = db.Exec(txCtx, `UPDATE portal_bootstrap
			SET status=CASE WHEN status='ready' AND (cursor<>$5 OR source_complete<>$6) THEN 'incomplete' ELSE status END,
			cursor=$5, source_complete=$6, updated_at=$7
			WHERE bridge_id=$1 AND user_login_id=$2 AND portal_id=$3 AND portal_receiver=$4`,
			db.BridgeID, login, portal.ID, portal.Receiver, cursor, complete, time.Now().UnixNano())
		return err
	})
}

func (db *Database) GetPendingBootstrapItems(ctx context.Context, login networkid.UserLoginID, portal networkid.PortalKey, limit int) ([]BootstrapItem, error) {
	return db.GetPendingBootstrapItemsAfter(ctx, login, portal, 0, limit)
}

func (db *Database) GetPendingBootstrapItemsAfter(ctx context.Context, login networkid.UserLoginID, portal networkid.PortalKey, after int64, limit int) ([]BootstrapItem, error) {
	if limit < 1 {
		return nil, fmt.Errorf("bootstrap page size must be positive")
	}
	rows, err := db.Query(ctx, `SELECT stable_id, kind, version, payload, source_ts, arrival_order, delivered
		FROM portal_bootstrap_item WHERE bridge_id=$1 AND user_login_id=$2 AND portal_id=$3 AND portal_receiver=$4
		AND delivered=false AND arrival_order>$5 ORDER BY arrival_order LIMIT $6`, db.BridgeID, login, portal.ID, portal.Receiver, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []BootstrapItem
	for rows.Next() {
		var item BootstrapItem
		if err = rows.Scan(&item.StableID, &item.Kind, &item.Version, &item.Payload, &item.SourceTS, &item.Order, &item.Delivered); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

// GetPendingBootstrapHistory returns oldest source messages first. Source pages must be staged newest-to-oldest.
func (db *Database) GetPendingBootstrapHistory(ctx context.Context, login networkid.UserLoginID, portal networkid.PortalKey, limit int) ([]BootstrapItem, error) {
	if limit < 1 {
		return nil, fmt.Errorf("bootstrap page size must be positive")
	}
	rows, err := db.Query(ctx, `SELECT stable_id, kind, version, payload, source_ts, arrival_order, delivered
		FROM portal_bootstrap_item WHERE bridge_id=$1 AND user_login_id=$2 AND portal_id=$3 AND portal_receiver=$4
		AND kind='history' AND delivered=false ORDER BY source_ts, arrival_order DESC LIMIT $5`,
		db.BridgeID, login, portal.ID, portal.Receiver, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []BootstrapItem
	for rows.Next() {
		var item BootstrapItem
		if err = rows.Scan(&item.StableID, &item.Kind, &item.Version, &item.Payload, &item.SourceTS, &item.Order, &item.Delivered); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

// CheckpointBootstrapParts fixes the expected mapping IDs before sending a live event.
func (db *Database) CheckpointBootstrapParts(ctx context.Context, login networkid.UserLoginID, portal networkid.PortalKey, stableID string, parts []networkid.PartID) error {
	encoded, err := json.Marshal(append([]networkid.PartID{}, parts...))
	if err != nil {
		return err
	}
	result, err := db.Exec(ctx, `UPDATE portal_bootstrap_item SET expected_parts=$6
		WHERE bridge_id=$1 AND user_login_id=$2 AND portal_id=$3 AND portal_receiver=$4 AND stable_id=$5
		AND delivered=false AND (expected_parts IS NULL OR expected_parts=$6)`,
		db.BridgeID, login, portal.ID, portal.Receiver, stableID, string(encoded))
	if err != nil {
		return err
	}
	if count, err := result.RowsAffected(); err != nil {
		return err
	} else if count != 1 {
		return fmt.Errorf("staged item %s has conflicting expected message parts", stableID)
	}
	return nil
}

func (db *Database) GetBootstrapExpectedParts(ctx context.Context, login networkid.UserLoginID, portal networkid.PortalKey, stableID string) ([]networkid.PartID, bool, error) {
	var stored sql.NullString
	err := db.QueryRow(ctx, `SELECT expected_parts FROM portal_bootstrap_item
		WHERE bridge_id=$1 AND user_login_id=$2 AND portal_id=$3 AND portal_receiver=$4 AND stable_id=$5`,
		db.BridgeID, login, portal.ID, portal.Receiver, stableID).Scan(&stored)
	if err != nil || !stored.Valid {
		return nil, false, err
	}
	var parts []networkid.PartID
	if err = json.Unmarshal([]byte(stored.String), &parts); err != nil {
		return nil, false, fmt.Errorf("decode expected message parts for %s: %w", stableID, err)
	}
	return parts, true, nil
}

func (db *Database) MarkBootstrapDelivered(ctx context.Context, login networkid.UserLoginID, portal networkid.PortalKey, stableID string) error {
	result, err := db.Exec(ctx, `UPDATE portal_bootstrap_item SET delivered=true
		WHERE bridge_id=$1 AND user_login_id=$2 AND portal_id=$3 AND portal_receiver=$4 AND stable_id=$5`,
		db.BridgeID, login, portal.ID, portal.Receiver, stableID)
	if err != nil {
		return err
	}
	if count, err := result.RowsAffected(); err != nil {
		return err
	} else if count != 1 {
		return fmt.Errorf("bootstrap item %s not found", stableID)
	}
	return nil
}

func (db *Database) SetBootstrapStatus(ctx context.Context, login networkid.UserLoginID, portal networkid.PortalKey, status, lastError string) error {
	result, err := db.Exec(ctx, `UPDATE portal_bootstrap SET status=$5, last_error=$6, updated_at=$7
		WHERE bridge_id=$1 AND user_login_id=$2 AND portal_id=$3 AND portal_receiver=$4
		AND (status <> 'reconcile' OR $5='reconcile')
		AND ($5 <> 'ready' OR (source_complete=true AND NOT EXISTS (
			SELECT 1 FROM portal_bootstrap_item i WHERE i.bridge_id=$1 AND i.user_login_id=$2 AND i.portal_id=$3
			AND i.portal_receiver=$4 AND i.delivered=false)))`,
		db.BridgeID, login, portal.ID, portal.Receiver, status, lastError, time.Now().UnixNano())
	if err != nil {
		return err
	}
	if count, err := result.RowsAffected(); err != nil {
		return err
	} else if count != 1 {
		return fmt.Errorf("bootstrap %s cannot transition to %s", portal.ID, status)
	}
	return nil
}
