package bridgev2

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"go.mau.fi/util/dbutil"

	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/id"
)

func TestBatchCheckpointKeepsBodyAndMetadata(t *testing.T) {
	type metadata struct{ Value int }
	body := []byte(`{"events":[{"content":{"ciphertext":"encrypted"}}]}`)
	saved, err := makeBatchCheckpointData(body, &compileBatchOutput{
		DBMessages:  []*database.Message{{Metadata: &metadata{Value: 42}}},
		DBReactions: []*database.Reaction{{Metadata: &metadata{Value: 17}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	var data batchCheckpointData
	if err = json.Unmarshal(saved, &data); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(body, data.Body) {
		t.Fatal("request bytes changed")
	}
	msg := &database.Message{Metadata: &metadata{}}
	reaction := &database.Reaction{Metadata: &metadata{}}
	if err = json.Unmarshal(data.Messages[0], msg); err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(data.Reactions[0], reaction); err != nil {
		t.Fatal(err)
	}
	if msg.Metadata.(*metadata).Value != 42 || reaction.Metadata.(*metadata).Value != 17 {
		t.Fatal("mapping metadata changed")
	}
}

type checkpointSenderTest struct {
	MatrixConnector
	sent [][]byte
}

func (s *checkpointSenderTest) PrepareBatchSend(context.Context, id.RoomID, *mautrix.ReqBeeperBatchSend) ([]byte, error) {
	return nil, fmt.Errorf("checkpoint must already be prepared")
}

func (s *checkpointSenderTest) SendPreparedBatch(_ context.Context, _ id.RoomID, body []byte) (*mautrix.RespBeeperBatchSend, error) {
	s.sent = append(s.sent, bytes.Clone(body))
	return &mautrix.RespBeeperBatchSend{}, nil
}

func TestBatchCheckpointReplaysAfterMappingFailure(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "batch.db")
	open := func() *database.Database {
		t.Helper()
		raw, err := dbutil.NewWithDialect("file:"+path+"?_txlock=immediate&_foreign_keys=on", "sqlite3")
		if err != nil {
			t.Fatal(err)
		}
		db := database.New("wa", database.MetaTypes{}, raw)
		if err = db.Upgrade(ctx); err != nil {
			t.Fatal(err)
		}
		return db
	}
	db := open()
	portalKey := networkid.PortalKey{ID: "chat"}
	roomID := id.RoomID("!room:example.com")
	if err := db.Portal.Insert(ctx, &database.Portal{PortalKey: portalKey, MXID: roomID}); err != nil {
		t.Fatal(err)
	}
	if err := db.Ghost.Insert(ctx, &database.Ghost{ID: "sender"}); err != nil {
		t.Fatal(err)
	}
	body := []byte(`{"events":[{"content":{"ciphertext":"first-and-only"}}]}`)
	msg := &database.Message{Room: portalKey, ID: "remote-message", PartID: "", MXID: "$event:example.com", SenderID: "sender", SenderMXID: "@sender:example.com", Timestamp: time.Now()}
	data, err := makeBatchCheckpointData(body, &compileBatchOutput{DBMessages: []*database.Message{msg}})
	if err != nil {
		t.Fatal(err)
	}
	cp := &database.BatchCheckpoint{Portal: portalKey, RoomID: roomID, Data: data}
	if err = db.InsertBatchCheckpoint(ctx, cp); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(ctx, `CREATE TRIGGER fail_mapping BEFORE INSERT ON message BEGIN SELECT RAISE(FAIL, 'simulated mapping failure'); END`); err != nil {
		t.Fatal(err)
	}
	sender := &checkpointSenderTest{}
	bridge := &Bridge{DB: db, Matrix: sender}
	if err = bridge.replayBatchCheckpoint(ctx, cp); err == nil {
		t.Fatal("mapping failure incorrectly cleared checkpoint")
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	db = open()
	if _, err = db.Exec(ctx, `DROP TRIGGER fail_mapping`); err != nil {
		t.Fatal(err)
	}
	cp, err = db.GetBatchCheckpoint(ctx, portalKey)
	if err != nil || cp == nil {
		t.Fatalf("checkpoint lost after restart: %+v %v", cp, err)
	}
	bridge.DB = db
	if err = bridge.replayBatchCheckpoint(ctx, cp); err != nil {
		t.Fatal(err)
	}
	if len(sender.sent) != 2 || !bytes.Equal(sender.sent[0], body) || !bytes.Equal(sender.sent[0], sender.sent[1]) {
		t.Fatalf("retry changed encrypted batch: %q", sender.sent)
	}
	if cp, err = db.GetBatchCheckpoint(ctx, portalKey); err != nil || cp != nil {
		t.Fatalf("checkpoint not cleared after mapping: %+v %v", cp, err)
	}
	mapped, err := db.Message.GetPartByID(ctx, portalKey.Receiver, msg.ID, msg.PartID)
	if err != nil || mapped == nil || mapped.MXID != msg.MXID {
		t.Fatalf("message mapping missing after replay: %+v %v", mapped, err)
	}
}
