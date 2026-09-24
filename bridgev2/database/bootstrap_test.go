package database

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"go.mau.fi/util/dbutil"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/id"
)

func TestBootstrapPageAndIncomingRecovery(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "bootstrap.db")
	open := func() *Database {
		t.Helper()
		raw, err := dbutil.NewWithDialect("file:"+path+"?_txlock=immediate&_foreign_keys=on", "sqlite3")
		if err != nil {
			t.Fatal(err)
		}
		db := New("ig", MetaTypes{}, raw)
		if err = db.Upgrade(ctx); err != nil {
			t.Fatal(err)
		}
		return db
	}
	login := networkid.UserLoginID("login")
	portal := networkid.PortalKey{ID: "thread"}
	original := BootstrapItem{StableID: "message-1", Kind: "message", Version: 1, Payload: `{"id":"one"}`, SourceTS: 1}
	db := open()
	if err := db.StageBootstrapIncoming(ctx, login, portal, original); err != nil {
		t.Fatal(err)
	}
	if err := db.StageBootstrapPage(ctx, login, portal, []BootstrapItem{original}, "next", false); err != nil {
		t.Fatal(err)
	}
	if err := db.StageBootstrapPage(ctx, login, portal, nil, "", false); err != nil {
		t.Fatalf("incomplete WhatsApp cache cannot reset its scan cursor: %v", err)
	}
	if err := db.StageBootstrapPage(ctx, login, portal, []BootstrapItem{
		{StableID: "message-2", Kind: "history", Version: 1, Payload: `{"id":"two"}`, SourceTS: 2},
		{StableID: "message-older", Kind: "history", Version: 1, Payload: `{"id":"older"}`, SourceTS: 1},
	}, "", true); err != nil {
		t.Fatal(err)
	}
	if err := db.SetBootstrapStatus(ctx, login, portal, "ready", ""); err == nil {
		t.Fatal("job became ready with undelivered messages")
	}
	if err := db.CheckpointBootstrapParts(ctx, login, portal, original.StableID, []networkid.PartID{"", "caption"}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = open()
	parts, found, err := db.GetBootstrapExpectedParts(ctx, login, portal, original.StableID)
	if err != nil || !found || len(parts) != 2 || parts[1] != "caption" {
		t.Fatalf("message-part checkpoint did not survive restart: %v %v %v", parts, found, err)
	}
	if err = db.CheckpointBootstrapParts(ctx, login, portal, original.StableID, []networkid.PartID{"different"}); err == nil {
		t.Fatal("conflicting message-part retry was accepted")
	}
	job, err := db.GetBootstrapJob(ctx, login, portal)
	if err != nil || job == nil || !job.SourceComplete {
		t.Fatalf("source completion did not survive restart: job=%+v err=%v", job, err)
	}
	if err = db.EnsureBootstrapJob(ctx, login, portal); err != nil {
		t.Fatal(err)
	}
	job, err = db.GetBootstrapJob(ctx, login, portal)
	if err != nil || job == nil || !job.SourceComplete {
		t.Fatalf("reselecting chat reset its completed source: job=%+v err=%v", job, err)
	}
	items, err := db.GetPendingBootstrapItems(ctx, login, portal, 10)
	if err != nil || len(items) != 3 || items[0].StableID != "message-1" || items[1].StableID != "message-2" || items[2].StableID != "message-older" {
		t.Fatalf("unexpected recovered items: %+v err=%v", items, err)
	}
	history, err := db.GetPendingBootstrapHistory(ctx, login, portal, 10)
	if err != nil || len(history) != 2 || history[0].StableID != "message-older" || history[1].StableID != "message-2" {
		t.Fatalf("history not oldest-first: %+v err=%v", history, err)
	}
	conflict := original
	conflict.Payload = `{"id":"different"}`
	if err = db.StageBootstrapIncoming(ctx, login, portal, conflict); err == nil {
		t.Fatal("conflicting incoming retry was acknowledged")
	}
	if err = db.StageBootstrapPage(ctx, login, portal, []BootstrapItem{conflict}, "changed", false); err == nil {
		t.Fatal("conflicting item advanced cursor")
	}
	job, err = db.GetBootstrapJob(ctx, login, portal)
	if err != nil || job.Cursor != "" || !job.SourceComplete {
		t.Fatalf("failed page changed completed job: %+v err=%v", job, err)
	}
	for _, item := range items {
		if err = db.MarkBootstrapDelivered(ctx, login, portal, item.StableID); err != nil {
			t.Fatal(err)
		}
	}
	if err = db.SetBootstrapStatus(ctx, login, portal, "ready", ""); err != nil {
		t.Fatal(err)
	}
	late := BootstrapItem{StableID: "message-3", Kind: "message", Version: 1, Payload: `{"id":"late"}`, SourceTS: 3}
	if err = db.StageBootstrapIncoming(ctx, login, portal, late); err != nil {
		t.Fatal(err)
	}
	job, err = db.GetBootstrapJob(ctx, login, portal)
	if err != nil || job.Status != "incomplete" || job.SourceComplete {
		t.Fatalf("late incoming item remained ready: %+v err=%v", job, err)
	}
	if err = db.MarkBootstrapDelivered(ctx, login, portal, late.StableID); err != nil {
		t.Fatal(err)
	}
	if err = db.StageBootstrapPage(ctx, login, portal, nil, "", true); err != nil {
		t.Fatal(err)
	}
	if err = db.SetBootstrapStatus(ctx, login, portal, "ready", ""); err != nil {
		t.Fatal(err)
	}
	if err = db.StageBootstrapPage(ctx, login, portal, []BootstrapItem{{
		StableID: "history-late", Kind: "history", Version: 1, Payload: `{"id":"older"}`, SourceTS: 0,
	}}, "", false); err != nil {
		t.Fatal(err)
	}
	job, err = db.GetBootstrapJob(ctx, login, portal)
	if err != nil || job.Status != "reconcile" || job.SourceComplete {
		t.Fatalf("late older history was silently published: %+v err=%v", job, err)
	}
	jobs, err := db.GetUnfinishedBootstrapJobs(ctx, login)
	if err != nil || len(jobs) != 0 {
		t.Fatalf("late older history was scheduled for automatic import: %+v err=%v", jobs, err)
	}
}

func TestBootstrapOlderHistoryAfterInterruptedRoom(t *testing.T) {
	ctx := context.Background()
	raw, err := dbutil.NewWithDialect("file:"+filepath.Join(t.TempDir(), "late.db")+"?_foreign_keys=on", "sqlite3")
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	db := New("ig", MetaTypes{}, raw)
	if err = db.Upgrade(ctx); err != nil {
		t.Fatal(err)
	}
	login := networkid.UserLoginID("login")
	key := networkid.PortalKey{ID: "thread"}
	if err = db.Portal.Insert(ctx, &Portal{PortalKey: key, MXID: "!room:localhost"}); err != nil {
		t.Fatal(err)
	}
	if err = db.Ghost.Insert(ctx, &Ghost{ID: "sender"}); err != nil {
		t.Fatal(err)
	}
	if err = db.StageBootstrapPage(ctx, login, key, []BootstrapItem{{StableID: "newer", Kind: "history", Version: 1, Payload: `{"id":"newer"}`, SourceTS: 2}}, "", true); err != nil {
		t.Fatal(err)
	}
	if err = db.Message.Insert(ctx, &Message{Room: key, ID: "newer", MXID: "$newer", SenderID: "sender", SenderMXID: id.UserID("@sender:localhost"), Timestamp: time.UnixMilli(2)}); err != nil {
		t.Fatal(err)
	}
	if err = db.MarkBootstrapDelivered(ctx, login, key, "newer"); err != nil {
		t.Fatal(err)
	}
	if err = db.SetBootstrapStatus(ctx, login, key, "incomplete", "interrupted incoming replay"); err != nil {
		t.Fatal(err)
	}
	if err = db.StageBootstrapPage(ctx, login, key, []BootstrapItem{{StableID: "older", Kind: "history", Version: 1, Payload: `{"id":"older"}`, SourceTS: 1}}, "", true); err != nil {
		t.Fatal(err)
	}
	job, err := db.GetBootstrapJob(ctx, login, key)
	if err != nil || job.Status != "reconcile" {
		t.Fatalf("older history was scheduled after mapped history: %+v %v", job, err)
	}
	jobs, err := db.GetUnfinishedBootstrapJobs(ctx, login)
	if err != nil || len(jobs) != 0 {
		t.Fatalf("out-of-order history remained runnable: %+v %v", jobs, err)
	}
	if err = db.SetBootstrapStatus(ctx, login, key, "importing", "automatic retry"); err == nil {
		t.Fatal("automatic retry cleared the reconciliation barrier")
	}
	job, err = db.GetBootstrapJob(ctx, login, key)
	if err != nil || job.Status != "reconcile" {
		t.Fatalf("reconciliation barrier did not persist: %+v %v", job, err)
	}
}
