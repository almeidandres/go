package bridgev2

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog"
	"go.mau.fi/util/dbutil"

	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/bridgev2/bridgeconfig"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

type gatedSourceTest struct {
	NetworkAPI
	replay func(database.BootstrapItem) error
}

func (gatedSourceTest) StageBootstrapHistory(context.Context, *Portal) error { return nil }
func (gatedSourceTest) ConvertBootstrapHistory(context.Context, *Portal, database.BootstrapItem) (*BackfillMessage, error) {
	return nil, nil
}
func (g gatedSourceTest) ReplayBootstrapIncoming(_ context.Context, _ *Portal, item database.BootstrapItem) error {
	if g.replay != nil {
		return g.replay(item)
	}
	return nil
}

type bootstrapMatrixTest struct{ MatrixConnector }

func (bootstrapMatrixTest) GetStateEvent(_ context.Context, _ id.RoomID, evtType event.Type, _ string) (*event.Event, error) {
	if evtType == event.StateMember {
		return &event.Event{Content: event.Content{Parsed: &event.MemberEventContent{Membership: event.MembershipJoin}}}, nil
	}
	return &event.Event{Content: event.Content{Parsed: &event.EncryptionEventContent{Algorithm: id.AlgorithmMegolmV1}}}, nil
}

type unencryptedBootstrapMatrixTest struct{ bootstrapMatrixTest }

func (unencryptedBootstrapMatrixTest) GetStateEvent(_ context.Context, _ id.RoomID, evtType event.Type, _ string) (*event.Event, error) {
	if evtType == event.StateMember {
		return &event.Event{Content: event.Content{Parsed: &event.MemberEventContent{Membership: event.MembershipJoin}}}, nil
	}
	return nil, nil
}

type chatResyncGateTest struct {
	RemoteEvent
	key networkid.PortalKey
}

func (e chatResyncGateTest) GetType() RemoteEventType          { return RemoteEventChatResync }
func (e chatResyncGateTest) GetPortalKey() networkid.PortalKey { return e.key }

type partialBootstrapIntentTest struct {
	MatrixAPI
	sends []networkid.PartID
	fail  bool
}

func (intent *partialBootstrapIntentTest) GetMXID() id.UserID   { return "@ghost:localhost" }
func (intent *partialBootstrapIntentTest) IsDoublePuppet() bool { return false }
func (intent *partialBootstrapIntentTest) SendMessage(_ context.Context, _ id.RoomID, _ event.Type, _ *event.Content, extra *MatrixSendExtra) (*mautrix.RespSendEvent, error) {
	intent.sends = append(intent.sends, extra.MessageMeta.PartID)
	if extra.MessageMeta.PartID == "caption" && intent.fail {
		return nil, errors.New("simulated second-part send failure")
	}
	return &mautrix.RespSendEvent{EventID: id.EventID("$" + string(extra.MessageMeta.PartID))}, nil
}

func TestBootstrapPartialMessageRetry(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "parts.db")
	open := func() *database.Database {
		t.Helper()
		raw, err := dbutil.NewWithDialect("file:"+path+"?_foreign_keys=on", "sqlite3")
		if err != nil {
			t.Fatal(err)
		}
		db := database.New("ig", database.MetaTypes{}, raw)
		if err := db.Upgrade(ctx); err != nil {
			t.Fatal(err)
		}
		return db
	}
	db := open()
	defer func() { db.Close() }()
	var err error
	key := networkid.PortalKey{ID: "thread"}
	login := &UserLogin{UserLogin: &database.UserLogin{ID: "login"}}
	portal := &Portal{Portal: &database.Portal{MXID: "!room:localhost", PortalKey: key}, Bridge: &Bridge{DB: db}}
	if err = db.Portal.Insert(ctx, portal.Portal); err != nil {
		t.Fatal(err)
	}
	if err = db.Ghost.Insert(ctx, &database.Ghost{ID: "sender"}); err != nil {
		t.Fatal(err)
	}
	if err = db.StageBootstrapIncoming(ctx, login.ID, key, database.BootstrapItem{StableID: "incoming", Kind: "incoming", Version: 1, Payload: "payload"}); err != nil {
		t.Fatal(err)
	}
	converted := &ConvertedMessage{Parts: []*ConvertedMessagePart{
		{ID: "body", Type: event.EventMessage, Content: &event.MessageEventContent{MsgType: event.MsgText, Body: "body"}},
		{ID: "caption", Type: event.EventMessage, Content: &event.MessageEventContent{MsgType: event.MsgText, Body: "caption"}},
	}}
	intent := &partialBootstrapIntentTest{fail: true}
	replay := func() EventHandlingResult {
		replayCtx := context.WithValue(ctx, bootstrapTransactionKey{}, &bootstrapTransactionState{prefix: "stable", stableID: "incoming"})
		if err := portal.checkpointBootstrapMessage(replayCtx, login, converted); err != nil {
			t.Fatal(err)
		}
		_, result := portal.sendConvertedMessage(replayCtx, login, "message", intent, "sender", converted, time.Unix(1, 0), 0, nil)
		return result
	}
	if result := replay(); result.Success {
		t.Fatal("partial first delivery was acknowledged")
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	db = open()
	portal.Bridge.DB = db
	intent.fail = false
	if result := replay(); !result.Success {
		t.Fatalf("partial message did not resume: %v", result.Error)
	}
	if len(intent.sends) != 3 || intent.sends[0] != "body" || intent.sends[1] != "caption" || intent.sends[2] != "caption" {
		t.Fatalf("retry resent mapped message part: %v", intent.sends)
	}
	mapped, err := db.Message.GetAllPartsByID(ctx, key.Receiver, "message")
	if err != nil || len(mapped) != 2 {
		t.Fatalf("partial message lost a mapping: %+v %v", mapped, err)
	}
}

func TestBootstrapTransactionIDStableAcrossRetry(t *testing.T) {
	prefix := "bridge/login/thread/room/incoming-message"
	first := context.WithValue(context.Background(), bootstrapTransactionKey{}, &bootstrapTransactionState{prefix: prefix})
	retry := context.WithValue(context.Background(), bootstrapTransactionKey{}, &bootstrapTransactionState{prefix: prefix})
	if got := BootstrapTransactionID(context.Background()); got != "" {
		t.Fatalf("normal event unexpectedly has bootstrap transaction ID %q", got)
	}
	firstPart := BootstrapTransactionID(first)
	secondPart := BootstrapTransactionID(first)
	if firstPart == "" || firstPart == secondPart || firstPart != BootstrapTransactionID(retry) || secondPart != BootstrapTransactionID(retry) {
		t.Fatal("incoming replay cannot retry multiple sends with stable distinct transaction IDs")
	}
	partID := BootstrapTransactionIDForPart(first, "message:second")
	if partID == firstPart || partID == secondPart || partID != BootstrapTransactionIDForPart(retry, "message:second") {
		t.Fatal("retrying a partially mapped message changed its part transaction ID")
	}
}

func TestBootstrapIncomingReplayRecoversAfterRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "replay.db")
	open := func() *database.Database {
		t.Helper()
		raw, err := dbutil.NewWithDialect("file:"+path+"?_foreign_keys=on", "sqlite3")
		if err != nil {
			t.Fatal(err)
		}
		db := database.New("ig", database.MetaTypes{}, raw)
		if err := db.Upgrade(ctx); err != nil {
			t.Fatal(err)
		}
		return db
	}
	key := networkid.PortalKey{ID: "thread"}
	loginID := networkid.UserLoginID("login")
	db := open()
	for _, stableID := range []string{"receipt", "one", "two"} {
		if err := db.StageBootstrapIncoming(ctx, loginID, key, database.BootstrapItem{StableID: stableID, Kind: "incoming", Version: 1, Payload: stableID}); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.StageBootstrapPage(ctx, loginID, key, nil, "", true); err != nil {
		t.Fatal(err)
	}
	portal := &Portal{Portal: &database.Portal{MXID: "!room:localhost", PortalKey: key}, Bridge: &Bridge{DB: db, Matrix: bootstrapMatrixTest{}}, Log: zerolog.Nop()}
	failed := false
	source := gatedSourceTest{replay: func(item database.BootstrapItem) error {
		if item.StableID == "receipt" {
			return errBootstrapReceiptTargetMissing
		}
		if item.StableID == "two" && !failed {
			failed = true
			return errors.New("send failed")
		}
		return nil
	}}
	login := &UserLogin{UserLogin: &database.UserLogin{ID: loginID, UserMXID: "@owner:localhost"}, Client: source}
	portal.Bridge.Matrix = unencryptedBootstrapMatrixTest{}
	if err := portal.finishBootstrapInLoop(ctx, login); err == nil || failed {
		t.Fatal("unencrypted room imported incoming events")
	}
	portal.Bridge.Matrix = bootstrapMatrixTest{}
	if err := portal.finishBootstrapInLoop(ctx, login); err == nil {
		t.Fatal("failed replay was acknowledged")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = open()
	defer db.Close()
	portal.Bridge.DB = db
	var replayed []string
	var secondMessageMapped bool
	login.Client = gatedSourceTest{replay: func(item database.BootstrapItem) error {
		if item.StableID == "receipt" && !secondMessageMapped {
			return errBootstrapReceiptTargetMissing
		}
		if item.StableID == "two" {
			secondMessageMapped = true
		}
		replayed = append(replayed, item.StableID)
		return nil
	}}
	if err := portal.finishBootstrapInLoop(ctx, login); err != nil {
		t.Fatal(err)
	}
	if len(replayed) != 2 || replayed[0] != "two" || replayed[1] != "receipt" {
		t.Fatalf("restart replayed wrong items or dropped an early receipt: %v", replayed)
	}
	job, err := db.GetBootstrapJob(ctx, loginID, key)
	if err != nil || job == nil || job.Status != "ready" {
		t.Fatalf("recovered portal did not become ready: %+v %v", job, err)
	}
	if err := db.StageBootstrapPage(ctx, loginID, key, []database.BootstrapItem{{
		StableID: "older-history", Kind: "history", Version: 1, Payload: "older", SourceTS: 1,
	}}, "", false); err != nil {
		t.Fatal(err)
	}
	if err := portal.finishBootstrapInLoop(ctx, login); !errors.Is(err, ErrBootstrapNeedsReconciliation) {
		t.Fatalf("occupied room imported late older history automatically: %v", err)
	}
}

func TestUnselectedResyncCannotCreatePortal(t *testing.T) {
	ctx := context.Background()
	raw, err := dbutil.NewWithDialect("file:"+filepath.Join(t.TempDir(), "gate.db")+"?_foreign_keys=on", "sqlite3")
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	db := database.New("ig", database.MetaTypes{}, raw)
	if err = db.Upgrade(ctx); err != nil {
		t.Fatal(err)
	}
	bridge := &Bridge{ID: "ig", DB: db, Config: &bridgeconfig.BridgeConfig{}, BackgroundCtx: ctx}
	login := &UserLogin{UserLogin: &database.UserLogin{ID: "login"}, Bridge: bridge, Client: gatedSourceTest{}, Log: zerolog.Nop()}
	key := networkid.PortalKey{ID: "unselected", Receiver: "login"}
	res := bridge.QueueRemoteEvent(login, chatResyncGateTest{key: key})
	if !res.Ignored || !res.Success {
		t.Fatalf("unselected resync was not ignored: %+v", res)
	}
	portal, err := db.Portal.GetByKey(ctx, key)
	if err != nil || portal != nil {
		t.Fatalf("login resync created a portal record: %+v %v", portal, err)
	}
}
