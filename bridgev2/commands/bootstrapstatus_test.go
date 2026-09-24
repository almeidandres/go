package commands

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog"
	"go.mau.fi/util/dbutil"
	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

type bootstrapStatusClientTest struct{ bridgev2.NetworkAPI }

type bootstrapStatusNetworkTest struct{ bridgev2.NetworkConnector }

func (bootstrapStatusNetworkTest) Init(*bridgev2.Bridge)              {}
func (bootstrapStatusNetworkTest) GetDBMetaTypes() database.MetaTypes { return database.MetaTypes{} }
func (bootstrapStatusNetworkTest) LoadUserLogin(_ context.Context, login *bridgev2.UserLogin) error {
	login.Client = bootstrapStatusClientTest{}
	return nil
}

type bootstrapStatusBotTest struct {
	bridgev2.MatrixAPI
	message string
}

func (bot *bootstrapStatusBotTest) SendMessage(_ context.Context, _ id.RoomID, _ event.Type, content *event.Content, _ *bridgev2.MatrixSendExtra) (*mautrix.RespSendEvent, error) {
	bot.message = content.AsMessage().Body
	return &mautrix.RespSendEvent{EventID: "$notice"}, nil
}

type bootstrapStatusMatrixTest struct {
	bridgev2.MatrixConnector
	bot *bootstrapStatusBotTest
}

func (bootstrapStatusMatrixTest) Init(*bridgev2.Bridge)                {}
func (matrix bootstrapStatusMatrixTest) BotIntent() bridgev2.MatrixAPI { return matrix.bot }

func TestBootstrapStatusDoesNotExposeOtherLogin(t *testing.T) {
	ctx := context.Background()
	raw, err := dbutil.NewWithDialect("file:"+filepath.Join(t.TempDir(), "status.db")+"?_foreign_keys=on", "sqlite3")
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	bot := &bootstrapStatusBotTest{}
	matrix := bootstrapStatusMatrixTest{bot: bot}
	bridge := bridgev2.NewBridge("test", raw, zerolog.Nop(), nil, matrix, bootstrapStatusNetworkTest{}, func(*bridgev2.Bridge) bridgev2.CommandProcessor { return nil })
	if err = bridge.DB.Upgrade(ctx); err != nil {
		t.Fatal(err)
	}
	for _, entry := range []struct {
		user  id.UserID
		login networkid.UserLoginID
	}{{"@owner:localhost", "owner"}, {"@other:localhost", "other"}} {
		if err = bridge.DB.User.Insert(ctx, &database.User{MXID: entry.user}); err != nil {
			t.Fatal(err)
		}
		if err = bridge.DB.UserLogin.Insert(ctx, &database.UserLogin{ID: entry.login, UserMXID: entry.user}); err != nil {
			t.Fatal(err)
		}
	}
	owner, err := bridge.GetUserByMXID(ctx, "@owner:localhost")
	if err != nil {
		t.Fatal(err)
	}
	other, err := bridge.GetUserByMXID(ctx, "@other:localhost")
	if err != nil {
		t.Fatal(err)
	}
	for _, login := range append(owner.GetUserLogins(), other.GetUserLogins()...) {
		defer login.BridgeState.Destroy()
	}
	key := networkid.PortalKey{ID: "private-chat", Receiver: "other"}
	if err = bridge.DB.EnsureBootstrapJob(ctx, "other", key); err != nil {
		t.Fatal(err)
	}
	ce := &Event{Bridge: bridge, User: owner, Bot: bot, Ctx: ctx, OrigRoomID: "!management:localhost", Args: []string{"other", "private-chat"}, Log: &bridge.Log}
	fnBootstrapStatus(ce)
	if !strings.Contains(bot.message, "not yours") || strings.Contains(bot.message, "pending") {
		t.Fatalf("status leaked another account's selected chat: %q", bot.message)
	}
	ce.User = other
	fnBootstrapStatus(ce)
	if !strings.Contains(bot.message, "pending") {
		t.Fatalf("owner cannot inspect pending chat without retrying: %q", bot.message)
	}
	job, err := bridge.DB.GetBootstrapJob(ctx, "other", key)
	if err != nil || job.Status != "pending" {
		t.Fatalf("read-only status changed bootstrap job: %+v %v", job, err)
	}
	ce.Args = []string{"other", "private-chat", "!orphan:localhost"}
	CommandBootstrapAdoptRoom.Run(ce)
	if !strings.Contains(bot.message, "limited to bridge administrators") {
		t.Fatalf("non-admin reached room adoption: %q", bot.message)
	}
}
