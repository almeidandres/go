package bridgev2

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

var (
	ErrBootstrapPending              = errors.New("selected chat import is pending")
	ErrBootstrapNotSelected          = errors.New("chat was not selected for room creation")
	ErrBootstrapNeedsReconciliation  = errors.New("portal needs explicit reconciliation before import")
	errBootstrapReceiptTargetMissing = errors.New("staged receipt has no imported target")
)

// PortalBootstrapSource stages history and replays incoming events on the portal's event loop.
type PortalBootstrapSource interface {
	StageBootstrapHistory(context.Context, *Portal) error
	ConvertBootstrapHistory(context.Context, *Portal, database.BootstrapItem) (*BackfillMessage, error)
	ReplayBootstrapIncoming(context.Context, *Portal, database.BootstrapItem) error
}

// ResumeBootstrapJobs retries durable selections after a login connects or stages a new event.
func (source *UserLogin) ResumeBootstrapJobs() {
	if source == nil || source.Client == nil || source.Bridge == nil {
		return
	}
	if _, ok := source.Client.(PortalBootstrapSource); !ok {
		return
	}
	source.bootstrapResumeAgain.Store(true)
	if !source.bootstrapResumeRunning.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer func() {
			source.bootstrapResumeRunning.Store(false)
			if source.bootstrapResumeAgain.Load() {
				source.ResumeBootstrapJobs()
			}
		}()
		ctx := source.Log.WithContext(source.Bridge.BackgroundCtx)
		for source.bootstrapResumeAgain.Swap(false) && ctx.Err() == nil {
			jobs, err := source.Bridge.DB.GetUnfinishedBootstrapJobs(ctx, source.ID)
			if err != nil {
				source.Log.Err(err).Msg("Failed to load unfinished portal imports")
				return
			}
			for _, job := range jobs {
				portal, err := source.Bridge.GetPortalByKey(ctx, job.Portal)
				if err == nil {
					err = portal.CreateMatrixRoom(ctx, source, nil)
				}
				if err != nil && !errors.Is(err, ErrBootstrapPending) {
					source.Log.Err(err).Stringer("portal", job.Portal).Msg("Failed to resume selected portal")
				}
			}
		}
	}()
}

type bootstrapTransactionKey struct{}

type bootstrapTransactionState struct {
	prefix   string
	stableID string
	sequence atomic.Uint64
}

// BootstrapTransactionID gives retried incoming sends the same Matrix transaction IDs.
func BootstrapTransactionID(ctx context.Context) string {
	return BootstrapTransactionIDForPart(ctx, "")
}

// BootstrapTransactionIDForPart remains stable when earlier parts were already mapped.
func BootstrapTransactionIDForPart(ctx context.Context, part string) string {
	state, ok := ctx.Value(bootstrapTransactionKey{}).(*bootstrapTransactionState)
	if !ok {
		return ""
	}
	if part == "" {
		part = fmt.Sprintf("send:%d", state.sequence.Add(1))
	}
	digest := sha256.Sum256([]byte(fmt.Sprintf("%q/%q", state.prefix, part)))
	return "bootstrap_" + hex.EncodeToString(digest[:16])
}

func (portal *Portal) checkpointBootstrapMessage(ctx context.Context, source *UserLogin, converted *ConvertedMessage) error {
	state, ok := ctx.Value(bootstrapTransactionKey{}).(*bootstrapTransactionState)
	if !ok {
		return nil
	}
	if converted == nil {
		return fmt.Errorf("staged message has no converted parts")
	}
	parts := make([]networkid.PartID, len(converted.Parts))
	for i, part := range converted.Parts {
		if part == nil {
			return fmt.Errorf("staged message has an empty part")
		}
		parts[i] = part.ID
	}
	return portal.Bridge.DB.CheckpointBootstrapParts(ctx, source.ID, portal.PortalKey, state.stableID, parts)
}

// HandleBootstrapEvent handles a staged event directly on the portal's creation loop.
func (portal *Portal) HandleBootstrapEvent(ctx context.Context, source *UserLogin, evt RemoteEvent) error {
	if portal.MXID == "" || source == nil || evt.GetPortalKey() != portal.PortalKey {
		return fmt.Errorf("bootstrap event has no matching Matrix room or source")
	}
	if evt.GetType() == RemoteEventChatDelete {
		return fmt.Errorf("cannot delete a portal during room creation")
	}
	if evt.GetType() == RemoteEventReadReceipt || evt.GetType() == RemoteEventDeliveryReceipt {
		receipt, ok := evt.(interface{ GetReceiptTargets() []networkid.MessageID })
		if !ok {
			return fmt.Errorf("staged receipt has no target provider")
		}
		targets := receipt.GetReceiptTargets()
		mappedTarget := false
		for _, target := range targets {
			mapped, err := portal.Bridge.DB.Message.GetAllPartsByID(ctx, portal.Receiver, target)
			if err != nil {
				return err
			}
			if len(mapped) > 0 {
				mappedTarget = true
				break
			}
		}
		if len(targets) > 0 && !mappedTarget {
			return errBootstrapReceiptTargetMissing
		}
	}
	result := portal.handleRemoteEvent(ctx, source, evt.GetType(), evt)
	if !result.Success || result.Queued {
		if result.Error != nil {
			return result.Error
		}
		return fmt.Errorf("bootstrap event did not complete synchronously")
	}
	return nil
}

// ReplayBootstrapIncoming confirms each event only after synchronous handling finishes.
func (portal *Portal) ReplayBootstrapIncoming(ctx context.Context, source *UserLogin) error {
	return portal.replayBootstrapIncoming(ctx, source, false)
}

func (portal *Portal) replayBootstrapIncoming(ctx context.Context, source *UserLogin, quarantineHistory bool) error {
	if portal.MXID == "" || source == nil || source.Client == nil {
		return fmt.Errorf("bootstrap replay requires a Matrix room and source login")
	}
	replayer, ok := source.Client.(PortalBootstrapSource)
	if !ok {
		return fmt.Errorf("source login cannot replay incoming bootstrap events")
	}
	for {
		var after int64
		var deferred, progressed bool
		for {
			items, err := portal.Bridge.DB.GetPendingBootstrapItemsAfter(ctx, source.ID, portal.PortalKey, after, 100)
			if err != nil {
				return err
			}
			if len(items) == 0 {
				break
			}
			for _, item := range items {
				after = item.Order
				if item.Kind == "history" {
					if quarantineHistory {
						continue
					}
					return fmt.Errorf("history item %s was not imported before incoming replay", item.StableID)
				}
				prefix := fmt.Sprintf("%q/%q/%q/%q/%q/%q", portal.Bridge.ID, source.ID, portal.ID, portal.Receiver, portal.MXID, item.StableID)
				replayCtx := context.WithValue(ctx, bootstrapTransactionKey{}, &bootstrapTransactionState{prefix: prefix, stableID: item.StableID})
				if err := replayer.ReplayBootstrapIncoming(replayCtx, portal, item); errors.Is(err, errBootstrapReceiptTargetMissing) {
					deferred = true
					continue
				} else if err != nil {
					return fmt.Errorf("replay incoming item %s: %w", item.StableID, err)
				}
				if err := portal.Bridge.DB.MarkBootstrapDelivered(ctx, source.ID, portal.PortalKey, item.StableID); err != nil {
					return fmt.Errorf("confirm incoming item %s: %w", item.StableID, err)
				}
				progressed = true
			}
		}
		if !deferred {
			return nil
		}
		if !progressed {
			return errBootstrapReceiptTargetMissing
		}
	}
}

func (portal *Portal) verifyBootstrapRoomState(ctx context.Context, source *UserLogin, roomID id.RoomID) error {
	stateReader, ok := portal.Bridge.Matrix.(MatrixConnectorWithArbitraryRoomState)
	if !ok {
		return fmt.Errorf("encrypted portal import requires Matrix room-state access")
	}
	for _, userID := range []id.UserID{portal.Bridge.Bot.GetMXID(), source.UserMXID} {
		state, err := stateReader.GetStateEvent(ctx, roomID, event.StateMember, userID.String())
		if err != nil {
			return fmt.Errorf("verify portal member %s: %w", userID, err)
		}
		if state == nil || state.Content.AsMember().Membership != event.MembershipJoin {
			return fmt.Errorf("portal member %s is not joined; encrypted history remains pending", userID)
		}
	}
	state, err := stateReader.GetStateEvent(ctx, roomID, event.StateEncryption, "")
	if err != nil {
		return fmt.Errorf("verify encrypted portal state: %w", err)
	}
	if state == nil {
		return fmt.Errorf("portal encryption is missing; history remains pending")
	}
	content, ok := state.Content.Parsed.(*event.EncryptionEventContent)
	if !ok || content.Algorithm != id.AlgorithmMegolmV1 {
		return fmt.Errorf("portal encryption is unsupported; history remains pending")
	}
	return nil
}

// AdoptBootstrapRoom binds a verified orphan room after uncertain creation.
// Late-history reconciliation is never eligible for adoption.
func (portal *Portal) AdoptBootstrapRoom(ctx context.Context, source *UserLogin, roomID id.RoomID) error {
	if source == nil || source.UserLogin == nil || source.UserMXID == "" || roomID == "" {
		return fmt.Errorf("room adoption requires a source login and Matrix room ID")
	}
	if _, ok := source.Client.(PortalBootstrapSource); !ok {
		return fmt.Errorf("source login cannot import portal history")
	}
	portal.roomCreateLock.Lock()
	defer portal.roomCreateLock.Unlock()
	if portal.MXID != "" {
		return fmt.Errorf("portal already has a Matrix room")
	}
	stateReader, ok := portal.Bridge.Matrix.(MatrixConnectorWithArbitraryRoomState)
	if !ok {
		return fmt.Errorf("room adoption requires Matrix room-state access")
	}
	botID := portal.Bridge.Bot.GetMXID()
	create, err := stateReader.GetStateEvent(ctx, roomID, event.StateCreate, "")
	if err != nil {
		return fmt.Errorf("verify Matrix room creator: %w", err)
	}
	if create == nil || create.Sender != botID {
		return fmt.Errorf("Matrix room was not created by this bridge bot")
	}
	stateKey, expected := portal.getBridgeInfo()
	if expected.Protocol.ID == "" {
		return fmt.Errorf("bridge protocol has no identity")
	}
	state, err := stateReader.GetStateEvent(ctx, roomID, event.StateBridge, stateKey)
	if err != nil {
		return fmt.Errorf("verify Matrix bridge identity: %w", err)
	}
	if state == nil {
		return fmt.Errorf("Matrix room has no bridge identity")
	}
	info, ok := state.Content.Parsed.(*event.BridgeEventContent)
	if !ok || info.BridgeBot != botID || info.Protocol.ID != expected.Protocol.ID ||
		info.Channel.ID != expected.Channel.ID || info.Channel.Receiver != expected.Channel.Receiver {
		return fmt.Errorf("Matrix room belongs to a different bridge or chat")
	}
	if err := portal.verifyBootstrapRoomState(ctx, source, roomID); err != nil {
		return err
	}
	if err := portal.Bridge.DB.AdoptBootstrapRoom(ctx, source.ID, portal.PortalKey, roomID); err != nil {
		return err
	}
	portal.MXID = roomID
	portal.Bridge.cacheLock.Lock()
	portal.Bridge.portalsByMXID[roomID] = portal
	portal.Bridge.cacheLock.Unlock()
	portal.RoomCreated.Set()
	portal.updateLogger()
	return nil
}

// finishBootstrapInLoop resumes an occupied room after a crash or a failed import.
func (portal *Portal) finishBootstrapInLoop(ctx context.Context, source *UserLogin) (retErr error) {
	history, ok := source.Client.(PortalBootstrapSource)
	if !ok || portal.MXID == "" {
		return fmt.Errorf("bootstrap requires a source and an existing Matrix room")
	}
	job, err := portal.Bridge.DB.GetBootstrapJob(ctx, source.ID, portal.PortalKey)
	if err != nil {
		return err
	}
	if job == nil {
		return ErrBootstrapNotSelected
	}
	if job.Status == "ready" {
		return nil
	}
	if job.PublishedCached {
		if job.Status != "incomplete" && job.Status != "reconcile" {
			return fmt.Errorf("published cached portal has invalid status %s", job.Status)
		}
		if err := portal.verifyBootstrapRoomState(ctx, source, portal.MXID); err != nil {
			return err
		}
		return portal.replayBootstrapIncoming(ctx, source, true)
	}
	if job.Status == "reconcile" {
		return ErrBootstrapNeedsReconciliation
	}
	defer func() {
		if retErr != nil && !errors.Is(retErr, ErrBootstrapPending) && !errors.Is(retErr, ErrBootstrapNeedsReconciliation) {
			statusCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancel()
			if statusErr := portal.Bridge.DB.SetBootstrapStatus(statusCtx, source.ID, portal.PortalKey, "incomplete", retErr.Error()); statusErr != nil {
				portal.Log.Err(statusErr).Msg("Failed to record incomplete portal import")
			}
		}
	}()
	if !job.SourceComplete && !job.PublishRequested {
		if err := history.StageBootstrapHistory(ctx, portal); err != nil {
			return fmt.Errorf("stage selected chat history: %w", err)
		}
		job, err = portal.Bridge.DB.GetBootstrapJob(ctx, source.ID, portal.PortalKey)
		if err != nil {
			return err
		}
		if job == nil || !job.SourceComplete {
			return ErrBootstrapPending
		}
		if job.Status == "reconcile" {
			return ErrBootstrapNeedsReconciliation
		}
	}
	if err := portal.Bridge.DB.SetBootstrapStatus(ctx, source.ID, portal.PortalKey, "importing", ""); err != nil {
		return err
	}
	if err := portal.verifyBootstrapRoomState(ctx, source, portal.MXID); err != nil {
		return err
	}
	if err := portal.ImportBootstrapHistory(ctx, source); err != nil {
		return err
	}
	if err := portal.ReplayBootstrapIncoming(ctx, source); err != nil {
		return err
	}
	if !job.SourceComplete {
		return portal.Bridge.DB.MarkCachedPublished(ctx, source.ID, portal.PortalKey)
	}
	if err := portal.Bridge.DB.SetBootstrapStatus(ctx, source.ID, portal.PortalKey, "ready", ""); err != nil {
		return fmt.Errorf("confirm imported portal ready: %w", err)
	}
	return nil
}

// ImportBootstrapHistory retries each exact batch until both events and message mappings are durable.
// It does not release the portal or advance incoming-event acknowledgements.
func (portal *Portal) ImportBootstrapHistory(ctx context.Context, source *UserLogin) error {
	if portal.MXID == "" || source == nil || source.Client == nil {
		return fmt.Errorf("bootstrap requires a Matrix room and source login")
	}
	converter, ok := source.Client.(PortalBootstrapSource)
	if !ok {
		return fmt.Errorf("source login cannot convert bootstrap history")
	}
	job, err := portal.Bridge.DB.GetBootstrapJob(ctx, source.ID, portal.PortalKey)
	if err != nil {
		return err
	}
	if job == nil || (!job.SourceComplete && !job.PublishRequested) {
		return fmt.Errorf("source history is not complete for %s", portal.ID)
	}
	for {
		job, err = portal.Bridge.DB.GetBootstrapJob(ctx, source.ID, portal.PortalKey)
		if err != nil {
			return err
		}
		if job.Status == "reconcile" {
			return ErrBootstrapNeedsReconciliation
		}
		items, err := portal.Bridge.DB.GetPendingBootstrapHistory(ctx, source.ID, portal.PortalKey, 100)
		if err != nil {
			return err
		}
		if len(items) == 0 {
			return nil
		}
		messages := make([]*BackfillMessage, 0, len(items))
		for _, item := range items {
			message, err := converter.ConvertBootstrapHistory(ctx, portal, item)
			if err != nil {
				return fmt.Errorf("convert bootstrap item %s: %w", item.StableID, err)
			}
			if message == nil || message.ConvertedMessage == nil || len(message.Parts) == 0 {
				return fmt.Errorf("bootstrap item %s has no deliverable content", item.StableID)
			}
			messages = append(messages, message)
		}
		if err := portal.sendBatch(ctx, source, messages, true, false, false, false); err != nil {
			return fmt.Errorf("send bootstrap history batch: %w", err)
		}
		for _, message := range messages {
			for _, part := range message.Parts {
				stored, err := portal.Bridge.DB.Message.GetPartByID(ctx, portal.Receiver, message.ID, part.ID)
				if err != nil {
					return fmt.Errorf("verify imported message %s: %w", message.ID, err)
				}
				if stored == nil {
					return fmt.Errorf("imported message %s has no mapping for part %s", message.ID, part.ID)
				}
			}
			for _, reaction := range message.Reactions {
				if reaction == nil {
					continue
				}
				partID := message.Parts[0].ID
				if reaction.TargetPart != nil {
					partID = *reaction.TargetPart
				} else {
					for _, part := range message.Parts[1:] {
						if part.ID < partID {
							partID = part.ID
						}
					}
				}
				stored, err := portal.Bridge.DB.Reaction.GetByID(ctx, portal.Receiver, message.ID, partID, reaction.Sender.Sender, reaction.EmojiID)
				if err != nil {
					return fmt.Errorf("verify imported reaction on %s: %w", message.ID, err)
				}
				if stored == nil {
					return fmt.Errorf("imported reaction on %s has no mapping", message.ID)
				}
			}
		}
		for _, item := range items {
			if err := portal.Bridge.DB.MarkBootstrapDelivered(ctx, source.ID, portal.PortalKey, item.StableID); err != nil {
				return fmt.Errorf("confirm bootstrap item %s: %w", item.StableID, err)
			}
		}
	}
}
