// Copyright (c) 2024 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package bridgev2

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog"

	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/format"
	"maunium.net/go/mautrix/id"
)

func (br *Bridge) handleBotInvite(ctx context.Context, evt *event.Event, sender *User) EventHandlingResult {
	log := zerolog.Ctx(ctx)
	// These invites should already be rejected in QueueMatrixEvent
	if !sender.Permissions.Commands {
		log.Warn().Msg("Received bot invite from user without permission to send commands")
		return EventHandlingResultIgnored
	}
	err := br.Bot.EnsureJoined(ctx, evt.RoomID)
	if err != nil {
		log.Err(err).Msg("Failed to accept invite to room")
		return EventHandlingResultFailed.WithError(err)
	}
	log.Debug().Msg("Accepted invite to room as bot")
	members, err := br.Matrix.GetMembers(ctx, evt.RoomID)
	if err != nil {
		log.Err(err).Msg("Failed to get members of room after accepting invite")
	}
	if len(members) == 2 {
		var message string
		if sender.ManagementRoom == "" {
			message = fmt.Sprintf("Hello, I'm a %s bridge bot.\n\nUse `help` for help or `login` to log in.\n\nThis room has been marked as your management room.", br.Network.GetName().DisplayName)
			sender.ManagementRoom = evt.RoomID
			err = br.DB.User.Update(ctx, sender.User)
			if err != nil {
				log.Err(err).Msg("Failed to update user's management room in database")
			}
		} else {
			message = fmt.Sprintf("Hello, I'm a %s bridge bot.\n\nUse `%s help` for help.", br.Network.GetName().DisplayName, br.Config.CommandPrefix)
		}
		_, err = br.Bot.SendMessage(ctx, evt.RoomID, event.EventMessage, &event.Content{
			Parsed: format.RenderMarkdown(message, true, false),
		}, nil)
		if err != nil {
			log.Err(err).Msg("Failed to send welcome message to room")
		}
	}
	return EventHandlingResultSuccess
}

func sendNotice(ctx context.Context, evt *event.Event, intent MatrixAPI, message string, args ...any) {
	if len(args) > 0 {
		message = fmt.Sprintf(message, args...)
	}
	content := format.RenderMarkdown(message, true, false)
	content.MsgType = event.MsgNotice
	resp, err := intent.SendMessage(ctx, evt.RoomID, event.EventMessage, &event.Content{Parsed: content}, nil)
	if err != nil {
		zerolog.Ctx(ctx).Err(err).
			Stringer("room_id", evt.RoomID).
			Stringer("inviter_id", evt.Sender).
			Stringer("invitee_id", intent.GetMXID()).
			Str("notice_text", message).
			Msg("Failed to send notice")
	} else {
		zerolog.Ctx(ctx).Debug().
			Stringer("notice_event_id", resp.EventID).
			Stringer("room_id", evt.RoomID).
			Stringer("inviter_id", evt.Sender).
			Stringer("invitee_id", intent.GetMXID()).
			Str("notice_text", message).
			Msg("Sent notice")
	}
}

func sendErrorAndLeave(ctx context.Context, evt *event.Event, intent MatrixAPI, message string, args ...any) {
	sendNotice(ctx, evt, intent, message, args...)
	rejectInvite(ctx, evt, intent, "")
}

func (portal *Portal) CleanupOrphanedDM(ctx context.Context, userMXID id.UserID) {
	if portal.MXID == "" {
		return
	}
	log := zerolog.Ctx(ctx)
	existingPortalMembers, err := portal.Bridge.Matrix.GetMembers(ctx, portal.MXID)
	if err != nil {
		log.Err(err).
			Stringer("old_portal_mxid", portal.MXID).
			Msg("Failed to check existing portal members, deleting room")
	} else if targetUserMember, ok := existingPortalMembers[userMXID]; !ok {
		log.Debug().
			Stringer("old_portal_mxid", portal.MXID).
			Msg("Inviter has no member event in old portal, deleting room")
	} else if targetUserMember.Membership.IsInviteOrJoin() {
		return
	} else {
		log.Debug().
			Stringer("old_portal_mxid", portal.MXID).
			Str("membership", string(targetUserMember.Membership)).
			Msg("Inviter is not in old portal, deleting room")
	}

	if err = portal.RemoveMXID(ctx); err != nil {
		log.Err(err).Msg("Failed to delete old portal mxid")
	} else if err = portal.Bridge.Bot.DeleteRoom(ctx, portal.MXID, true); err != nil {
		log.Err(err).Msg("Failed to clean up old portal room")
	}
}

func (br *Bridge) handleGhostDMInvite(ctx context.Context, evt *event.Event, sender *User) EventHandlingResult {
	ghostID, _ := br.Matrix.ParseGhostMXID(id.UserID(evt.GetStateKey()))
	validator, ok := br.Network.(IdentifierValidatingNetwork)
	if ghostID == "" || (ok && !validator.ValidateUserID(ghostID)) {
		rejectInvite(ctx, evt, br.Matrix.GhostIntent(ghostID), "Malformed user ID")
		return EventHandlingResultIgnored
	}
	// A ghost invite starts in an already-visible room; the bridge cannot stage history first.
	reason := fmt.Sprintf("Create chats with `%s pm` in the bridge management room so history can be imported first", br.Config.CommandPrefix)
	rejectInvite(ctx, evt, br.Matrix.GhostIntent(ghostID), reason)
	if sender.ManagementRoom != "" {
		content := format.RenderMarkdown(reason, true, false)
		content.MsgType = event.MsgNotice
		if _, err := br.Bot.SendMessage(ctx, sender.ManagementRoom, event.EventMessage, &event.Content{Parsed: content}, nil); err != nil {
			zerolog.Ctx(ctx).Err(err).Msg("Failed to explain rejected ghost invite in management room")
		}
	}
	return EventHandlingResultIgnored
}

func (br *Bridge) givePowerToBot(ctx context.Context, roomID id.RoomID, userWithPower MatrixAPI) error {
	powers, err := br.Matrix.GetPowerLevels(ctx, roomID)
	if err != nil {
		return fmt.Errorf("failed to get power levels: %w", err)
	}
	userLevel := powers.GetUserLevel(userWithPower.GetMXID())
	if powers.EnsureUserLevelAs(userWithPower.GetMXID(), br.Bot.GetMXID(), userLevel) {
		if userLevel > powers.UsersDefault {
			powers.SetUserLevel(userWithPower.GetMXID(), userLevel-1)
		}
		_, err = userWithPower.SendState(ctx, roomID, event.StatePowerLevels, "", &event.Content{
			Parsed: powers,
		}, time.Time{})
		if err != nil {
			return fmt.Errorf("failed to give power to bot: %w", err)
		}
	}
	return nil
}
