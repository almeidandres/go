package bridgev2

import (
	"context"
	"encoding/json"
	"fmt"

	"maunium.net/go/mautrix/bridgev2/database"
)

// The body is already encrypted and serialized. Never reconstruct it on retry.
type batchCheckpointData struct {
	Body      []byte                          `json:"body"`
	Messages  []json.RawMessage               `json:"messages"`
	Reactions []json.RawMessage               `json:"reactions"`
	Disappear []*database.DisappearingMessage `json:"disappear"`
}

func makeBatchCheckpointData(body []byte, out *compileBatchOutput) ([]byte, error) {
	data := batchCheckpointData{Body: body, Disappear: out.Disappear}
	for _, msg := range out.DBMessages {
		raw, err := json.Marshal(msg)
		if err != nil {
			return nil, fmt.Errorf("encode batch message: %w", err)
		}
		data.Messages = append(data.Messages, raw)
	}
	for _, reaction := range out.DBReactions {
		raw, err := json.Marshal(reaction)
		if err != nil {
			return nil, fmt.Errorf("encode batch reaction: %w", err)
		}
		data.Reactions = append(data.Reactions, raw)
	}
	return json.Marshal(data)
}

func (br *Bridge) replayBatchCheckpoint(ctx context.Context, cp *database.BatchCheckpoint) error {
	sender, ok := br.Matrix.(BatchCheckpointSender)
	if !ok {
		return fmt.Errorf("matrix connector does not support durable batch replay")
	}
	var data batchCheckpointData
	if err := json.Unmarshal(cp.Data, &data); err != nil {
		return fmt.Errorf("decode batch checkpoint: %w", err)
	}
	if len(data.Body) == 0 || cp.RoomID == "" {
		return fmt.Errorf("invalid batch checkpoint for %v", cp.Portal)
	}
	// Validate every mapping before sending. A corrupt checkpoint must never send an unmappable batch.
	messages := make([]*database.Message, 0, len(data.Messages))
	for _, raw := range data.Messages {
		msg := &database.Message{Metadata: br.DB.Message.MetaType()}
		if err := json.Unmarshal(raw, msg); err != nil {
			return fmt.Errorf("decode batch message: %w", err)
		}
		messages = append(messages, msg)
	}
	reactions := make([]*database.Reaction, 0, len(data.Reactions))
	for _, raw := range data.Reactions {
		reaction := &database.Reaction{Metadata: br.DB.Reaction.MetaType()}
		if err := json.Unmarshal(raw, reaction); err != nil {
			return fmt.Errorf("decode batch reaction: %w", err)
		}
		reactions = append(reactions, reaction)
	}
	if _, err := sender.SendPreparedBatch(ctx, cp.RoomID, data.Body); err != nil {
		return fmt.Errorf("send saved batch: %w", err)
	}
	err := br.DB.DoTxn(ctx, nil, func(txCtx context.Context) error {
		for _, msg := range messages {
			existing, err := br.DB.Message.GetPartByID(txCtx, msg.Room.Receiver, msg.ID, msg.PartID)
			if err != nil {
				return fmt.Errorf("check batch message: %w", err)
			}
			if existing != nil {
				if existing.MXID != msg.MXID || existing.Room != msg.Room {
					return fmt.Errorf("batch message %s/%s already mapped differently", msg.ID, msg.PartID)
				}
				continue
			}
			if err = br.DB.Message.Insert(txCtx, msg); err != nil {
				return fmt.Errorf("map batch message %s/%s: %w", msg.ID, msg.PartID, err)
			}
		}
		for _, reaction := range reactions {
			existing, err := br.DB.Reaction.GetByID(txCtx, reaction.Room.Receiver, reaction.MessageID, reaction.MessagePartID, reaction.SenderID, reaction.EmojiID)
			if err != nil {
				return fmt.Errorf("check batch reaction: %w", err)
			}
			if existing != nil {
				if existing.MXID != reaction.MXID || existing.Room != reaction.Room {
					return fmt.Errorf("batch reaction %s already mapped differently", reaction.MXID)
				}
				continue
			}
			if err = br.DB.Reaction.Upsert(txCtx, reaction); err != nil {
				return fmt.Errorf("map batch reaction: %w", err)
			}
		}
		for _, disappearing := range data.Disappear {
			if err := br.DB.DisappearingMessage.Put(txCtx, disappearing); err != nil {
				return fmt.Errorf("map disappearing batch message: %w", err)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	// If clearing fails, replay repeats the same HTTP bytes and mapping checks remain idempotent.
	if err = br.DB.DeleteBatchCheckpoint(ctx, cp.Portal); err != nil {
		return fmt.Errorf("clear batch checkpoint: %w", err)
	}
	for _, disappearing := range data.Disappear {
		br.DisappearLoop.Add(ctx, disappearing)
	}
	return nil
}

func (br *Bridge) replayPendingBatches(ctx context.Context) error {
	checkpoints, err := br.DB.ListBatchCheckpoints(ctx)
	if err != nil {
		return fmt.Errorf("list batch checkpoints: %w", err)
	}
	for _, cp := range checkpoints {
		if err = br.replayBatchCheckpoint(ctx, cp); err != nil {
			return fmt.Errorf("replay batch for %v: %w", cp.Portal, err)
		}
	}
	return nil
}
