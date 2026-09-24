-- v30 (compatible with v9+): Persist prepared batch requests until mappings commit
CREATE TABLE batch_checkpoint (
    bridge_id TEXT NOT NULL,
    room_id TEXT NOT NULL,
    room_receiver TEXT NOT NULL,
    room_mxid TEXT NOT NULL,
    data jsonb NOT NULL,
    PRIMARY KEY (bridge_id, room_id, room_receiver),
    FOREIGN KEY (bridge_id, room_id, room_receiver)
        REFERENCES portal (bridge_id, id, receiver) ON DELETE CASCADE ON UPDATE CASCADE
);
