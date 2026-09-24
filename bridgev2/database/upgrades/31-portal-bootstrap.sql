-- v31 (compatible with v9+): Stage selected-chat history and incoming events before publishing a portal
CREATE TABLE portal_bootstrap (
    bridge_id TEXT NOT NULL,
    user_login_id TEXT NOT NULL,
    portal_id TEXT NOT NULL,
    portal_receiver TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'importing', 'ready', 'incomplete', 'reconcile', 'unknown')),
    cursor TEXT NOT NULL DEFAULT '',
    source_complete BOOLEAN NOT NULL DEFAULT false,
    next_order BIGINT NOT NULL DEFAULT 0,
    last_error TEXT,
    updated_at BIGINT NOT NULL,
    PRIMARY KEY (bridge_id, user_login_id, portal_id, portal_receiver)
);

CREATE TABLE portal_bootstrap_item (
    bridge_id TEXT NOT NULL,
    user_login_id TEXT NOT NULL,
    portal_id TEXT NOT NULL,
    portal_receiver TEXT NOT NULL,
    stable_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    version INTEGER NOT NULL,
    payload TEXT NOT NULL,
    source_ts BIGINT NOT NULL,
    arrival_order BIGINT NOT NULL,
    delivered BOOLEAN NOT NULL DEFAULT false,
    expected_parts TEXT,
    PRIMARY KEY (bridge_id, user_login_id, portal_id, portal_receiver, stable_id),
    UNIQUE (bridge_id, user_login_id, portal_id, portal_receiver, arrival_order),
    FOREIGN KEY (bridge_id, user_login_id, portal_id, portal_receiver)
        REFERENCES portal_bootstrap (bridge_id, user_login_id, portal_id, portal_receiver)
        ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE INDEX portal_bootstrap_item_pending_idx
    ON portal_bootstrap_item (bridge_id, user_login_id, portal_id, portal_receiver, delivered, arrival_order);
CREATE INDEX portal_bootstrap_history_pending_idx
    ON portal_bootstrap_item (bridge_id, user_login_id, portal_id, portal_receiver, kind, delivered, source_ts, arrival_order DESC);
