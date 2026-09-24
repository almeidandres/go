-- v32: Explicitly opt selected portals into cached-only publication without claiming source completion.
ALTER TABLE portal_bootstrap ADD COLUMN publish_requested BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE portal_bootstrap ADD COLUMN published_cached BOOLEAN NOT NULL DEFAULT false;
