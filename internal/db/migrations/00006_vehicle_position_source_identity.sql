-- Vehicle-position endpoints are identified by their source: the full
-- configured feed URL, query string included, reduced to a non-secret stable
-- hash (see models.FeedSourceKey). Feeds that differ only by query string are
-- therefore different sources, so the same vehicle ID on two of them is two
-- vehicles, not one. The server and the source together identify the endpoint;
-- the normalized URL is the sanitized form of the source's feed URL and is
-- kept for display.
--
-- Existing endpoints get a NULL key: the normalized URL no longer carries the
-- query string, so the source cannot be reconstructed from the stored row.
-- Watchdog keeps their open episodes from being closed as removed while their
-- server is still configured, and only their own server's departure closes
-- them. No backfill is possible without guessing the query string.

-- +goose Up
ALTER TABLE vehicle_position_endpoint
    ADD COLUMN feed_source_key TEXT;

-- 00001 declares this UNIQUE inline, so PostgreSQL generated the 63-character
-- name below; the longer spelling is truncated to a different name and cannot
-- be dropped.
ALTER TABLE vehicle_position_endpoint
    DROP CONSTRAINT vehicle_position_endpoint_server_id_normalized_vehicle_posi_key,
    ADD CONSTRAINT vehicle_position_endpoint_server_id_feed_source_key_key
        UNIQUE (server_id, feed_source_key);

-- +goose Down
-- Restoring the old uniqueness needs one endpoint per normalized URL. Two
-- query-string-distinct endpoints can share one, so stop instead of deleting
-- endpoints or the history that references them.
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM vehicle_position_endpoint
        GROUP BY server_id, normalized_vehicle_position_url
        HAVING count(*) > 1
    ) THEN
        RAISE EXCEPTION 'cannot downgrade 00006: query-string-distinct endpoints share a normalized URL';
    END IF;
END
$$;
-- +goose StatementEnd

ALTER TABLE vehicle_position_endpoint
    DROP CONSTRAINT vehicle_position_endpoint_server_id_feed_source_key_key,
    ADD CONSTRAINT vehicle_position_endpoint_server_id_normalized_vehicle_posi_key
        UNIQUE (server_id, normalized_vehicle_position_url);

ALTER TABLE vehicle_position_endpoint
    DROP COLUMN feed_source_key;
