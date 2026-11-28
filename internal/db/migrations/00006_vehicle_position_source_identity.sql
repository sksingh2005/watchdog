-- Vehicle-position endpoints are identified by their source: the full
-- configured feed URL, query string included, reduced to a non-secret stable
-- hash (see models.FeedSourceKey). Feeds that differ only by query string are
-- therefore different sources, so the same vehicle ID on two of them is two
-- vehicles, not one. The server and the source together identify the endpoint;
-- the normalized URL is the sanitized form of the source's feed URL and is
-- kept for display.

-- +goose Up
ALTER TABLE vehicle_position_endpoint
    ADD COLUMN feed_source_key TEXT;

ALTER TABLE vehicle_position_endpoint
    DROP CONSTRAINT vehicle_position_endpoint_server_id_normalized_vehicle_position_url_key,
    ADD CONSTRAINT vehicle_position_endpoint_server_id_feed_source_key_key
        UNIQUE (server_id, feed_source_key);

-- +goose Down
ALTER TABLE vehicle_position_endpoint
    DROP CONSTRAINT vehicle_position_endpoint_server_id_feed_source_key_key,
    ADD CONSTRAINT vehicle_position_endpoint_server_id_normalized_vehicle_position_url_key
        UNIQUE (server_id, normalized_vehicle_position_url);
