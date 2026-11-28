-- New close reason for vehicle-anomaly episodes closed because their feed, or
-- the server that fed it, left the configuration.
--
-- An episode normally closes when a full dataset response lacks the vehicle
-- ('not_present_in_full_dataset'). When the vehicle's server or feed is no
-- longer configured there is no later response to wait for: the episode closes
-- against a response recorded for the removal, with the explicit reason.

-- +goose Up
ALTER TABLE vehicle_anomaly_episode
    DROP CONSTRAINT vehicle_anomaly_episode_close_reason_check,
    ADD CONSTRAINT vehicle_anomaly_episode_close_reason_check
        CHECK (close_reason IN (
            'fresh_source_timestamp',
            'not_present_in_full_dataset',
            'coordinates_valid_again',
            'inside_bounding_box',
            'no_longer_stopped_at_stop',
            'feed_removed'
        ));

-- +goose Down
-- Rows closed with a removal reason predate the choice, so fall back to the
-- absence reason; the closing response itself is kept.
UPDATE vehicle_anomaly_episode
SET close_reason = 'not_present_in_full_dataset'
WHERE close_reason = 'feed_removed';

ALTER TABLE vehicle_anomaly_episode
    DROP CONSTRAINT vehicle_anomaly_episode_close_reason_check,
    ADD CONSTRAINT vehicle_anomaly_episode_close_reason_check
        CHECK (close_reason IN (
            'fresh_source_timestamp',
            'not_present_in_full_dataset',
            'coordinates_valid_again',
            'inside_bounding_box',
            'no_longer_stopped_at_stop'
        ));
