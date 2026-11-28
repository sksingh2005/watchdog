package history

import (
	"sort"
	"time"

	"watchdog.onebusaway.org/internal/geo"
)

// vehicleAnomalyType is a vehicle_anomaly_type.anomaly_code.
type vehicleAnomalyType string

const (
	anomalyStaleSourceTimestamp vehicleAnomalyType = "stale_source_timestamp"
	anomalyInvalidCoordinate    vehicleAnomalyType = "invalid_coordinate"
	anomalyStoppedOutOfBounds   vehicleAnomalyType = "stopped_out_of_bounds"
)

// vehicleAnomalyTypes lists every type the rules evaluate, in a fixed order
// so plans are deterministic.
var vehicleAnomalyTypes = []vehicleAnomalyType{
	anomalyStaleSourceTimestamp,
	anomalyInvalidCoordinate,
	anomalyStoppedOutOfBounds,
}

// vehicleCloseReason is a vehicle_anomaly_episode.close_reason.
type vehicleCloseReason string

const (
	closeFreshSourceTimestamp  vehicleCloseReason = "fresh_source_timestamp"
	closeNotPresentInFull      vehicleCloseReason = "not_present_in_full_dataset"
	closeCoordinatesValidAgain vehicleCloseReason = "coordinates_valid_again"
	closeInsideBoundingBox     vehicleCloseReason = "inside_bounding_box"
	closeNoLongerStoppedAtStop vehicleCloseReason = "no_longer_stopped_at_stop"
	// closeFeedRemoved closes an episode whose server or feed left the
	// configuration, so no later response can close it by absence.
	closeFeedRemoved vehicleCloseReason = "feed_removed"
)

// The stale threshold is clamp(5 × fetch interval, 60 s, 300 s).
const (
	staleThresholdFetchMultiple = 5
	minStaleThreshold           = 60 * time.Second
	maxStaleThreshold           = 300 * time.Second
)

// staleThreshold is clamp(5 × fetch interval, 60 s, 300 s).
func staleThreshold(fetchInterval time.Duration) time.Duration {
	return min(max(staleThresholdFetchMultiple*fetchInterval, minStaleThreshold), maxStaleThreshold)
}

// vehicleFeed is one feed of a realtime snapshot. source is the feed's
// identity, models.FeedSourceKey of the configured feed URL, query string
// included, so feeds that differ only by query string are different feeds.
// feedURL is the sanitized form of that URL, kept for display and for the
// vehicle-position endpoint row. observedAt is the snapshot's observation
// time, fullDataset says whether the response was a full dataset, sourceTimestamp
// the FeedHeader timestamp, and payloadHash the canonical entity hash.
type vehicleFeed struct {
	feedURL         string
	source          string
	observedAt      time.Time
	fullDataset     bool
	sourceTimestamp *time.Time
	payloadHash     string
	// reportedVehicleIDs holds every vehicle ID in the response, including
	// vehicles that agency-mode filtering left out of this entry's
	// observations. Nil means the observations are the whole response.
	reportedVehicleIDs map[string]struct{}
}

// reported says whether the response contained the vehicle at all.
func (f vehicleFeed) reported(vehicleID string, observed bool) bool {
	if f.reportedVehicleIDs == nil {
		return observed
	}
	_, ok := f.reportedVehicleIDs[vehicleID]
	return ok
}

// vehicleObservation is what the rules need to know about one vehicle in one
// response. Resolving attribution and the bounding box happens before
// planning, so the rules stay pure.
type vehicleObservation struct {
	source    string
	vehicleID string
	// sourceTime is the vehicle's GTFS-Realtime timestamp, nil when absent.
	sourceTime     *time.Time
	positionReason geo.PositionValidationReason
	stoppedAtStop  bool
	// insideBox is nil when no bounding box applies.
	insideBox     *bool
	tripID        string
	tripStartDate string
	// facts holds what the vehicle's evidence row records. The rules do not
	// read it.
	facts vehicleFacts
}

// vehicleEpisodeKey identifies an episode: one anomaly type for one vehicle
// of one feed source.
type vehicleEpisodeKey struct {
	source    string
	vehicleID string
	anomaly   vehicleAnomalyType
}

// openVehicleEpisode is an open vehicle_anomaly_episode row held in memory,
// with the trip of its most recent evidence.
type openVehicleEpisode struct {
	episodeID     int64
	tripID        string
	tripStartDate string
}

// vehicleSightingKey identifies a vehicle on a feed source.
type vehicleSightingKey struct {
	source    string
	vehicleID string
}

// vehicleSighting is the newest response that reported a vehicle, kept in
// memory only and whether or not the vehicle has an open episode.
type vehicleSighting struct {
	// source is the feed source of that response, empty while unknown after a
	// restart. Feeds that differ only by query string are different sources, so
	// only a response from this source can close the vehicle's episodes on
	// absence.
	source string
	// observedAt is that response's observation time. Entries fetch
	// concurrently, so an older response can be recorded after a newer one;
	// it must neither open, change nor close anything.
	observedAt time.Time
}

// newerThan reports whether the sighting is newer than a response observed
// at observedAt.
func (s vehicleSighting) newerThan(observedAt time.Time) bool {
	return observedAt.Before(s.observedAt)
}

// vehicleEpisodeOpen opens an episode with the vehicle's evidence.
type vehicleEpisodeOpen struct {
	key         vehicleEpisodeKey
	observation vehicleObservation
}

// vehicleEpisodeChange adds meaningful_change evidence to an open episode.
type vehicleEpisodeChange struct {
	key         vehicleEpisodeKey
	observation vehicleObservation
}

// vehicleEpisodeClose closes an open episode. observation is nil when the
// vehicle was absent from a full dataset, so no resolution evidence is saved.
type vehicleEpisodeClose struct {
	key         vehicleEpisodeKey
	reason      vehicleCloseReason
	observation *vehicleObservation
}

// vehicleEpisodePlan is what one tick changes for one server.
type vehicleEpisodePlan struct {
	opens   []vehicleEpisodeOpen
	changes []vehicleEpisodeChange
	closes  []vehicleEpisodeClose
}

// empty reports whether the tick changes nothing, so it needs no database call.
func (p vehicleEpisodePlan) empty() bool {
	return len(p.opens) == 0 && len(p.changes) == 0 && len(p.closes) == 0
}

// ruleOutcome is what one rule says about a present vehicle.
type ruleOutcome struct {
	anomalous bool
	// closeReason is set when the condition is clear. When neither is set
	// the rule cannot judge the vehicle and an open episode stays open.
	closeReason vehicleCloseReason
}

// evaluateRule applies one anomaly type's rule to a present vehicle.
func evaluateRule(anomaly vehicleAnomalyType, obs vehicleObservation, observedAt time.Time, threshold time.Duration) ruleOutcome {
	switch anomaly {
	case anomalyStaleSourceTimestamp:
		if obs.sourceTime == nil {
			return ruleOutcome{}
		}
		if observedAt.Sub(*obs.sourceTime) > threshold {
			return ruleOutcome{anomalous: true}
		}
		return ruleOutcome{closeReason: closeFreshSourceTimestamp}
	case anomalyInvalidCoordinate:
		if obs.positionReason != "" {
			return ruleOutcome{anomalous: true}
		}
		return ruleOutcome{closeReason: closeCoordinatesValidAgain}
	case anomalyStoppedOutOfBounds:
		// Invalid coordinates say nothing about where the vehicle is.
		if obs.positionReason != "" {
			return ruleOutcome{}
		}
		if !obs.stoppedAtStop {
			return ruleOutcome{closeReason: closeNoLongerStoppedAtStop}
		}
		if obs.insideBox == nil {
			return ruleOutcome{}
		}
		if !*obs.insideBox {
			return ruleOutcome{anomalous: true}
		}
		return ruleOutcome{closeReason: closeInsideBoundingBox}
	}
	return ruleOutcome{}
}

// planVehicleAnomalies compares one tick's vehicles with the open episodes in
// memory. Vehicles without an ID are skipped. A vehicle missing from a
// full-dataset response closes its open episodes; a differential feed's
// absence changes nothing. A vehicle that is in the response but not among
// the observations (another source's vehicle on a shared feed) is left to
// the entry that observes it. Episodes are keyed by feed source, so two
// sources that share a sanitized feed URL never share an episode.
func planVehicleAnomalies(open map[vehicleEpisodeKey]*openVehicleEpisode, sightings map[vehicleSightingKey]vehicleSighting, feeds []vehicleFeed, observations []vehicleObservation, threshold time.Duration) vehicleEpisodePlan {
	feedBySource := make(map[string]vehicleFeed, len(feeds))
	for _, feed := range feeds {
		feedBySource[feed.source] = feed
	}

	var plan vehicleEpisodePlan
	type presentKey struct{ source, vehicleID string }
	present := make(map[presentKey]bool, len(observations))
	for _, obs := range observations {
		feed, ok := feedBySource[obs.source]
		pk := presentKey{source: obs.source, vehicleID: obs.vehicleID}
		if !ok || obs.vehicleID == "" || present[pk] {
			continue
		}
		present[pk] = true
		if sighting, ok := sightings[vehicleSightingKey{pk.source, pk.vehicleID}]; ok && sighting.newerThan(feed.observedAt) {
			continue
		}
		for _, anomaly := range vehicleAnomalyTypes {
			key := vehicleEpisodeKey{source: obs.source, vehicleID: obs.vehicleID, anomaly: anomaly}
			planPresentVehicle(&plan, key, open[key], obs, evaluateRule(anomaly, obs, feed.observedAt, threshold))
		}
	}

	for key := range open {
		feed, ok := feedBySource[key.source]
		pk := presentKey{source: key.source, vehicleID: key.vehicleID}
		if ok && closesOnAbsence(feed, sightings[vehicleSightingKey{key.source, key.vehicleID}], key.vehicleID, present[pk]) {
			plan.closes = append(plan.closes, vehicleEpisodeClose{key: key, reason: closeNotPresentInFull})
		}
	}
	plan.sort()
	return plan
}

// closesOnAbsence reports whether a full-dataset response that lacks the
// vehicle closes its episodes. The response must come from the source last
// seen reporting the vehicle, and must not be older than that sighting. A
// sighting of unknown source, which is what a restart leaves behind, closes
// nothing: the source the vehicle was seen on is not known, so absence cannot
// be attributed to a response.
func closesOnAbsence(feed vehicleFeed, sighting vehicleSighting, vehicleID string, observed bool) bool {
	fromItsSource := sighting.source != "" && sighting.source == feed.source
	return feed.fullDataset &&
		fromItsSource &&
		!sighting.newerThan(feed.observedAt) &&
		!feed.reported(vehicleID, observed)
}

// planPresentVehicle adds what one rule's outcome means for one vehicle.
func planPresentVehicle(plan *vehicleEpisodePlan, key vehicleEpisodeKey, episode *openVehicleEpisode, obs vehicleObservation, outcome ruleOutcome) {
	switch {
	case episode == nil && outcome.anomalous:
		plan.opens = append(plan.opens, vehicleEpisodeOpen{key: key, observation: obs})
	case episode == nil:
	case outcome.closeReason != "":
		plan.closes = append(plan.closes, vehicleEpisodeClose{key: key, reason: outcome.closeReason, observation: &obs})
	case obs.tripID != episode.tripID || obs.tripStartDate != episode.tripStartDate:
		plan.changes = append(plan.changes, vehicleEpisodeChange{key: key, observation: obs})
	}
}

func (p *vehicleEpisodePlan) sort() {
	sort.Slice(p.opens, func(i, j int) bool { return p.opens[i].key.less(p.opens[j].key) })
	sort.Slice(p.changes, func(i, j int) bool { return p.changes[i].key.less(p.changes[j].key) })
	sort.Slice(p.closes, func(i, j int) bool { return p.closes[i].key.less(p.closes[j].key) })
}

func (k vehicleEpisodeKey) less(other vehicleEpisodeKey) bool {
	if k.source != other.source {
		return k.source < other.source
	}
	if k.vehicleID != other.vehicleID {
		return k.vehicleID < other.vehicleID
	}
	return k.anomaly < other.anomaly
}
