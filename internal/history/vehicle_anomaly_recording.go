package history

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	remoteGtfs "github.com/OneBusAway/go-gtfs"
	gtfsrt "github.com/OneBusAway/go-gtfs/proto"
	"github.com/jackc/pgx/v5/pgtype"

	"watchdog.onebusaway.org/internal/db/dbgen"
	"watchdog.onebusaway.org/internal/geo"
	"watchdog.onebusaway.org/internal/gtfs"
	"watchdog.onebusaway.org/internal/metrics"
	"watchdog.onebusaway.org/internal/models"
	"watchdog.onebusaway.org/internal/utils"
)

// Steps name these writes in logs, Sentry and the write failure counter.
const (
	vehicleAnomalyStep            = "vehicle_anomalies"
	vehicleAnomalyLoadStep        = "vehicle_anomaly_load"
	vehicleAnomalyRuleVersionStep = "vehicle_anomaly_rule_versions"
	vehicleAnomalyFlushStep       = "vehicle_anomaly_flush"
)

// Evidence roles link evidence to an episode.
const (
	evidenceOpening          = "opening"
	evidenceMeaningfulChange = "meaningful_change"
	evidenceResolution       = "resolution"
)

// vehicleAnomalyRuleLogicVersion is bumped whenever a rule's detection logic
// changes, so a new vehicle_anomaly_rule_version row is recorded.
const vehicleAnomalyRuleLogicVersion int32 = 1

// gtfsDateLayout is a GTFS date (YYYYMMDD), the form trip start dates are
// compared and classified in.
const gtfsDateLayout = "20060102"

// legacySourcePrefix marks an endpoint that predates source-keyed endpoints.
// Its stored URL has no query string, so its feed source is unknown. Keeping
// the endpoint ID in the key keeps episodes of different legacy endpoints
// apart, while sourceIsKnown keeps them from being closed as a departed feed
// while their server is still configured.
const legacySourcePrefix = "legacy:"

func legacySource(endpointID int64) string {
	return fmt.Sprintf("%s%d", legacySourcePrefix, endpointID)
}

func sourceIsKnown(source string) bool {
	return source != "" && !strings.HasPrefix(source, legacySourcePrefix)
}

// vehicleFacts is what a vehicle's evidence row records beyond the rule
// inputs.
type vehicleFacts struct {
	vehicle remoteGtfs.Vehicle
	// latitude and longitude are the raw values, kept even when invalid.
	latitude  *float64
	longitude *float64
	// agency is the attributed agency entry; it is the zero value when
	// attributionReason is set.
	agency            models.ObaServer
	attributionReason gtfs.AttributionFailureReason
	// computedSpeedMps and speedDiscrepancyRatio are what the telemetry pass
	// computed for the vehicle this tick, nil when it computed none.
	computedSpeedMps      *float64
	speedDiscrepancyRatio *float64
	// boxVersionID is the bounding box version the vehicle was checked
	// against, read when it was checked. It is nil when no box applied or the
	// box's version is not recorded.
	boxVersionID *int64
}

// serverVehicleEpisodes is one server's open episodes. mu serializes every
// change to them, from every entry on the server.
type serverVehicleEpisodes struct {
	mu     sync.Mutex
	server models.ObaServer
	loaded bool
	open   map[vehicleEpisodeKey]*openVehicleEpisode
	// sightings outlives a reload: it is memory-only information the
	// database cannot restore.
	sightings map[vehicleSightingKey]vehicleSighting
}

// openVehicleEpisodes holds every server's open episodes, keyed by the
// sanitized base URL. The database allows one open episode per vehicle and
// type, and a vehicle belongs to a feed source of a server, not to an agency,
// so every entry on a base URL shares one set: agency-mode entries that share
// a feed source, and a server-scoped entry alongside them, see the same episode.
type openVehicleEpisodes struct {
	mu      sync.Mutex
	entries map[string]*serverVehicleEpisodes
}

func newOpenVehicleEpisodes() *openVehicleEpisodes {
	return &openVehicleEpisodes{entries: make(map[string]*serverVehicleEpisodes)}
}

// forServer returns the server's open episodes, creating an unloaded set if
// needed.
func (o *openVehicleEpisodes) forServer(baseURL string, server models.ObaServer) *serverVehicleEpisodes {
	o.mu.Lock()
	defer o.mu.Unlock()
	entry, ok := o.entries[baseURL]
	if !ok {
		entry = &serverVehicleEpisodes{
			server:    server,
			open:      make(map[vehicleEpisodeKey]*openVehicleEpisode),
			sightings: make(map[vehicleSightingKey]vehicleSighting),
		}
		o.entries[baseURL] = entry
	}
	return entry
}

// lookup returns the server's entry without creating one.
func (o *openVehicleEpisodes) lookup(baseURL string) (*serverVehicleEpisodes, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	entry, ok := o.entries[baseURL]
	return entry, ok
}

// remove drops the server's entry and returns it, if there was one.
func (o *openVehicleEpisodes) remove(baseURL string) (*serverVehicleEpisodes, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	entry, ok := o.entries[baseURL]
	delete(o.entries, baseURL)
	return entry, ok
}

// baseURLs returns the sanitized base URLs holding open episodes, sorted, so
// closes are written in a deterministic order.
func (o *openVehicleEpisodes) baseURLs() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	urls := make([]string, 0, len(o.entries))
	for url := range o.entries {
		urls = append(urls, url)
	}
	sort.Strings(urls)
	return urls
}

// vehicleRuleVersions holds the anomaly type IDs and the rule version in
// effect for each type. It is filled once, before the first episode opens.
type vehicleRuleVersions struct {
	mu        sync.Mutex
	ready     bool
	typeIDs   map[vehicleAnomalyType]int16
	versionID map[vehicleAnomalyType]int64
}

// vehicleRuleSettings is one type's rule as Watchdog currently applies it.
type vehicleRuleSettings struct {
	logicVersion              int32
	staleAgeThresholdSeconds  *int32
	versionName               string
	reportedSpeedThresholdMps *float64
}

// ruleSettings returns the rule each type is applied with.
func ruleSettings(anomaly vehicleAnomalyType, threshold time.Duration) vehicleRuleSettings {
	settings := vehicleRuleSettings{logicVersion: vehicleAnomalyRuleLogicVersion, versionName: fmt.Sprintf("logic %d", vehicleAnomalyRuleLogicVersion)}
	if anomaly == anomalyStaleSourceTimestamp {
		seconds := int32(threshold / time.Second)
		settings.staleAgeThresholdSeconds = &seconds
		settings.versionName = fmt.Sprintf("logic %d, stale > %ds", vehicleAnomalyRuleLogicVersion, seconds)
	}
	return settings
}

// sameRule reports whether the newest stored rule version matches settings.
func sameRule(row dbgen.ListNewestVehicleAnomalyRuleVersionsRow, settings vehicleRuleSettings) bool {
	return row.RuleLogicVersion == settings.logicVersion &&
		equalPtr(row.StaleAgeThresholdSeconds, settings.staleAgeThresholdSeconds) &&
		equalPtr(row.ReportedSpeedThresholdMps, settings.reportedSpeedThresholdMps)
}

// SetFetchInterval sets the collection interval the stale threshold is
// derived from. app.New calls it once, before collection starts.
func (h *HistoryService) SetFetchInterval(interval time.Duration) {
	h.fetchInterval = interval
}

// RecordVehiclePositions records the vehicle anomalies of one realtime
// snapshot for one server. server, agencies and attributor are the ones the
// vehicle passes used; boxes and lastSeen are the bounding box and
// last-seen stores they read and wrote.
//
// Call it only after a successful realtime fetch. A snapshot that opens,
// closes and adds evidence to nothing does not touch the database.
func (h *HistoryService) RecordVehiclePositions(ctx context.Context, server models.ObaServer, snapshot *models.RealtimeData, attributor metrics.VehicleAttributor, boxes *geo.BoundingBoxStore, lastSeen *metrics.VehicleLastSeen) {
	if h == nil || h.pool == nil || snapshot == nil {
		return
	}
	baseURL := utils.SanitizeServerURL(server.ObaBaseURL)
	if baseURL == "" {
		return
	}
	// write bounds the transaction, but the queries run on the context fn
	// captures, so bound that too.
	ctx, cancel := context.WithTimeout(ctx, writeTimeout)
	defer cancel()

	entry := h.vehicles.forServer(baseURL, server)
	entry.mu.Lock()
	defer entry.mu.Unlock()

	if !entry.loaded && !h.loadOpenVehicleEpisodes(ctx, server, baseURL, entry) {
		return
	}
	// Rule versions are recorded at the first recording after startup, even
	// one that only closes episodes, so the database always holds the rules
	// this process applies.
	threshold := staleThreshold(h.fetchInterval)
	if !h.ensureRuleVersions(ctx, server, threshold) {
		return
	}

	// Read the static version together with the observations, so evidence
	// cites the version current when the vehicle was judged.
	var staticVersionID *int64
	if version, ok := h.static.currentVersion(server.ServerKey()); ok {
		staticVersionID = &version.id
	}
	feeds, observations := vehicleObservations(server, snapshot, vehicleJudges{attributor: attributor, boxes: boxes, boxVersion: h.static.box, lastSeen: lastSeen})
	plan := planVehicleAnomalies(entry.open, entry.sightings, feeds, observations, threshold)
	if plan.empty() {
		noteSightings(entry, feeds, observations)
		return
	}

	var opened map[vehicleEpisodeKey]*openVehicleEpisode
	committed := h.write(ctx, vehicleAnomalyStep, publicationTags(server), func(q *dbgen.Queries) error {
		var err error
		opened, err = h.recordVehicleEpisodeChanges(ctx, q, server, plan, entry.open, feeds, staticVersionID)
		return err
	})
	if !committed {
		// The commit may have reached the database even though the client
		// saw an error, so memory can no longer be trusted: reload on the
		// next snapshot, which then records any difference with its own data.
		entry.loaded = false
		return
	}
	for key, episode := range opened {
		entry.open[key] = episode
	}
	for _, change := range plan.changes {
		episode := entry.open[change.key]
		episode.tripID = change.observation.tripID
		episode.tripStartDate = change.observation.tripStartDate
	}
	for _, closed := range plan.closes {
		delete(entry.open, closed.key)
	}
	noteSightings(entry, feeds, observations)
}

// sightingRetention is how long a sighting is kept for a vehicle with no
// open episode. Out-of-order responses come from one collection tick, so a
// sighting only needs to outlive that; it is dropped after this so memory
// does not grow with every vehicle ever seen.
const sightingRetention = 10 * time.Minute

// noteSightings records, in memory only, the source and observation time of
// the newest response that reported each vehicle: every vehicle in the raw
// response, including those agency filtering left out of the observations.
// A response older than a vehicle's sighting leaves it alone.
func noteSightings(entry *serverVehicleEpisodes, feeds []vehicleFeed, observations []vehicleObservation) {
	observed := make(map[string][]string, len(feeds))
	for _, obs := range observations {
		observed[obs.source] = append(observed[obs.source], obs.vehicleID)
	}
	var newest time.Time
	for _, feed := range feeds {
		if feed.observedAt.After(newest) {
			newest = feed.observedAt
		}
		ids := observed[feed.source]
		if feed.reportedVehicleIDs != nil {
			ids = make([]string, 0, len(feed.reportedVehicleIDs))
			for id := range feed.reportedVehicleIDs {
				ids = append(ids, id)
			}
		}
		for _, id := range ids {
			key := vehicleSightingKey{source: feed.source, vehicleID: id}
			if current, ok := entry.sightings[key]; ok && current.newerThan(feed.observedAt) {
				continue
			}
			entry.sightings[key] = vehicleSighting{source: feed.source, observedAt: feed.observedAt}
		}
	}
	pruneSightings(entry, newest)
}

// pruneSightings drops sightings older than sightingRetention whose vehicle
// has no open episode.
func pruneSightings(entry *serverVehicleEpisodes, now time.Time) {
	withEpisode := make(map[vehicleSightingKey]bool, len(entry.open))
	for key := range entry.open {
		withEpisode[vehicleSightingKey{source: key.source, vehicleID: key.vehicleID}] = true
	}
	for key, sighting := range entry.sightings {
		if !withEpisode[key] && now.Sub(sighting.observedAt) > sightingRetention {
			delete(entry.sightings, key)
		}
	}
}

// seedSighting gives a loaded episode's vehicle a sighting from its newest
// evidence, unless memory already holds a newer one. The source is the one the
// episode's endpoint reported, and is empty for legacy rows that predate
// source-scoped endpoints, where it stays unknown.
func seedSighting(entry *serverVehicleEpisodes, key vehicleEpisodeKey, observedAt time.Time) {
	sightingKey := vehicleSightingKey{source: key.source, vehicleID: key.vehicleID}
	if current, ok := entry.sightings[sightingKey]; ok && !current.observedAt.Before(observedAt) {
		return
	}
	entry.sightings[sightingKey] = vehicleSighting{source: key.source, observedAt: observedAt}
}

// loadOpenVehicleEpisodes replaces entry's open episodes with the ones in the
// database. It reports whether it succeeded; on failure entry is unchanged
// and stays unloaded, and the next snapshot tries again.
func (h *HistoryService) loadOpenVehicleEpisodes(ctx context.Context, server models.ObaServer, baseURL string, entry *serverVehicleEpisodes) bool {
	rows, err := dbgen.New(h.pool).ListOpenVehicleAnomalyEpisodesForServer(ctx, baseURL)
	if err != nil {
		reportWriteFailure(h.logger, vehicleAnomalyLoadStep, publicationTags(server), fmt.Errorf("load open vehicle anomaly episodes: %w", err))
		return false
	}
	entry.open = make(map[vehicleEpisodeKey]*openVehicleEpisode, len(rows))
	for _, row := range rows {
		source := ""
		if row.FeedSourceKey != nil {
			source = *row.FeedSourceKey
		} else {
			// A pre-upgrade endpoint has no source: its normalized URL does
			// not carry the query string. Use the endpoint ID so two legacy
			// endpoints on one server do not collide in memory.
			source = legacySource(row.EndpointID)
		}
		key := vehicleEpisodeKey{
			source:    source,
			vehicleID: row.ExternalVehicleID,
			anomaly:   vehicleAnomalyType(row.AnomalyCode),
		}
		episode := &openVehicleEpisode{episodeID: row.VehicleAnomalyEpisodeID}
		seedSighting(entry, key, row.ObservedAt)
		if row.TripID != nil {
			episode.tripID = *row.TripID
		}
		if row.TripStartDate.Valid {
			episode.tripStartDate = row.TripStartDate.Time.Format(gtfsDateLayout)
		}
		entry.open[key] = episode
	}
	entry.loaded = true
	return true
}

// ensureRuleVersions records the rule version of every anomaly type once per
// process, before the first episode opens. A type gets a new row when its
// newest stored version differs from the rule Watchdog applies now, or when
// it has none. It reports whether the versions are ready.
func (h *HistoryService) ensureRuleVersions(ctx context.Context, server models.ObaServer, threshold time.Duration) bool {
	h.rules.mu.Lock()
	defer h.rules.mu.Unlock()
	if h.rules.ready {
		return true
	}

	typeIDs := make(map[vehicleAnomalyType]int16, len(vehicleAnomalyTypes))
	versionIDs := make(map[vehicleAnomalyType]int64, len(vehicleAnomalyTypes))
	committed := h.write(ctx, vehicleAnomalyRuleVersionStep, publicationTags(server), func(q *dbgen.Queries) error {
		types, err := q.ListVehicleAnomalyTypeIDs(ctx)
		if err != nil {
			return fmt.Errorf("list vehicle anomaly types: %w", err)
		}
		for _, row := range types {
			typeIDs[vehicleAnomalyType(row.AnomalyCode)] = row.AnomalyTypeID
		}
		newest, err := q.ListNewestVehicleAnomalyRuleVersions(ctx)
		if err != nil {
			return fmt.Errorf("list newest vehicle anomaly rule versions: %w", err)
		}
		newestByType := make(map[int16]dbgen.ListNewestVehicleAnomalyRuleVersionsRow, len(newest))
		for _, row := range newest {
			newestByType[row.AnomalyTypeID] = row
		}

		for _, anomaly := range vehicleAnomalyTypes {
			typeID, ok := typeIDs[anomaly]
			if !ok {
				return fmt.Errorf("vehicle anomaly type %q is not in the database", anomaly)
			}
			settings := ruleSettings(anomaly, threshold)
			if row, ok := newestByType[typeID]; ok && sameRule(row, settings) {
				versionIDs[anomaly] = row.RuleVersionID
				continue
			}
			id, err := q.InsertVehicleAnomalyRuleVersion(ctx, dbgen.InsertVehicleAnomalyRuleVersionParams{
				AnomalyTypeID:            typeID,
				RuleLogicVersion:         settings.logicVersion,
				VersionName:              settings.versionName,
				WatchdogBuildVersion:     nonEmpty(h.version),
				EffectiveAt:              h.startedAt,
				StaleAgeThresholdSeconds: settings.staleAgeThresholdSeconds,
			})
			if err != nil {
				return fmt.Errorf("insert rule version for %q: %w", anomaly, err)
			}
			versionIDs[anomaly] = id
		}
		return nil
	})
	if !committed {
		return false
	}
	h.rules.typeIDs = typeIDs
	h.rules.versionID = versionIDs
	h.rules.ready = true
	return true
}

// vehicleObservations turns a realtime snapshot into the rules' input. A
// vehicle the snapshot only knows from a trip update, with no position
// entity, is not a position report and is left out, as is a vehicle with no
// ID.
func vehicleObservations(server models.ObaServer, snapshot *models.RealtimeData, judges vehicleJudges) ([]vehicleFeed, []vehicleObservation) {
	feeds := make([]vehicleFeed, 0, len(snapshot.Feeds))
	sourceByFeedID := make(map[string]string, len(snapshot.Feeds))
	for _, feed := range snapshot.Feeds {
		feeds = append(feeds, vehicleFeed{
			feedURL:            feed.FeedURL,
			source:             feed.SourceKey,
			observedAt:         snapshot.ObservationAt,
			fullDataset:        feed.FullDataset,
			sourceTimestamp:    feed.SourceTimestamp,
			payloadHash:        feed.PayloadHash,
			reportedVehicleIDs: feed.ReportedVehicleIDs,
		})
		sourceByFeedID[feed.FeedID] = feed.SourceKey
	}

	observations := make([]vehicleObservation, 0, len(snapshot.Vehicles))
	for _, realtimeVehicle := range snapshot.Vehicles {
		vehicle := realtimeVehicle.Vehicle
		source, ok := sourceByFeedID[realtimeVehicle.FeedID]
		vehicleID := vehicle.GetID().ID
		if !ok || vehicleID == "" || !vehicle.IsEntityInMessage {
			continue
		}
		observations = append(observations, observeVehicle(server, source, realtimeVehicle.FeedID, vehicle, judges))
	}
	return feeds, observations
}

// observeVehicle judges one vehicle the way trackInvalidVehiclesAndStoppedOutOfBounds
// does: coordinates first, then the agency box when the vehicle is
// attributed and its agency has one, otherwise the server box.
func observeVehicle(server models.ObaServer, source, feedID string, vehicle remoteGtfs.Vehicle, judges vehicleJudges) vehicleObservation {
	lat, lon, positionReason := geo.VehiclePosition(vehicle)
	agency, attributionReason := judges.attributor.Attribute(vehicle)

	obs := vehicleObservation{
		source:         source,
		vehicleID:      vehicle.GetID().ID,
		sourceTime:     vehicle.Timestamp,
		positionReason: positionReason,
		stoppedAtStop:  vehicle.CurrentStatus != nil && *vehicle.CurrentStatus == gtfsrt.VehiclePosition_STOPPED_AT,
		facts: vehicleFacts{
			vehicle:           vehicle,
			agency:            agency,
			attributionReason: attributionReason,
		},
	}
	if vehicle.Position != nil {
		obs.facts.latitude = float32Ptr(vehicle.Position.Latitude)
		obs.facts.longitude = float32Ptr(vehicle.Position.Longitude)
	}
	if vehicle.Trip != nil {
		obs.tripID = vehicle.Trip.ID.ID
		if vehicle.Trip.ID.HasStartDate {
			obs.tripStartDate = vehicle.Trip.ID.StartDate.Format(gtfsDateLayout)
		}
	}

	if judges.lastSeen != nil {
		if seen, ok := judges.lastSeen.Get(judges.attributor.LastSeenKey(agency), feedID, obs.vehicleID); ok {
			obs.facts.computedSpeedMps = seen.ComputedSpeedMps
			obs.facts.speedDiscrepancyRatio = seen.SpeedDiscrepancyRatio
		}
	}

	boxes := judges.boxes
	if boxes == nil {
		return obs
	}
	boxKey := server.ServerKey()
	if attributionReason == "" && agency.AgencyID != "" {
		agencyKey := models.ServerKey(server.ObaBaseURL, agency.AgencyID)
		if _, ok := boxes.Get(agencyKey); ok {
			boxKey = agencyKey
		}
	}
	if box, ok := boxes.Get(boxKey); ok {
		if id, ok := judges.boxVersion(boxKey); ok {
			obs.facts.boxVersionID = &id
		}
		if positionReason == "" {
			inside := box.Contains(lat, lon)
			obs.insideBox = &inside
		}
	}
	return obs
}

// vehicleJudges are what a vehicle is judged with: the vehicle passes'
// attribution and bounding boxes, the recorded box versions, and the speeds
// the telemetry pass computed.
type vehicleJudges struct {
	attributor metrics.VehicleAttributor
	boxes      *geo.BoundingBoxStore
	boxVersion func(boxKey string) (int64, bool)
	lastSeen   *metrics.VehicleLastSeen
}

// vehicleEvidenceKey identifies the one evidence row a vehicle gets per feed
// response, shared by every episode that tick links to it.
type vehicleEvidenceKey struct {
	source    string
	vehicleID string
}

// vehicleTickWriter writes one plan inside one transaction, creating each
// identity, response and evidence row at most once.
type vehicleTickWriter struct {
	h        *HistoryService
	q        *dbgen.Queries
	server   models.ObaServer
	serverID int64
	feeds    map[string]vehicleFeed
	// needsBox holds the vehicles whose evidence backs a
	// stopped_out_of_bounds episode, so their row records the box version.
	needsBox        map[vehicleEvidenceKey]bool
	staticVersionID *int64

	endpoints map[string]int64
	responses map[string]int64
	vehicles  map[vehicleEvidenceKey]int64
	evidence  map[vehicleEvidenceKey]int64
	agencies  map[string]int64
}

// recordVehicleEpisodeChanges writes the plan's opens, changes and closes. It
// returns the opened episodes, which the caller adds to memory once the
// transaction commits.
func (h *HistoryService) recordVehicleEpisodeChanges(ctx context.Context, q *dbgen.Queries, server models.ObaServer, plan vehicleEpisodePlan, open map[vehicleEpisodeKey]*openVehicleEpisode, feeds []vehicleFeed, staticVersionID *int64) (map[vehicleEpisodeKey]*openVehicleEpisode, error) {
	serverID, err := ensureServer(ctx, q, server, h.resolver)
	if err != nil {
		return nil, err
	}
	w := &vehicleTickWriter{
		h: h, q: q, server: server, serverID: serverID,
		feeds:     make(map[string]vehicleFeed, len(feeds)),
		needsBox:  plan.boxEvidence(),
		endpoints: make(map[string]int64),
		responses: make(map[string]int64),
		vehicles:  make(map[vehicleEvidenceKey]int64),
		evidence:  make(map[vehicleEvidenceKey]int64),
		agencies:  make(map[string]int64),

		staticVersionID: staticVersionID,
	}
	for _, feed := range feeds {
		w.feeds[feed.source] = feed
	}

	opened := make(map[vehicleEpisodeKey]*openVehicleEpisode, len(plan.opens))
	for _, o := range plan.opens {
		episode, err := w.open(ctx, o)
		if err != nil {
			return nil, err
		}
		opened[o.key] = episode
	}
	for _, change := range plan.changes {
		if err := w.link(ctx, open[change.key].episodeID, change.key, change.observation, evidenceMeaningfulChange); err != nil {
			return nil, err
		}
	}
	for _, c := range plan.closes {
		if err := w.close(ctx, open[c.key].episodeID, c); err != nil {
			return nil, err
		}
	}
	return opened, nil
}

// boxEvidence returns the vehicles whose evidence this plan links to a
// stopped_out_of_bounds episode.
func (p vehicleEpisodePlan) boxEvidence() map[vehicleEvidenceKey]bool {
	needs := make(map[vehicleEvidenceKey]bool)
	mark := func(key vehicleEpisodeKey) {
		if key.anomaly == anomalyStoppedOutOfBounds {
			needs[vehicleEvidenceKey{source: key.source, vehicleID: key.vehicleID}] = true
		}
	}
	for _, o := range p.opens {
		mark(o.key)
	}
	for _, change := range p.changes {
		mark(change.key)
	}
	for _, c := range p.closes {
		if c.observation != nil {
			mark(c.key)
		}
	}
	return needs
}

// open inserts an episode and links its opening evidence.
func (w *vehicleTickWriter) open(ctx context.Context, o vehicleEpisodeOpen) (*openVehicleEpisode, error) {
	responseID, err := w.response(ctx, o.key.source)
	if err != nil {
		return nil, err
	}
	vehicleID, err := w.vehicle(ctx, o.key.source, o.key.vehicleID)
	if err != nil {
		return nil, err
	}
	episodeID, err := w.q.InsertVehicleAnomalyEpisode(ctx, dbgen.InsertVehicleAnomalyEpisodeParams{
		VehicleEntityID:   vehicleID,
		AnomalyTypeID:     w.h.rules.typeIDs[o.key.anomaly],
		RuleVersionID:     w.h.rules.versionID[o.key.anomaly],
		OpeningResponseID: responseID,
	})
	if err != nil {
		return nil, fmt.Errorf("server %q: open %s for vehicle %q: %w", w.server.ServerName, o.key.anomaly, o.key.vehicleID, err)
	}
	if err := w.link(ctx, episodeID, o.key, o.observation, evidenceOpening); err != nil {
		return nil, err
	}
	return &openVehicleEpisode{
		episodeID:     episodeID,
		tripID:        o.observation.tripID,
		tripStartDate: o.observation.tripStartDate,
	}, nil
}

// close closes an episode and, when the vehicle was present, links its
// resolution evidence.
func (w *vehicleTickWriter) close(ctx context.Context, episodeID int64, c vehicleEpisodeClose) error {
	responseID, err := w.response(ctx, c.key.source)
	if err != nil {
		return err
	}
	rows, err := w.q.CloseVehicleAnomalyEpisode(ctx, dbgen.CloseVehicleAnomalyEpisodeParams{
		ClosingResponseID:       responseID,
		CloseReason:             string(c.reason),
		VehicleAnomalyEpisodeID: episodeID,
	})
	if err != nil {
		return fmt.Errorf("server %q: close %s for vehicle %q: %w", w.server.ServerName, c.key.anomaly, c.key.vehicleID, err)
	}
	if rows != 1 {
		return fmt.Errorf("server %q: close %s for vehicle %q: episode %d is not open", w.server.ServerName, c.key.anomaly, c.key.vehicleID, episodeID)
	}
	if c.observation == nil {
		return nil
	}
	return w.link(ctx, episodeID, c.key, *c.observation, evidenceResolution)
}

// link links the vehicle's evidence for this tick to an episode, inserting
// the evidence the first time the tick needs it.
func (w *vehicleTickWriter) link(ctx context.Context, episodeID int64, key vehicleEpisodeKey, obs vehicleObservation, role string) error {
	evidenceID, err := w.evidenceFor(ctx, obs)
	if err != nil {
		return err
	}
	if err := w.q.LinkVehicleAnomalyEpisodeEvidence(ctx, dbgen.LinkVehicleAnomalyEpisodeEvidenceParams{
		VehicleAnomalyEpisodeID:   episodeID,
		VehiclePositionEvidenceID: evidenceID,
		EvidenceRole:              role,
	}); err != nil {
		return fmt.Errorf("server %q: link %s evidence for vehicle %q: %w", w.server.ServerName, role, key.vehicleID, err)
	}
	return nil
}

// response returns the feed's vehicle_position_response for this tick,
// inserting it the first time. Only feeds with a change get one.
func (w *vehicleTickWriter) response(ctx context.Context, source string) (int64, error) {
	if id, ok := w.responses[source]; ok {
		return id, nil
	}
	endpointID, err := w.endpoint(ctx, source)
	if err != nil {
		return 0, err
	}
	feed := w.feeds[source]
	incrementality := gtfsrt.FeedHeader_DIFFERENTIAL.String()
	if feed.fullDataset {
		incrementality = gtfsrt.FeedHeader_FULL_DATASET.String()
	}
	id, err := w.q.InsertVehiclePositionResponse(ctx, dbgen.InsertVehiclePositionResponseParams{
		EndpointID:       endpointID,
		ObservedAt:       feed.observedAt,
		SourceHeaderTime: feed.sourceTimestamp,
		Incrementality:   incrementality,
		PayloadSha256:    nonEmpty(feed.payloadHash),
	})
	if err != nil {
		return 0, fmt.Errorf("server %q: insert vehicle-position response: %w", w.server.ServerName, err)
	}
	w.responses[source] = id
	return id, nil
}

func (w *vehicleTickWriter) endpoint(ctx context.Context, source string) (int64, error) {
	if id, ok := w.endpoints[source]; ok {
		return id, nil
	}
	feed, ok := w.feeds[source]
	if !ok {
		return 0, fmt.Errorf("server %q: no vehicle feed for source %q", w.server.ServerName, source)
	}
	id, err := ensureEndpoint(ctx, w.q, w.serverID, w.server.ServerName, feed.feedURL, feed.source)
	if err != nil {
		return 0, err
	}
	w.endpoints[source] = id
	return id, nil
}

func (w *vehicleTickWriter) vehicle(ctx context.Context, source, vehicleID string) (int64, error) {
	key := vehicleEvidenceKey{source: source, vehicleID: vehicleID}
	if id, ok := w.vehicles[key]; ok {
		return id, nil
	}
	endpointID, err := w.endpoint(ctx, source)
	if err != nil {
		return 0, err
	}
	id, err := ensureVehicle(ctx, w.q, endpointID, vehicleID)
	if err != nil {
		return 0, err
	}
	w.vehicles[key] = id
	return id, nil
}

func (w *vehicleTickWriter) agency(ctx context.Context, agency models.ObaServer) (int64, error) {
	if id, ok := w.agencies[agency.AgencyID]; ok {
		return id, nil
	}
	id, err := ensureAgency(ctx, w.q, w.serverID, w.server.ServerName, agency.AgencyID, agency.AgencyName)
	if err != nil {
		return 0, err
	}
	w.agencies[agency.AgencyID] = id
	return id, nil
}

// evidenceFor returns the vehicle's evidence row for this tick, inserting it
// the first time.
func (w *vehicleTickWriter) evidenceFor(ctx context.Context, obs vehicleObservation) (int64, error) {
	key := vehicleEvidenceKey{source: obs.source, vehicleID: obs.vehicleID}
	if id, ok := w.evidence[key]; ok {
		return id, nil
	}
	responseID, err := w.response(ctx, obs.source)
	if err != nil {
		return 0, err
	}
	vehicleID, err := w.vehicle(ctx, obs.source, obs.vehicleID)
	if err != nil {
		return 0, err
	}
	params := evidenceParams(obs, w.feeds[obs.source].observedAt, w.h.scheduleStore())
	params.VehiclePositionResponseID = responseID
	params.VehicleEntityID = &vehicleID
	params.GtfsStaticVersionID = w.staticVersionID
	if obs.facts.attributionReason == "" && obs.facts.agency.AgencyID != "" {
		agencyID, err := w.agency(ctx, obs.facts.agency)
		if err != nil {
			return 0, err
		}
		params.AgencyID = &agencyID
	}
	if w.needsBox[key] {
		params.BoundingBoxVersionID = obs.facts.boxVersionID
	}
	id, err := w.q.InsertVehiclePositionEvidence(ctx, params)
	if err != nil {
		return 0, fmt.Errorf("server %q: insert evidence for vehicle %q: %w", w.server.ServerName, obs.vehicleID, err)
	}
	w.evidence[key] = id
	return id, nil
}

// scheduleStore returns the compiled schedules, or nil before SetStaticStore.
func (h *HistoryService) scheduleStore() *gtfs.ScheduleStore {
	if h.staticStore == nil {
		return nil
	}
	return h.staticStore.ScheduleStore()
}

// evidenceParams fills the evidence columns that come from the vehicle
// itself. The caller sets the identity, version and agency columns.
func evidenceParams(obs vehicleObservation, observedAt time.Time, schedules *gtfs.ScheduleStore) dbgen.InsertVehiclePositionEvidenceParams {
	vehicle := obs.facts.vehicle
	params := dbgen.InsertVehiclePositionEvidenceParams{
		VehicleSourceTime:     vehicle.Timestamp,
		Latitude:              obs.facts.latitude,
		Longitude:             obs.facts.longitude,
		PositionValid:         obs.positionReason == "",
		ComputedSpeedMps:      obs.facts.computedSpeedMps,
		SpeedDiscrepancyRatio: obs.facts.speedDiscrepancyRatio,
	}
	if obs.positionReason != "" {
		params.PositionValidationReason = nonEmpty(string(obs.positionReason))
	}
	if obs.facts.attributionReason != "" {
		params.AttributionReason = nonEmpty(string(obs.facts.attributionReason))
	}
	if vehicle.Position != nil {
		params.ReportedSpeedMps = float32Ptr(vehicle.Position.Speed)
	}
	if vehicle.CurrentStatus != nil {
		status := int16(*vehicle.CurrentStatus)
		params.CurrentStatus = &status
	}
	if vehicle.Trip != nil {
		fillTripEvidence(&params, vehicle.Trip.ID)
	}
	fillScheduleEvidence(&params, obs, observedAt, schedules)
	return params
}

// fillTripEvidence copies the realtime trip descriptor into the evidence.
func fillTripEvidence(params *dbgen.InsertVehiclePositionEvidenceParams, trip remoteGtfs.TripID) {
	params.TripID = nonEmpty(trip.ID)
	params.RouteID = nonEmpty(trip.RouteID)
	if trip.HasStartDate {
		y, m, d := trip.StartDate.Date()
		params.TripStartDate = pgtype.Date{Time: time.Date(y, m, d, 0, 0, 0, 0, time.UTC), Valid: true}
	}
	if trip.HasStartTime {
		seconds := int32(trip.StartTime / time.Second)
		params.TripStartTimeSeconds = &seconds
	}
	relationship := trip.ScheduleRelationship.String()
	params.ScheduleRelationship = &relationship
}

// fillScheduleEvidence classifies the trip against the attributed agency's
// compiled schedule. The columns stay null when the vehicle is unattributed
// or no schedule is available.
func fillScheduleEvidence(params *dbgen.InsertVehiclePositionEvidenceParams, obs vehicleObservation, observedAt time.Time, schedules *gtfs.ScheduleStore) {
	agency := obs.facts.agency
	if schedules == nil || obs.facts.attributionReason != "" || agency.AgencyID == "" {
		return
	}
	descriptor := gtfs.TripDescriptor{TripID: obs.tripID, StartDate: obs.tripStartDate}
	if trip := obs.facts.vehicle.Trip; trip != nil && trip.ID.HasStartTime {
		descriptor.StartTime = gtfsClock(trip.ID.StartTime)
	}
	classification := schedules.ClassifyTrip(models.ServerKey(agency.ObaBaseURL, agency.AgencyID), descriptor, observedAt)
	if !classification.ScheduleAvailable {
		return
	}
	state := string(classification.State)
	reason := string(classification.Reason)
	params.ExpectedScheduleState = &state
	params.ExpectedScheduleReason = &reason
	if !classification.WindowStart.IsZero() {
		start, end := classification.WindowStart.UTC(), classification.WindowEnd.UTC()
		params.ExpectedWindowStart = &start
		params.ExpectedWindowEnd = &end
	}
}

// gtfsClock formats a duration after service-day midnight as GTFS HH:MM:SS,
// where the hour may exceed 23.
func gtfsClock(d time.Duration) string {
	seconds := int64(d / time.Second)
	return fmt.Sprintf("%02d:%02d:%02d", seconds/3600, seconds%3600/60, seconds%60)
}

func float32Ptr(v *float32) *float64 {
	if v == nil {
		return nil
	}
	f := float64(*v)
	return &f
}

func nonEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// vehicleDeparture is one open episode closed because its server or feed left
// the configuration.
type vehicleDeparture struct {
	episodeID int64
	key       vehicleEpisodeKey
}

// CloseOpenVehicleEpisodes closes the open vehicle-anomaly episodes whose
// server or feed is no longer configured, and drops the memory of the servers
// they were held under, so an episode cannot stay open until retention. It
// serves configuration refreshes (app.OnConfigUpdated) and the startup
// reconcile alike: both close feeds that left, and the startup pass also finds
// servers that left while Watchdog was down by reading the open episodes from
// the database. A failed close stays pending there and is retried by the next
// call. Episodes of legacy endpoints with no known source are only closed when
// their whole server is gone: a still-configured feed cannot be identified
// from the stored URL, so it must not be blamed for their removal.
// It returns whether any episode was closed.
func (h *HistoryService) CloseOpenVehicleEpisodes(ctx context.Context, servers []models.ObaServer) bool {
	if h == nil || h.pool == nil || h.vehicles == nil {
		return false
	}
	// base URL → the feed sources the current configuration still fetches.
	// Every entry counts: a server-scoped entry fetches its feeds too, so its
	// replaced feeds must close like any other entry's.
	configured := make(map[string]map[string]struct{})
	serverByURL := make(map[string]models.ObaServer)
	for _, server := range servers {
		baseURL := utils.SanitizeServerURL(server.ObaBaseURL)
		if baseURL == "" {
			continue
		}
		if _, ok := serverByURL[baseURL]; !ok {
			serverByURL[baseURL] = server
		}
		sources := configured[baseURL]
		if sources == nil {
			sources = make(map[string]struct{})
			configured[baseURL] = sources
		}
		for _, feed := range server.GtfsRTFeeds {
			sources[models.FeedSourceKey(feed.VehiclePositionURL)] = struct{}{}
		}
	}

	closed := false
	// In-memory entries first: a config refresh closes feeds that left while
	// Watchdog was running.
	for _, baseURL := range h.vehicles.baseURLs() {
		entry, ok := h.vehicles.lookup(baseURL)
		if !ok {
			continue
		}
		sources, stillConfigured := configured[baseURL]
		if h.closeOpenVehicleEpisodes(ctx, entry, sources, stillConfigured) {
			closed = true
		}
	}
	// The in-memory registry is empty after a restart, and a server that left
	// while Watchdog was down has no entry at all. Read the open episodes from
	// the database so both are handled. This also retries a close that failed
	// earlier; after a successful one there is nothing left to close.
	baseURLs, err := dbgen.New(h.pool).ListServerBaseURLsWithOpenVehicleAnomalies(ctx)
	if err != nil {
		reportWriteFailure(h.logger, vehicleAnomalyLoadStep, writeTags{}, fmt.Errorf("list servers with open vehicle anomaly episodes: %w", err))
		return closed
	}
	for _, baseURL := range baseURLs {
		sources, stillConfigured := configured[baseURL]
		server, ok := serverByURL[baseURL]
		if !ok {
			// The server left the configuration, so its display name is no
			// longer available. The base URL identifies it well enough for a
			// synthetic closing response and for the failure tags.
			server = models.ObaServer{ObaBaseURL: baseURL, ServerName: baseURL}
		}
		entry, found := h.vehicles.lookup(baseURL)
		if !found {
			entry = h.vehicles.forServer(baseURL, server)
		}
		// loadOpenVehicleEpisodes owns entry.open and entry.loaded, and
		// RecordVehiclePositions takes the same lock for its own load.
		entry.mu.Lock()
		loaded := entry.loaded || h.loadOpenVehicleEpisodes(ctx, server, baseURL, entry)
		entry.mu.Unlock()
		if !loaded {
			continue
		}
		if h.closeOpenVehicleEpisodes(ctx, entry, sources, stillConfigured) {
			closed = true
		}
	}
	return closed
}

// closeOpenVehicleEpisodes closes entry's open episodes whose feed source is
// not in keep, then drops the closed ones from memory. stillConfigured is
// false for a server that left the configuration: every one of its episodes
// is then a departure, including those of legacy endpoints with no source.
func (h *HistoryService) closeOpenVehicleEpisodes(ctx context.Context, entry *serverVehicleEpisodes, keep map[string]struct{}, stillConfigured bool) bool {
	entry.mu.Lock()
	departures := collectVehicleDepartures(entry.open, keep, stillConfigured)
	entry.mu.Unlock()
	if len(departures) == 0 {
		return false
	}
	if !h.closeDepartedVehicleEpisodes(ctx, entry.server, departures...) {
		// The commit may have reached the database even though the client
		// saw an error, so memory can no longer be trusted. Reload before the
		// next close, as a failed normal write does in RecordVehiclePositions.
		entry.mu.Lock()
		entry.loaded = false
		entry.mu.Unlock()
		return false
	}
	entry.mu.Lock()
	for _, departure := range departures {
		delete(entry.open, departure.key)
	}
	empty := len(entry.open) == 0
	if empty {
		h.vehicles.remove(utils.SanitizeServerURL(entry.server.ObaBaseURL))
	}
	entry.mu.Unlock()
	return true
}

// collectVehicleDepartures gathers the open episodes whose source is not in
// keep, sorted for a deterministic write. A server that is no longer
// configured departs whole, so every episode counts. On a configured server
// an unknown source is kept: it belongs to a pre-upgrade endpoint whose query
// string was never stored, and closing it would blame the wrong feed.
func collectVehicleDepartures(open map[vehicleEpisodeKey]*openVehicleEpisode, keep map[string]struct{}, stillConfigured bool) []vehicleDeparture {
	var departures []vehicleDeparture
	for key, episode := range open {
		if stillConfigured {
			if !sourceIsKnown(key.source) {
				continue
			}
			if _, ok := keep[key.source]; ok {
				continue
			}
		}
		departures = append(departures, vehicleDeparture{episodeID: episode.episodeID, key: key})
	}
	sort.Slice(departures, func(i, j int) bool { return departures[i].key.less(departures[j].key) })
	return departures
}

// closeDepartedVehicleEpisodes closes the given episodes against a synthetic
// response of the feed that left the configuration, with the explicit close
// reason of a server or feed that left, and reports whether the write
// committed. The trigger requires the closing response to come from the
// episode's own endpoint and to be observed at or after its opening response;
// a synthetic response satisfies both, because the feed is gone. The episodes
// leave memory through the caller once this write commits.
func (h *HistoryService) closeDepartedVehicleEpisodes(ctx context.Context, server models.ObaServer, departures ...vehicleDeparture) bool {
	if len(departures) == 0 {
		return true
	}
	return h.write(ctx, vehicleAnomalyFlushStep, serverTags(server), func(q *dbgen.Queries) error {
		for _, departure := range departures {
			if err := closeVehicleDeparture(ctx, q, server, departure); err != nil {
				return err
			}
		}
		return nil
	})
}

// closeVehicleDeparture closes one episode whose server or feed left the
// configuration, against a synthetic response of the feed that was open.
func closeVehicleDeparture(ctx context.Context, q *dbgen.Queries, server models.ObaServer, departure vehicleDeparture) error {
	episode, err := q.GetVehicleAnomalyEpisode(ctx, departure.episodeID)
	if err != nil {
		return fmt.Errorf("server %q: load vehicle-anomaly episode %d: %w", server.ServerName, departure.episodeID, err)
	}
	closingID, err := q.InsertVehiclePositionResponse(ctx, dbgen.InsertVehiclePositionResponseParams{
		EndpointID:     episode.VehicleAnomalyEpisodeView.EndpointID,
		ObservedAt:     time.Now().UTC(),
		Incrementality: gtfsrt.FeedHeader_FULL_DATASET.String(),
	})
	if err != nil {
		return fmt.Errorf("server %q: insert closing response of vehicle-anomaly episode %d: %w", server.ServerName, departure.episodeID, err)
	}
	rows, err := q.CloseVehicleAnomalyEpisode(ctx, dbgen.CloseVehicleAnomalyEpisodeParams{
		ClosingResponseID:       closingID,
		CloseReason:             string(closeFeedRemoved),
		VehicleAnomalyEpisodeID: departure.episodeID,
	})
	if err != nil {
		return fmt.Errorf("server %q: close vehicle-anomaly episode %d: %w", server.ServerName, departure.episodeID, err)
	}
	if rows != 1 {
		return fmt.Errorf("server %q: close vehicle-anomaly episode %d: not open", server.ServerName, departure.episodeID)
	}
	return nil
}
