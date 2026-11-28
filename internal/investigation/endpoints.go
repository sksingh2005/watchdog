package investigation

import (
	"context"
	"net/http"
	"net/url"
	"strconv"

	"github.com/julienschmidt/httprouter"
	"watchdog.onebusaway.org/internal/db/dbgen"
	"watchdog.onebusaway.org/internal/models"
	"watchdog.onebusaway.org/internal/utils"
)

// kindEndpoint is the include kind of a vehicle-position endpoint (E07).
// Every link named endpoint points to one.
const kindEndpoint resourceKind = "endpoint"

// vehiclePositionEndpoint is one vehicle-position feed URL in a response
// (E06, E07).
type vehiclePositionEndpoint struct {
	EndpointID                   int64   `json:"endpoint_id"`
	ServerID                     int64   `json:"server_id"`
	NormalizedVehiclePositionURL string  `json:"normalized_vehicle_position_url"`
	CreatedAt                    apiTime `json:"created_at"`
	// Configured is true when the server's current configuration still
	// fetches this URL, so problems from a retired feed are not blamed on
	// the current one.
	Configured bool  `json:"configured"`
	Links      links `json:"links"`
}

// vehiclePositionEndpointList is the E06 body. It is not paginated: a server
// has few feed URLs.
type vehiclePositionEndpointList struct {
	Items    []vehiclePositionEndpoint `json:"items"`
	Links    links                     `json:"links"`
	Included included                  `json:"included,omitzero"`
}

// endpointIncludable is what include accepts on E06 and E07: an endpoint
// links to one server.
var endpointIncludable = map[string]resourceKind{"server": kindServer}

// listServerVehiclePositionEndpoints serves
// GET /v1/servers/:server_id/vehicle-position-endpoints: every
// vehicle-position feed URL recorded for one server, by endpoint_id
// ascending, including URLs no longer configured. An unknown server is a
// 404, not an empty list.
func (s *InvestigationService) listServerVehiclePositionEndpoints(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	serverID, ok := pathID(ps, "server_id")
	if !ok {
		writeNotFound(w)
		return
	}
	var bad invalidParams
	values := parseQuery(r, &bad, "include")
	inc := s.parseInclude(values, endpointIncludable, &bad)
	if len(bad) > 0 {
		writeInvalidParams(w, bad)
		return
	}

	var body vehiclePositionEndpointList
	err := s.read(r.Context(), func(ctx context.Context, q *dbgen.Queries) error {
		// GetServer returns pgx.ErrNoRows for an unknown server, which fail
		// turns into a 404. Its base URL selects the configuration entries
		// that decide configured.
		server, err := q.GetServer(ctx, serverID)
		if err != nil {
			return err
		}
		rows, err := q.ListServerVehiclePositionEndpoints(ctx, serverID)
		if err != nil {
			return err
		}
		configured := configuredVehiclePositionSources(s.Servers(), server.NormalizedBaseUrl)
		endpoints := make([]dbgen.VehiclePositionEndpoint, 0, len(rows))
		for _, row := range rows {
			endpoints = append(endpoints, dbgen.VehiclePositionEndpoint{
				EndpointID:                   row.EndpointID,
				ServerID:                     row.ServerID,
				NormalizedVehiclePositionUrl: row.NormalizedVehiclePositionUrl,
				FeedSourceKey:                row.FeedSourceKey,
				CreatedAt:                    row.CreatedAt,
			})
		}
		items, related := endpointItems(endpoints, configured)
		body = vehiclePositionEndpointList{
			Items: items,
			Links: links{"self": linkWithQuery(r.URL.Path, r.URL.Query())},
		}
		body.Included, err = s.loadIncluded(ctx, q, related, inc)
		return err
	})
	if err != nil {
		s.fail(w, r, err)
		return
	}
	s.writeJSON(w, r, body, cacheShort())
}

// configuredEndpoints holds the two ways a feed can be recognized as still
// configured. New endpoints carry the hash of the full configured URL; legacy
// endpoints created before migration 00006 have a NULL key, so their
// normalized URL is the only identity available for them.
type configuredEndpoints struct {
	sources map[string]bool
	urls    map[string]bool
}

// configuredVehiclePositionSources returns the feed source keys and normalized
// URLs configured for the server at baseURL (a normalized base URL). An
// agency-mode server has one configuration entry per agency, so the feeds of
// every entry for that server count.
func configuredVehiclePositionSources(servers []models.ObaServer, baseURL string) configuredEndpoints {
	configured := configuredEndpoints{
		sources: map[string]bool{},
		urls:    map[string]bool{},
	}
	for _, s := range servers {
		if utils.SanitizeServerURL(s.ObaBaseURL) != baseURL {
			continue
		}
		for _, feed := range s.GtfsRTFeeds {
			if feed.VehiclePositionURL == "" {
				continue
			}
			configured.sources[models.FeedSourceKey(feed.VehiclePositionURL)] = true
			if u := utils.SanitizeServerURL(feed.VehiclePositionURL); u != "" {
				configured.urls[u] = true
			}
		}
	}
	return configured
}

// endpointItems maps rows to their representations, and returns each item's
// links for loadIncluded. configured holds what the server's configuration
// still fetches. The items are never nil, so an empty list encodes as [] rather
// than null.
func endpointItems(rows []dbgen.VehiclePositionEndpoint, configured configuredEndpoints) ([]vehiclePositionEndpoint, []itemLinks) {
	items := make([]vehiclePositionEndpoint, 0, len(rows))
	related := make([]itemLinks, 0, len(rows))
	for _, row := range rows {
		l := endpointLinks(row)
		items = append(items, endpointRepresentation(row, l, configured))
		related = append(related, l)
	}
	return items, related
}

// endpointLinks is everything one endpoint links to that can be included,
// or is included elsewhere: itself and its server.
func endpointLinks(row dbgen.VehiclePositionEndpoint) itemLinks {
	return itemLinks{
		"self": {
			Kind: kindEndpoint,
			ID:   row.EndpointID,
			URL:  "/v1/vehicle-position-endpoints/" + strconv.FormatInt(row.EndpointID, 10),
		},
		"server": {
			Kind: kindServer,
			ID:   row.ServerID,
			URL:  "/v1/servers/" + strconv.FormatInt(row.ServerID, 10),
		},
	}
}

// endpointRepresentation maps one row to the representation that E06 lists
// and E07 returns.
func endpointRepresentation(row dbgen.VehiclePositionEndpoint, l itemLinks, configured configuredEndpoints) vehiclePositionEndpoint {
	out := l.links()
	out["vehicle_anomaly_episodes"] = linkWithQuery("/v1/vehicle-anomaly-episodes",
		url.Values{"endpoint_id": {strconv.FormatInt(row.EndpointID, 10)}})
	isConfigured := false
	if row.FeedSourceKey != nil {
		// The full URL, including its query string, decides identity now:
		// two feeds sharing a normalized URL are different sources.
		isConfigured = configured.sources[*row.FeedSourceKey]
	} else {
		// A legacy row has no source key; its normalized URL is all we have.
		isConfigured = configured.urls[row.NormalizedVehiclePositionUrl]
	}
	return vehiclePositionEndpoint{
		EndpointID:                   row.EndpointID,
		ServerID:                     row.ServerID,
		NormalizedVehiclePositionURL: row.NormalizedVehiclePositionUrl,
		CreatedAt:                    newAPITime(row.CreatedAt),
		Configured:                   isConfigured,
		Links:                        out,
	}
}

// vehiclePositionEndpointBody is the E07 body: one endpoint and, when
// requested, its included server.
type vehiclePositionEndpointBody struct {
	vehiclePositionEndpoint
	Included included `json:"included,omitzero"`
}

// getVehiclePositionEndpoint serves
// GET /v1/vehicle-position-endpoints/:endpoint_id: one vehicle-position feed
// URL, and whether its server's current configuration still fetches it.
func (s *InvestigationService) getVehiclePositionEndpoint(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	id, ok := pathID(ps, "endpoint_id")
	if !ok {
		writeNotFound(w)
		return
	}
	var bad invalidParams
	values := parseQuery(r, &bad, "include")
	inc := s.parseInclude(values, endpointIncludable, &bad)
	if len(bad) > 0 {
		writeInvalidParams(w, bad)
		return
	}

	var body vehiclePositionEndpointBody
	err := s.read(r.Context(), func(ctx context.Context, q *dbgen.Queries) error {
		// pgx.ErrNoRows for an unknown endpoint becomes a 404 in fail.
		row, err := q.GetVehiclePositionEndpoint(ctx, id)
		if err != nil {
			return err
		}
		l := endpointLinks(row.VehiclePositionEndpoint)
		configured := configuredVehiclePositionSources(s.Servers(), row.NormalizedBaseUrl)
		body.vehiclePositionEndpoint = endpointRepresentation(row.VehiclePositionEndpoint, l, configured)
		body.Included, err = s.loadIncluded(ctx, q, []itemLinks{l}, inc)
		return err
	})
	if err != nil {
		s.fail(w, r, err)
		return
	}
	s.writeJSON(w, r, body, cacheShort())
}

// loadVehiclePositionEndpoints is the include loader for kindEndpoint.
func (s *InvestigationService) loadVehiclePositionEndpoints(ctx context.Context, q *dbgen.Queries, ids []int64) (map[int64]any, error) {
	return loadVehiclePositionEndpoints(ctx, q, ids, s.Servers())
}

// loadVehiclePositionEndpoints returns the E07 representation of each
// endpoint in ids, in one query. servers is the current configuration; the
// endpoints can belong to different servers, so configured is worked out
// once per server base URL.
func loadVehiclePositionEndpoints(ctx context.Context, q *dbgen.Queries, ids []int64, servers []models.ObaServer) (map[int64]any, error) {
	rows, err := q.ListVehiclePositionEndpointsByIDs(ctx, ids)
	if err != nil {
		return nil, err
	}
	configuredByServer := map[string]configuredEndpoints{}
	out := make(map[int64]any, len(rows))
	for _, row := range rows {
		configured, ok := configuredByServer[row.NormalizedBaseUrl]
		if !ok {
			configured = configuredVehiclePositionSources(servers, row.NormalizedBaseUrl)
			configuredByServer[row.NormalizedBaseUrl] = configured
		}
		endpoint := row.VehiclePositionEndpoint
		out[endpoint.EndpointID] = endpointRepresentation(endpoint, endpointLinks(endpoint), configured)
	}
	return out, nil
}
