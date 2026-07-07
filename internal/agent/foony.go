package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	realtime "github.com/Foony-Limited/realtime-go"

	"github.com/Foony-Limited/foony-sync/internal/spec"
)

// FoonyClient talks to foony for the agent: the dbsync control endpoints
// (heartbeats and live-doc listing) over plain REST, and doc publishes through
// the realtime SDK's Rest client, all authenticated with the source's api key.
type FoonyClient struct {
	baseURL string
	// username/password are the Basic credentials derived from the agent key
	// (appSlug.keyId as user, secret as password).
	username string
	password string
	http     *http.Client
	// rest is the realtime SDK client doc publishes go through.
	rest *realtime.Rest
}

// Heartbeat is the body of POST /sync/heartbeat. Queries is a name-and-tables
// summary only: the definitions stay in the agent's local config.
type Heartbeat struct {
	AgentVersion string              `json:"agentVersion"`
	State        string              `json:"state"`
	SlotLagBytes int64               `json:"slotLagBytes"`
	DirtyDepth   int64               `json:"dirtyDepth"`
	LastEventAt  *time.Time          `json:"lastEventAt,omitempty"`
	LastError    string              `json:"lastError,omitempty"`
	Queries      []spec.QuerySummary `json:"queries"`
}

// NewFoonyClient parses the agent key (appSlug.keyId:secret) into Basic
// credentials for baseURL.
func NewFoonyClient(baseURL, agentKey string) (*FoonyClient, error) {
	username, password, ok := strings.Cut(strings.TrimSpace(agentKey), ":")
	if !ok || username == "" || password == "" || !strings.Contains(username, ".") {
		return nil, fmt.Errorf("dbsync: FOONY_SYNC_KEY must look like appSlug.keyId:secret")
	}
	rest, err := realtime.NewRest(realtime.RestOptions{
		Endpoint: baseURL,
		Key:      username + ":" + password,
	})
	if err != nil {
		return nil, fmt.Errorf("dbsync: realtime rest client: %w", err)
	}
	return &FoonyClient{
		baseURL:  strings.TrimSuffix(baseURL, "/"),
		username: username,
		password: password,
		http:     &http.Client{Timeout: 30 * time.Second},
		rest:     rest,
	}, nil
}

// KeyID returns the public key id of the agent credential, the stable local
// identity the replication slot name derives from.
func (client *FoonyClient) KeyID() string {
	_, keyID, _ := strings.Cut(client.username, ".")
	return keyID
}

// LiveDocs lists the db: channels that currently have subscribers, gathered
// from the edge fleet. The bool reports whether the view is complete; a
// truncated view must only extend the agent's live set, never shrink it.
func (client *FoonyClient) LiveDocs(ctx context.Context) ([]string, bool, error) {
	var response struct {
		Channels []string `json:"channels"`
		Complete bool     `json:"complete"`
	}
	if err := client.call(ctx, http.MethodGet, "/sync/live-docs", nil, &response); err != nil {
		return nil, false, err
	}
	return response.Channels, response.Complete, nil
}

// SendHeartbeat reports agent health to the source's dashboard row.
func (client *FoonyClient) SendHeartbeat(ctx context.Context, heartbeat Heartbeat) error {
	return client.call(ctx, http.MethodPost, "/sync/heartbeat", heartbeat, nil)
}

// PublishDoc publishes a computed doc to its db: channel through the realtime
// SDK. The channel's built-in retained-last policy makes this both the live
// fan-out and the stored snapshot new subscribers replay.
func (client *FoonyClient) PublishDoc(ctx context.Context, channel string, doc json.RawMessage) error {
	if _, err := client.rest.Channels.Get(channel).Publish(ctx, "doc", doc); err != nil {
		return fmt.Errorf("dbsync: publish %s: %w", channel, err)
	}
	return nil
}

// AgentKey returns the full credential (appSlug.keyId:secret) for the
// WebSocket handshake, which authenticates with the key directly: key auth is
// what opens the reserved dbsync: namespace.
func (client *FoonyClient) AgentKey() string {
	return client.username + ":" + client.password
}

// call runs one JSON request/response round-trip with Basic auth.
func (client *FoonyClient) call(ctx context.Context, method, path string, requestBody, responseBody any) error {
	var reader io.Reader
	if requestBody != nil {
		encoded, err := json.Marshal(requestBody)
		if err != nil {
			return fmt.Errorf("dbsync: encode %s: %w", path, err)
		}
		reader = bytes.NewReader(encoded)
	}
	request, err := http.NewRequestWithContext(ctx, method, client.baseURL+path, reader)
	if err != nil {
		return fmt.Errorf("dbsync: request %s: %w", path, err)
	}
	request.SetBasicAuth(client.username, client.password)
	if requestBody != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := client.http.Do(request)
	if err != nil {
		return fmt.Errorf("dbsync: %s: %w", path, err)
	}
	defer response.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(response.Body, 4<<20))
	if err != nil {
		return fmt.Errorf("dbsync: read %s: %w", path, err)
	}
	if response.StatusCode >= 400 {
		return fmt.Errorf("dbsync: %s: HTTP %d: %s", path, response.StatusCode, compactError(payload))
	}
	if responseBody != nil {
		if err := json.Unmarshal(payload, responseBody); err != nil {
			return fmt.Errorf("dbsync: decode %s: %w", path, err)
		}
	}
	return nil
}

// compactError trims an error body to one loggable line.
func compactError(payload []byte) string {
	text := strings.TrimSpace(string(payload))
	if len(text) > 300 {
		text = text[:300] + "..."
	}
	return text
}
