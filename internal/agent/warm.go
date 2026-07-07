package agent

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	realtime "github.com/Foony-Limited/realtime-go"

	"github.com/Foony-Limited/foony-sync/internal/spec"
)

// failedRetryDelay is how long the warm listener waits before retrying a
// terminally failed connection (a rejected credential). Transient drops are
// not affected: the SDK reconnects those itself with backoff.
const failedRetryDelay = 30 * time.Second

// runWarmListener keeps a subscription to the app's dbsync:warm channel alive
// through the realtime SDK, feeding each signal to handle with its kind
// ("warm" or "touch"). The SDK owns reconnect backoff and keep-alive probing;
// a broken warm path degrades cold-doc latency but never stops the
// replication side.
func runWarmListener(ctx context.Context, logger *slog.Logger, foonyURL string, client *FoonyClient, handle func(kind, channel string)) {
	// Key auth (not a minted token) is what opens the reserved dbsync:
	// namespace.
	realtimeClient, err := realtime.New(realtime.Options{
		Endpoint: foonyURL,
		Key:      client.AgentKey(),
		ClientID: "foony-sync",
	})
	if err != nil {
		logger.Error("warm listener disabled: bad realtime options", "error", err.Error())
		return
	}
	defer realtimeClient.Close()

	// Log drops and their reasons: transient disconnects self-heal, but the
	// operator should see why the warm path was down.
	realtimeClient.Connection.On(func(change realtime.ConnectionStateChange) {
		if change.Reason != nil && ctx.Err() == nil {
			logger.Warn("warm listener connection state", "state", string(change.Current), "error", change.Reason.Error())
		}
	})
	// A terminal failure (a rejected credential) stops the SDK's own retries.
	// Keys can be rotated back or restored on the dashboard, so keep retrying
	// on a slow cadence instead of giving up for the process lifetime.
	failed := make(chan struct{}, 1)
	realtimeClient.Connection.OnState(realtime.ConnectionFailed, func(realtime.ConnectionStateChange) {
		select {
		case failed <- struct{}{}:
		default:
		}
	})

	channel := realtimeClient.Channels.Get(spec.SyncWarmChannel)
	channel.Subscribe(func(message *realtime.Message) {
		var request spec.SyncWarmRequest
		if err := json.Unmarshal(message.Data, &request); err != nil || request.Channel == "" {
			return
		}
		handle(message.Name, request.Channel)
	}, "warm", "touch")

	for ctx.Err() == nil {
		if err := realtimeClient.Connect(ctx); err != nil && ctx.Err() == nil {
			logger.Warn("warm listener connect failed, retrying", "error", err.Error(), "retryIn", failedRetryDelay.String())
		} else {
			// Connected. The SDK re-subscribes and heals transient drops on
			// its own, so only a terminal failure needs us again.
			select {
			case <-ctx.Done():
				return
			case <-failed:
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(failedRetryDelay):
		}
	}
}
