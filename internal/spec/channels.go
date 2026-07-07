package spec

// The reserved channel namespaces the agent talks to. These mirror the
// realtime service's channel grammar (its wire package is the canonical
// source): db: carries computed docs, dbsync: carries agent control traffic.

// SyncDocPrefix is the reserved namespace Database Sync docs live on. The doc
// for a query row lives on "db:<query>[:<key>...]".
const SyncDocPrefix = "db:"

// SyncWarmChannel is where the edge asks an app's sync agents to compute a doc
// that had no retained value on subscribe. Channels are app-scoped, so the
// name needs no app id; every agent of the app subscribes and picks the
// requests whose query name it owns.
const SyncWarmChannel = "dbsync:warm"

// SyncWarmRequest is the payload of a message on SyncWarmChannel.
type SyncWarmRequest struct {
	// Channel is the cold db: doc channel a client just subscribed to.
	Channel string `json:"channel"`
}

// IsSyncDocChannel reports whether channel is under the reserved db: doc
// namespace.
func IsSyncDocChannel(channel string) bool {
	return len(channel) >= len(SyncDocPrefix) && channel[:len(SyncDocPrefix)] == SyncDocPrefix
}
