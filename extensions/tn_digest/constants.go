package tn_digest

const (
	ExtensionName         = "tn_digest"
	DefaultDigestSchedule = "0 */6 * * *" // every 6 hours

	// DefaultPruneSchedule matches duplicate_prune_config's own default, so it only
	// ever applies on a network whose row or column is missing.
	DefaultPruneSchedule = "0 */6 * * *" // every 6 hours
)
