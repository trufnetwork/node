package tn_vacuum

const (
	// ExtensionName is used for hook registration and config namespace.
	ExtensionName = "tn_vacuum"

	TriggerDigestCoupled = "digest_coupled"
	TriggerBlockInterval = "block_interval"
	TriggerCron          = "cron"
	TriggerManual        = "manual"
)

const (
	defaultTrigger      = TriggerManual
	defaultReloadBlocks = int64(1000)
	minBlockInterval    = int64(1)
)
