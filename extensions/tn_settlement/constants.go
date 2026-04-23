package tn_settlement

// ExtensionName is the name of the settlement extension
const ExtensionName = "tn_settlement"

// DefaultSettlementSchedule is the default cron schedule for settlement polling
// Format: "0 * * * *" = Every hour (on the hour)
const DefaultSettlementSchedule = "0 * * * *"
