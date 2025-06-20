package version

import (
	"time"

	kwilVersion "github.com/kwilteam/kwil-db/version"
)

// These variables can be overridden at build time with ldflags
var (
	// TN-specific version info that can be set via ldflags
	TNVersion   string // -X github.com/trufnetwork/node/cmd/version.TNVersion=...
	TNCommit    string // -X github.com/trufnetwork/node/cmd/version.TNCommit=...
	TNBuildTime string // -X github.com/trufnetwork/node/cmd/version.TNBuildTime=...
)

// getVersion returns the TN version if set, otherwise falls back to kwil-db version
func getVersion() string {
	if TNVersion != "" {
		return TNVersion
	}
	return kwilVersion.KwilVersion
}

// getCommit returns the TN commit (short form) if set, otherwise falls back to kwil-db commit
func getCommit() string {
	var commit string
	if TNCommit != "" {
		commit = TNCommit
	} else if kwilVersion.Build != nil {
		commit = kwilVersion.Build.Revision
	}

	// Return short form (9 chars) for readability
	const shortHashLength = 9
	if len(commit) > shortHashLength {
		return commit[:shortHashLength]
	}
	return commit
}

// getBuildTime returns the TN build time if set, otherwise falls back to kwil-db build time
func getBuildTime() time.Time {
	if TNBuildTime != "" {
		if t, err := time.Parse(time.RFC3339, TNBuildTime); err == nil {
			return t
		}
	}
	if kwilVersion.Build != nil {
		return kwilVersion.Build.RevTime
	}
	return time.Time{}
}

// getBuildTimeDisplay returns a formatted build time with context about whether it's commit or build time
func getBuildTimeDisplay() string {
	buildTime := getBuildTime()
	if buildTime.IsZero() {
		return "unknown"
	}

	// If we have a custom TN build time, it means we used our logic (commit time if clean, build time if dirty)
	if TNBuildTime != "" {
		// Check if workspace was dirty by looking at the version string
		if TNVersion != "" && len(TNVersion) > 5 && TNVersion[len(TNVersion)-5:] == "dirty" {
			return buildTime.Format(time.RFC3339) + " (build time)"
		}
		return buildTime.Format(time.RFC3339) + " (commit time)"
	}

	// Fallback to kwil-db build time (always commit time)
	return buildTime.Format(time.RFC3339) + " (commit time)"
}
