package extensions

import (
	"github.com/trufnetwork/node/extensions/tn_cache"
	"github.com/trufnetwork/node/extensions/tn_digest"
)

func init() {
	tn_cache.InitializeExtension()
	tn_digest.InitializeExtension()
}
