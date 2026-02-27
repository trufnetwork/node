package benchmark

import (
	"time"

	"github.com/trufnetwork/sdk-go/core/util"
	"github.com/trufnetwork/node/tests/streams/utils/cache"
)

type (
	ProcedureEnum string
	BenchmarkCase struct {
		QtyStreams      int
		BranchingFactor int
		DataPointsSet   []int
		Visibility      util.VisibilityEnum
		Samples         int
		Procedures      []ProcedureEnum
		CacheEnabled    bool                      // Whether to enable cache for this benchmark case
		CacheConfig     *cache.CacheOptions  // Optional custom cache configuration
	}
	Result struct {
		Case                   BenchmarkCase
		MaxDepth              int
		MemoryUsage           uint64
		Procedure             ProcedureEnum
		DataPoints            int
		CaseDurations         []time.Duration
		CacheVerified         bool          // Whether cache verification passed
		CachePerformanceDelta time.Duration // Performance difference: cached - non-cached
	}
)

const (
	ProcedureGetRecord      ProcedureEnum = "get_record"
	ProcedureGetIndex       ProcedureEnum = "get_index"
	ProcedureGetChangeIndex ProcedureEnum = "get_index_change"
	ProcedureGetFirstRecord ProcedureEnum = "get_first_record"
	ProcedureGetLastRecord  ProcedureEnum = "get_last_record"
)
