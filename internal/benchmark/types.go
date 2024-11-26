package benchmark

import (
	"time"

	"github.com/trufnetwork/sdk-go/core/util"
)

type (
	ProcedureEnum string
	BenchmarkCase struct {
		QtyStreams      int
		BranchingFactor int
		Days            []int
		Visibility      util.VisibilityEnum
		Samples         int
		Procedures      []ProcedureEnum
	}
	Result struct {
		Case          BenchmarkCase
		MaxDepth      int
		MemoryUsage   uint64
		Procedure     ProcedureEnum
		DaysQueried   int
		CaseDurations []time.Duration
	}
)

const (
	ProcedureGetRecord      ProcedureEnum = "get_record"
	ProcedureGetIndex       ProcedureEnum = "get_index"
	ProcedureGetChangeIndex ProcedureEnum = "get_index_change"
	ProcedureGetFirstRecord ProcedureEnum = "get_first_record"
)
