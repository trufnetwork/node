package benchmark

import (
	"time"

	"github.com/truflation/tsn-sdk/core/util"
)

type (
	ProcedureEnum string
	BenchmarkCase struct {
		Depth      int
		Days       int
		Visibility util.VisibilityEnum
		Procedure  ProcedureEnum
		Samples    int
	}
	Result struct {
		Case          BenchmarkCase
		CaseDurations []time.Duration
	}
)

const (
	ProcedureGetRecord      ProcedureEnum = "get_record"
	ProcedureGetIndex       ProcedureEnum = "get_index"
	ProcedureGetChangeIndex ProcedureEnum = "get_index_change"
)
