package benchmark

import (
	"time"

	"github.com/truflation/tsn-sdk/core/util"
)

const (
	RootStreamName       = "primitive"
	ComposedStreamPrefix = "composed"
	filePath             = "./benchmark_results.csv"
)

var (
	RootStreamId   = util.GenerateStreamId(RootStreamName)
	readerAddress  = MustNewEthereumAddressFromString("0x0000000000000000010000000000000000000001")
	fixedDate      = time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
	samplesPerCase = 3
	depths         = []int{0, 1, 10, 50, 100}
	days           = []int{1, 7, 30, 365}
)
