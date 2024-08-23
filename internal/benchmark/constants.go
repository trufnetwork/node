package benchmark

import (
	"time"

	"github.com/truflation/tsn-sdk/core/util"
)

const (
	RootStreamName       = "primitive"
	ComposedStreamPrefix = "composed"
)

var (
	RootStreamId   = util.GenerateStreamId(RootStreamName)
	readerAddress  = MustNewEthereumAddressFromString("0x0000000000000000010000000000000000000001")
	fixedDate      = time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
	samplesPerCase = 3
)