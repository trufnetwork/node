package benchmark

import (
	"time"
)

var (
	readerAddress    = MustNewEthereumAddressFromString("0x0000000000000000010000000000000000000001")
	deployer         = MustNewEthereumAddressFromString("0x0000000000000000000000000000000200000000")
	fixedDate        = time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
	maxDepth         = 179 // found empirically
	insertFreqInTime = 1 * time.Minute
)
