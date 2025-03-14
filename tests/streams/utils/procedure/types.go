package procedure

import (
	kwilTesting "github.com/kwilteam/kwil-db/testing"
	"github.com/trufnetwork/sdk-go/core/types"
)

type GetRecordInput struct {
	Platform      *kwilTesting.Platform
	StreamLocator types.StreamLocator
	FromTime      int64
	ToTime        int64
	FrozenAt      int64
	Height        int64
}

type GetIndexInput struct {
	Platform      *kwilTesting.Platform
	StreamLocator types.StreamLocator
	FromTime      int64
	ToTime        int64
	FrozenAt      int64
	Height        int64
	BaseTime      int64
	Interval      int
}

type ResultRow []string

type GetIndexChangeInput struct {
	Platform      *kwilTesting.Platform
	StreamLocator types.StreamLocator
	FromTime      int64
	ToTime        int64
	FrozenAt      int64
	Height        int64
	BaseTime      int64
	Interval      int
}

type GetFirstRecordInput struct {
	Platform      *kwilTesting.Platform
	StreamLocator types.StreamLocator
	AfterTime     int64
	FrozenAt      int64
	Height        int64
}

type SetMetadataInput struct {
	Platform      *kwilTesting.Platform
	StreamLocator types.StreamLocator
	Key           string
	Value         string
	ValType       string
	Height        int64
}

type SetTaxonomyInput struct {
	Platform      *kwilTesting.Platform
	StreamLocator types.StreamLocator
	DataProviders []string
	StreamIds     []string
	Weights       []string
	StartTime     int64
	Height        int64
}

type GetCategoryStreamsInput struct {
	Platform     *kwilTesting.Platform
	DataProvider string
	StreamId     string
	ActiveFrom   *int64
	ActiveTo     *int64
}
