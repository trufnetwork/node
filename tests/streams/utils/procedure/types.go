package procedure

import (
	kwilTesting "github.com/kwilteam/kwil-db/testing"
	"github.com/trufnetwork/sdk-go/core/types"
)

type GetRecordInput struct {
	Platform      *kwilTesting.Platform
	StreamLocator types.StreamLocator
	FromTime      *int64
	ToTime        *int64
	FrozenAt      *int64
	Height        int64
	PrintLogs     *bool
}

type GetIndexInput struct {
	Platform      *kwilTesting.Platform
	StreamLocator types.StreamLocator
	FromTime      *int64
	ToTime        *int64
	FrozenAt      *int64
	Height        int64
	BaseTime      *int64
}

type ResultRow []string

type GetIndexChangeInput struct {
	Platform      *kwilTesting.Platform
	StreamLocator types.StreamLocator
	FromTime      *int64
	ToTime        *int64
	FrozenAt      *int64
	Height        int64
	BaseTime      *int64
	Interval      *int
}

type GetFirstRecordInput struct {
	Platform      *kwilTesting.Platform
	StreamLocator types.StreamLocator
	AfterTime     *int64
	FrozenAt      *int64
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
	StartTime     *int64
	Height        int64
}

type GetCategoryStreamsInput struct {
	Platform     *kwilTesting.Platform
	DataProvider string
	StreamId     string
	ActiveFrom   *int64
	ActiveTo     *int64
}

type FilterStreamsByExistenceInput struct {
	Platform       *kwilTesting.Platform
	StreamLocators []types.StreamLocator
	ExistingOnly   *bool
	Height         int64
}

type DisableTaxonomyInput struct {
	Platform      *kwilTesting.Platform
	StreamLocator types.StreamLocator
	GroupSequence int
	Height        int64
}

type GrantRoleInput struct {
	Platform *kwilTesting.Platform
	Owner    string
	RoleName string
	Wallet   string
}

type RevokeRoleInput struct {
	Platform *kwilTesting.Platform
	Owner    string
	RoleName string
	Wallet   string
}

type IsMemberOfInput struct {
	Platform *kwilTesting.Platform
	Owner    string
	RoleName string
	Wallet   string
}
