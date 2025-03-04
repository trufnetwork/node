package procedure

import kwilTesting "github.com/kwilteam/kwil-db/testing"

type GetRecordInput struct {
	Platform *kwilTesting.Platform
	DBID     string
	DateFrom string
	DateTo   string
	FrozenAt int64
	Height   int64
}

type GetIndexInput struct {
	Platform *kwilTesting.Platform
	DBID     string
	DateFrom string
	DateTo   string
	FrozenAt int64
	Height   int64
	BaseDate string
}

type ResultRow []string

type GetIndexChangeInput struct {
	Platform *kwilTesting.Platform
	DBID     string
	DateFrom string
	DateTo   string
	FrozenAt int64
	Height   int64
	BaseDate string
	Interval int
}

type GetFirstRecordInput struct {
	Platform  *kwilTesting.Platform
	DBID      string
	AfterDate *string
	FrozenAt  int64
	Height    int64
}

type SetMetadataInput struct {
	Platform *kwilTesting.Platform
	DBID     string
	Key      string
	Value    string
	ValType  string
	Height   int64
}

type SetTaxonomyInput struct {
	Platform      *kwilTesting.Platform
	DBID          string
	DataProviders []string
	StreamIds     []string
	Weights       []string
	StartDate     string // Optional start date for taxonomy validity
}
