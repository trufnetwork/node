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
