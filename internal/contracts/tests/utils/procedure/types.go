package procedure

import kwilTesting "github.com/kwilteam/kwil-db/testing"

type GetRecordOrIndexInput struct {
	Platform *kwilTesting.Platform
	DBID     string
	DateFrom string
	DateTo   string
	Height   int64
}

type ResultRow []string

type GetIndexChangeInput struct {
	Platform *kwilTesting.Platform
	DBID     string
	DateFrom string
	DateTo   string
	Interval int
	FrozenAt *string
	Height   int64
}
