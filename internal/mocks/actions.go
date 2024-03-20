package mocks

import (
	"github.com/kwilteam/kwil-db/common"
	"github.com/kwilteam/kwil-db/extensions/precompiles"
)

type Instance interface {
	Call(scoper *precompiles.ProcedureContext, app *common.App, method string, inputs []any) ([]any, error)
}
