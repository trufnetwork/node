package hello

import (
	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/types"
	"github.com/trufnetwork/kwil-db/extensions/precompiles"
)

// init registers the `hello` precompile extension with kwild. The extension
// exposes a single `greet()` method that returns the string "hello". This is
// sufficient for testing the graceful-fallback mechanics because it is a
// deterministic, side-effect-free SYSTEM/PUBLIC method.
func init() {
	err := precompiles.RegisterPrecompile("hello", precompiles.Precompile{
		Methods: []precompiles.Method{
			{
				Name:            "greet", // called as hello.greet()
				AccessModifiers: []precompiles.Modifier{precompiles.PUBLIC, precompiles.VIEW},
				Returns: &precompiles.MethodReturn{
					IsTable: false,
					Fields: []precompiles.PrecompileValue{
						precompiles.NewPrecompileValue("msg", types.TextType, false),
					},
				},
				Handler: func(_ *common.EngineContext, _ *common.App, _ []any, resultFn func([]any) error) error {
					// Return a single-row, single-column result: "hello"
					return resultFn([]any{"hello"})
				},
			},
		},
	})
	if err != nil {
		// Registration errors are fatal at startup because they indicate a
		// programming error (e.g., duplicate registration). Panicking here is
		// consistent with other extension registration code in the codebase.
		panic(err)
	}
}
