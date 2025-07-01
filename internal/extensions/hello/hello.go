package hello

import (
	"log"

	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/types"
	"github.com/trufnetwork/kwil-db/extensions/precompiles"
)

// init registers the `hello` precompile extension with kwild. The extension
// exposes a single `greet()` method that returns the string "hello". This is
// sufficient for testing the graceful-fallback mechanics because it is a
// deterministic, side-effect-free SYSTEM/PUBLIC method.
func init() {
	log.Println("hello extension initialized")
	err := precompiles.RegisterPrecompile("hello", precompiles.Precompile{
		Methods: []precompiles.Method{
			{
				Name:            "greet", // called as hello.greet()
				AccessModifiers: []precompiles.Modifier{precompiles.PUBLIC, precompiles.VIEW},
				Returns: &precompiles.MethodReturn{
					// Return a table; the test will verify multiple rows are produced.
					IsTable: true,
					Fields: []precompiles.PrecompileValue{
						precompiles.NewPrecompileValue("msg", types.TextType, false),
					},
				},
				Handler: func(_ *common.EngineContext, _ *common.App, _ []any, resultFn func([]any) error) error {
					msgs := []string{"hello", "hola", "bonjour"}
					for _, m := range msgs {
						if err := resultFn([]any{m}); err != nil {
							return err
						}
					}
					return nil
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
