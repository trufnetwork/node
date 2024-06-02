package main

import (
	"fmt"
	"os"

	"github.com/kwilteam/kwil-db/extensions/auth"
	"github.com/kwilteam/kwil-db/extensions/precompiles"
	"github.com/truflation/tsn-db/internal/extensions/composed_stream"
	"github.com/truflation/tsn-db/internal/extensions/ed25519authenticator"
	"github.com/truflation/tsn-db/internal/extensions/mathutil"
	"github.com/truflation/tsn-db/internal/extensions/primitive_stream"
	"github.com/truflation/tsn-db/internal/extensions/realtime"
	"github.com/truflation/tsn-db/internal/extensions/whitelist"

	"github.com/kwilteam/kwil-db/cmd/kwild/root"
)

func main() {
	realTime := realtime.NewRealtimeExtension()
	if err := precompiles.RegisterPrecompile("realtime", realTime.Initialize); err != nil {
		panic(err)
	}

	// TODO: realTime can now be passed elsewhere to other code to add realtime data.
	// below is an example of adding some data for a specific dbid:
	realTime.SetValue("xdbid", 100)

	if err := root.RootCmd().Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	os.Exit(0)
}

func init() {
	err := auth.RegisterAuthenticator("ed25519_example", ed25519authenticator.Ed25519Authenticator{})
	if err != nil {
		panic(err)
	}

	err = precompiles.RegisterPrecompile("mathutil", mathutil.InitializeMathUtil)
	if err != nil {
		panic(err)
	}

	err = precompiles.RegisterPrecompile("composed_stream", composed_stream.InitializeComposedStream)
	if err != nil {
		panic(err)
	}

	err = precompiles.RegisterPrecompile("primitive_stream", primitive_stream.InitializePrimitiveStream)
	if err != nil {
		panic(err)
	}

	err = precompiles.RegisterPrecompile("whitelist", whitelist.InitializeExtension)
	if err != nil {
		panic(err)
	}
}
