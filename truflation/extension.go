package truflation

import (
	"github.com/kwilteam/kwil-db/extensions/actions"
	"github.com/kwilteam/kwil-db/truflation/mathutil"
	"github.com/kwilteam/kwil-db/truflation/tsn/basestream"
	"github.com/kwilteam/kwil-db/truflation/tsn/meats_poultry_fish_eggs"
	"github.com/kwilteam/kwil-db/truflation/tsn/stream"
)

// in order for an extension to be registered and included in the binary compilation,
// it must be registered here as follows:
// err = actions.RegisterExtension("extension-name", extension.InitializeExtension)
// if err != nil {
// 	panic(err)
// }

func init() {
	err := actions.RegisterExtension("mathutil", mathutil.InitializeMathUtil)
	if err != nil {
		panic(err)
	}

	err = actions.RegisterExtension("truflation_streams", stream.InitializeStream)
	if err != nil {
		panic(err)
	}

	err = actions.RegisterExtension("basestream", basestream.InitializeBasestream)
	if err != nil {
		panic(err)
	}

	err = actions.RegisterExtension("meats_poultry_fish_eggs", meats_poultry_fish_eggs.InitializeMeatsPoultryFishEggs)
	if err != nil {
		panic(err)
	}
}
