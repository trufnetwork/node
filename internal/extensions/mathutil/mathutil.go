package mathutil

import (
	"fmt"
	"strings"

	"github.com/kwilteam/kwil-db/common"
	"github.com/kwilteam/kwil-db/extensions/precompiles"
	"github.com/truflation/tsn-db/internal/utils"
)

func InitializeMathUtil(ctx *precompiles.DeploymentContext, service *common.Service, metadata map[string]string) (precompiles.Instance, error) {
	if len(metadata) != 0 {
		return nil, fmt.Errorf("mathutil does not take any configs")
	}

	return &mathUtilExt{}, nil
}

type mathUtilExt struct{}

func (m *mathUtilExt) Call(scoper *precompiles.ProcedureContext, app *common.App, method string, inputs []any) ([]any, error) {
	switch strings.ToLower(method) {
	case knownMethodFraction:
		if len(inputs) != 3 {
			return nil, fmt.Errorf("expected 3 inputs, got %d", len(inputs))
		}

		number, ok := inputs[0].(int64)
		if !ok {
			return nil, fmt.Errorf("expected int64 for arg 1, got %T", inputs[0])
		}

		numerator, ok := inputs[1].(int64)
		if !ok {
			return nil, fmt.Errorf("expected int64 for arg 2, got %T", inputs[1])
		}

		denominator, ok := inputs[2].(int64)
		if !ok {
			return nil, fmt.Errorf("expected int64 for arg 3, got %T", inputs[2])
		}

		return fraction(number, numerator, denominator)
	default:
		return nil, fmt.Errorf("unknown method '%s'", method)
	}
}

func fraction(number, numerator, denominator int64) ([]any, error) {
	result, err := utils.Fraction(number, numerator, denominator)
	if err != nil {
		return nil, err
	}

	return []any{result}, nil
}

const (
	knownMethodFraction = "fraction"
)
