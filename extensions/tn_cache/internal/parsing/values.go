package parsing

import (
	"fmt"
	"math/big"
	"strconv"

	"github.com/trufnetwork/kwil-db/core/types"
)

// ParseEventTime converts various types to int64 timestamp
func ParseEventTime(v interface{}) (int64, error) {
	switch val := v.(type) {
	case int64:
		return val, nil
	case int:
		return int64(val), nil
	case int32:
		return int64(val), nil
	case uint64:
		return int64(val), nil
	case uint32:
		return int64(val), nil
	case string:
		if parsed, err := strconv.ParseInt(val, 10, 64); err == nil {
			return parsed, nil
		}
		return 0, fmt.Errorf("invalid timestamp string: %v", val)
	default:
		return 0, fmt.Errorf("unsupported timestamp type: %T", v)
	}
}

// ParseEventValue converts various types to *types.Decimal with decimal(36,18) precision
func ParseEventValue(v interface{}) (*types.Decimal, error) {
	switch val := v.(type) {
	case *types.Decimal:
		if val == nil {
			return nil, fmt.Errorf("nil decimal value")
		}
		// Ensure decimal(36,18) precision
		if err := val.SetPrecisionAndScale(36, 18); err != nil {
			return nil, fmt.Errorf("set precision and scale: %w", err)
		}
		return val, nil
	case float64:
		// Convert float64 to decimal(36,18) - note: may lose precision if > 15 digits
		return types.ParseDecimalExplicit(strconv.FormatFloat(val, 'f', -1, 64), 36, 18)
	case float32:
		return types.ParseDecimalExplicit(strconv.FormatFloat(float64(val), 'f', -1, 32), 36, 18)
	case int64:
		return types.ParseDecimalExplicit(strconv.FormatInt(val, 10), 36, 18)
	case int:
		return types.ParseDecimalExplicit(strconv.Itoa(val), 36, 18)
	case int32:
		return types.ParseDecimalExplicit(strconv.FormatInt(int64(val), 10), 36, 18)
	case uint64:
		return types.ParseDecimalExplicit(strconv.FormatUint(val, 10), 36, 18)
	case uint32:
		return types.ParseDecimalExplicit(strconv.FormatUint(uint64(val), 10), 36, 18)
	case *big.Int:
		if val == nil {
			return nil, fmt.Errorf("nil big.Int value")
		}
		return types.ParseDecimalExplicit(val.String(), 36, 18)
	case string:
		// Parse string directly as decimal(36,18)
		return types.ParseDecimalExplicit(val, 36, 18)
	case []byte:
		// Handle potential byte array from database
		return types.ParseDecimalExplicit(string(val), 36, 18)
	case nil:
		return nil, fmt.Errorf("nil value")
	default:
		return nil, fmt.Errorf("unsupported value type: %T", v)
	}
}