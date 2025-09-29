package tn_vacuum

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/trufnetwork/kwil-db/common"
)

type Config struct {
	Enabled       bool
	BlockInterval int64
}

func LoadConfig(service *common.Service) (Config, error) {
	cfg := Config{Enabled: true, BlockInterval: defaultBlockInterval}

	if service == nil || service.LocalConfig == nil {
		return cfg, nil
	}

	raw, ok := service.LocalConfig.Extensions[ExtensionName]
	if !ok {
		return cfg, nil
	}

	if v, ok := raw["enabled"]; ok {
		boolVal, err := parseBool(v)
		if err != nil {
			return cfg, fmt.Errorf("parse enabled: %w", err)
		}
		cfg.Enabled = boolVal
	}

	if v, ok := raw["block_interval"]; ok {
		val, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
		if err != nil {
			return cfg, fmt.Errorf("parse block_interval: %w", err)
		}
		if val <= 0 {
			val = defaultBlockInterval
		}
		if val < minBlockInterval {
			val = minBlockInterval
		}
		cfg.BlockInterval = val
	}

	return cfg, nil
}

func parseBool(in string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(in)) {
	case "true", "1", "yes", "y", "on":
		return true, nil
	case "false", "0", "no", "n", "off", "":
		return false, nil
	default:
		return false, fmt.Errorf("invalid bool %q", in)
	}
}
