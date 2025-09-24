package tn_vacuum

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/trufnetwork/kwil-db/common"
)

type Config struct {
	Enabled bool

	Trigger              TriggerConfig
	ReloadIntervalBlocks int64
}

type TriggerConfig struct {
	Kind          string
	BlockInterval int64
	CronSchedule  string
}

func LoadConfig(service *common.Service) (Config, error) {
	cfg := Config{
		Trigger: TriggerConfig{
			Kind: triggerFromString(""),
		},
		ReloadIntervalBlocks: defaultReloadBlocks,
	}

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

	if v, ok := raw["trigger"]; ok {
		cfg.Trigger.Kind = triggerFromString(v)
	}

	if v, ok := raw["block_interval"]; ok {
		val, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
		if err != nil {
			return cfg, fmt.Errorf("parse block_interval: %w", err)
		}
		if val < minBlockInterval {
			val = minBlockInterval
		}
		cfg.Trigger.BlockInterval = val
	}

	if v, ok := raw["cron_schedule"]; ok {
		cfg.Trigger.CronSchedule = strings.TrimSpace(v)
	}

	if v, ok := raw["reload_interval_blocks"]; ok {
		val, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
		if err != nil {
			return cfg, fmt.Errorf("parse reload_interval_blocks: %w", err)
		}
		if val <= 0 {
			val = defaultReloadBlocks
		}
		cfg.ReloadIntervalBlocks = val
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

func triggerFromString(in string) string {
	switch strings.ToLower(strings.TrimSpace(in)) {
	case TriggerDigestCoupled:
		return TriggerDigestCoupled
	case TriggerBlockInterval:
		return TriggerBlockInterval
	case TriggerCron:
		return TriggerCron
	case TriggerManual:
		return TriggerManual
	case "":
		return defaultTrigger
	default:
		return defaultTrigger
	}
}
