-- MIGRATION 041: SETTLEMENT CONFIG ACTIONS
--
-- Enables automatic settlement and adds actions to manage settlement configuration.
-- Uses existing network_writer role for access control.
--
-- Changes:
-- - Enables settlement with 30-minute schedule by default
-- - Adds update_settlement_config action (role-gated to network_writer)
-- - Adds get_settlement_config action (public view)
--
-- Dependencies:
-- - Migration 039: settlement_config table must exist
-- - Migration 015: system:network_writer role must exist

-- =============================================================================
-- ENABLE SETTLEMENT BY DEFAULT (every 30 minutes: at 0 and 30 past the hour)
-- =============================================================================
UPDATE settlement_config
SET
    enabled = true,
    settlement_schedule = '0,30 * * * *',
    max_markets_per_run = 100,
    retry_attempts = 3,
    updated_at = 0
WHERE id = 1;

-- =============================================================================
-- ACTION: update_settlement_config
-- =============================================================================
-- Updates the settlement extension configuration.
-- Requires caller to be a member of system:network_writer role.
--
-- Parameters:
-- - $enabled: Enable/disable automatic settlement (BOOL)
-- - $schedule: Cron schedule for settlement checks (TEXT)
-- - $max_markets_per_run: Max markets to process per job run (INT, 1-100)
-- - $retry_attempts: Number of retry attempts for failed settlements (INT, 1-10)
--
-- Usage:
--   kwil-cli call-action update_settlement_config bool:true text:'0,30 * * * *' int:100 int:3
CREATE OR REPLACE ACTION update_settlement_config(
    $enabled BOOL,
    $schedule TEXT,
    $max_markets_per_run INT,
    $retry_attempts INT
) PUBLIC {
    -- Validate caller has network_writer role
    $caller_addr TEXT := LOWER(@caller);
    $has_role BOOL := false;

    for $row in SELECT 1 FROM role_members
        WHERE owner = 'system'
        AND role_name = 'network_writer'
        AND wallet = $caller_addr {
        $has_role := true;
    }

    if NOT $has_role {
        ERROR('caller must be a member of system:network_writer role');
    }

    -- Validate inputs
    if $enabled IS NULL {
        ERROR('enabled cannot be NULL');
    }

    if $schedule IS NULL OR $schedule = '' {
        ERROR('schedule cannot be empty');
    }

    if $max_markets_per_run IS NULL OR $max_markets_per_run < 1 OR $max_markets_per_run > 100 {
        ERROR('max_markets_per_run must be between 1 and 100');
    }

    if $retry_attempts IS NULL OR $retry_attempts < 1 OR $retry_attempts > 10 {
        ERROR('retry_attempts must be between 1 and 10');
    }

    -- Update configuration
    UPDATE settlement_config
    SET
        enabled = $enabled,
        settlement_schedule = $schedule,
        max_markets_per_run = $max_markets_per_run,
        retry_attempts = $retry_attempts,
        updated_at = @height
    WHERE id = 1;
};

-- =============================================================================
-- ACTION: get_settlement_config
-- =============================================================================
-- Returns the current settlement configuration.
-- Public view action - anyone can read the config.
--
-- Returns:
-- - enabled: Whether automatic settlement is enabled
-- - settlement_schedule: Cron schedule for settlement checks
-- - max_markets_per_run: Max markets processed per job run
-- - retry_attempts: Retry attempts for failed settlements
-- - updated_at: Block height when config was last updated
CREATE OR REPLACE ACTION get_settlement_config()
PUBLIC VIEW RETURNS TABLE(
    enabled BOOL,
    settlement_schedule TEXT,
    max_markets_per_run INT,
    retry_attempts INT,
    updated_at INT8
) {
    for $row in SELECT
        enabled,
        settlement_schedule,
        max_markets_per_run,
        retry_attempts,
        updated_at
    FROM settlement_config
    WHERE id = 1 {
        RETURN NEXT $row.enabled, $row.settlement_schedule, $row.max_markets_per_run, $row.retry_attempts, $row.updated_at;
    }
};
