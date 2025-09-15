-- Fee configuration table for ERC20 bridge operations
-- Stores configurable fees and treasury address for deposit and withdraw operations

CREATE TABLE IF NOT EXISTS fee_configs (
    operation_type TEXT NOT NULL,
    fee_percentage NUMERIC(4, 4) NOT NULL,
    treasury_address TEXT NOT NULL,
    created_at INT8 NOT NULL,
    updated_at INT8 NOT NULL,

    PRIMARY KEY (operation_type),

    -- Constraints
    CHECK (operation_type IN ('deposit', 'withdraw')),
    CHECK (fee_percentage >= 0 AND fee_percentage <= 1), -- 0% to 100%
    CHECK (treasury_address LIKE '0x%' AND LENGTH(treasury_address) = 42)
);

-- Action to get fee configuration by operation type
CREATE OR REPLACE ACTION get_fee_config_by_type($operation_type TEXT) 
PUBLIC VIEW RETURNS (
    fee_percentage NUMERIC(4, 4),
    treasury_address TEXT
) {
    FOR $config IN SELECT fee_percentage, treasury_address FROM fee_configs WHERE operation_type = $operation_type {
        RETURN $config.fee_percentage, $config.treasury_address;
    }
};
